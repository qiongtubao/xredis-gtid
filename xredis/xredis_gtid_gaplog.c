/* Copyright (c) 2026, ctrip.com
 * All rights reserved.
 *
 * 高性能GTID Gaplog实现 - 环形缓冲区版本
 * 优化点：
 * 1. 使用环形缓冲区，插入和删除都是O(1)，避免数组移动
 * 2. 不在gtidUuidEntry中存储uuid（dict key已有）
 * 3. 使用robj引用计数而非深拷贝
 * 4. 固定容量，创建时分配，避免动态扩容
 */

#include "server.h"
#include "./xredis_gtid_gaplog.h"

/* 前向声明 */
static void gtidGnoEntryDestroy(gtidGnoEntry *entry);

/* ============================================================================
 * Dict类型定义 - 使用sds作为key
 * ============================================================================ */

/* sds哈希函数 */
static uint64_t sdsDictHash(const void *key) {
    return dictGenHashFunction((unsigned char*)key, sdslen((sds)key));
}

/* sds比较函数 */
static int sdsDictKeyCompare(void *privdata, const void *key1, const void *key2) {
    DICT_NOTUSED(privdata);
    return sdscmp((sds)key1, (sds)key2) == 0;
}

/* sds析构函数 */
static void sdsDictKeyDestructor(void *privdata, void *val) {
    DICT_NOTUSED(privdata);
    sdsfree((sds)val);
}

/* gtidUuidEntry析构函数 */
static void uuidEntryDestructor(void *privdata, void *val) {
    DICT_NOTUSED(privdata);
    gtidUuidEntry *entry = (gtidUuidEntry *)val;
    if (entry == NULL) return;

    /* 销毁所有gno条目 */
    for (size_t i = 0; i < entry->count; i++) {
        size_t idx = (entry->head + i) % entry->capacity;
        gtidGnoEntryDestroy(entry->entries[idx]);
    }
    zfree(entry->entries);
    zfree(entry);
}

/* dict类型定义 */
static dictType uuidDictType = {
    sdsDictHash,           /* hash function */
    NULL,                  /* key dup */
    NULL,                  /* val dup */
    sdsDictKeyCompare,     /* key compare */
    sdsDictKeyDestructor,  /* key destructor */
    uuidEntryDestructor    /* val destructor */
};

/* ============================================================================
 * gtidGnoEntry Functions
 * ============================================================================ */

static gtidGnoEntry *gtidGnoEntryCreate(gno_t gno, robj **keys, robj **subkeys, size_t key_count) {
    gtidGnoEntry *entry = zmalloc(sizeof(gtidGnoEntry));
    if (entry == NULL) return NULL;

    entry->gno = gno;
    entry->key_count = key_count;
    entry->timestamp = time(NULL);

    /* 存储robj指针并增加引用计数，避免深拷贝 */
    if (key_count > 0 && keys != NULL) {
        entry->keys = zmalloc(sizeof(robj*) * key_count);
        if (entry->keys == NULL) {
            zfree(entry);
            return NULL;
        }
        for (size_t i = 0; i < key_count; i++) {
            entry->keys[i] = keys[i];
            if (keys[i]) incrRefCount(keys[i]);
        }
    } else {
        entry->keys = NULL;
    }

    if (key_count > 0 && subkeys != NULL) {
        entry->subkeys = zmalloc(sizeof(robj*) * key_count);
        if (entry->subkeys == NULL) {
            /* 回滚已分配的keys */
            if (entry->keys) {
                for (size_t i = 0; i < key_count; i++) {
                    if (entry->keys[i]) decrRefCount(entry->keys[i]);
                }
                zfree(entry->keys);
            }
            zfree(entry);
            return NULL;
        }
        for (size_t i = 0; i < key_count; i++) {
            entry->subkeys[i] = subkeys[i];
            if (subkeys[i]) incrRefCount(subkeys[i]);
        }
    } else {
        entry->subkeys = NULL;
    }

    return entry;
}

static void gtidGnoEntryDestroy(gtidGnoEntry *entry) {
    if (entry == NULL) return;
    /* 减少引用计数并释放数组 */
    if (entry->keys) {
        for (size_t i = 0; i < entry->key_count; i++) {
            if (entry->keys[i]) decrRefCount(entry->keys[i]);
        }
        zfree(entry->keys);
    }
    if (entry->subkeys) {
        for (size_t i = 0; i < entry->key_count; i++) {
            if (entry->subkeys[i]) decrRefCount(entry->subkeys[i]);
        }
        zfree(entry->subkeys);
    }
    zfree(entry);
}

/* ============================================================================
 * gtidUuidEntry Functions - 环形缓冲区版本
 * ============================================================================ */

/**
 * 创建UUID条目（环形缓冲区）
 * @param capacity 缓冲区容量，固定大小
 */
static gtidUuidEntry *gtidUuidEntryCreate(size_t capacity) {
    gtidUuidEntry *entry = zmalloc(sizeof(gtidUuidEntry));
    if (entry == NULL) return NULL;

    entry->capacity = capacity;
    entry->entries = zmalloc(sizeof(gtidGnoEntry*) * capacity);
    if (entry->entries == NULL) {
        zfree(entry);
        return NULL;
    }
    memset(entry->entries, 0, sizeof(gtidGnoEntry*) * capacity);

    entry->head = 0;
    entry->tail = 0;
    entry->count = 0;
    entry->min_gno = 0;
    entry->max_gno = 0;

    return entry;
}

/**
 * 环形缓冲区插入 - O(1)
 * 如果缓冲区已满，自动删除最老条目
 * @return GAPLOG_OK成功，GAPLOG_ERR表示删除了旧条目（调用者可据此调整计数）
 */
static int gtidUuidEntryPush(gtidUuidEntry *uuid_entry, gtidGnoEntry *gno_entry) {
    int deleted_old = 0;

    /* 如果缓冲区已满，删除最老条目 */
    if (uuid_entry->count >= uuid_entry->capacity) {
        gtidGnoEntryDestroy(uuid_entry->entries[uuid_entry->head]);
        uuid_entry->head = (uuid_entry->head + 1) % uuid_entry->capacity;
        uuid_entry->count--;
        deleted_old = 1;
    }

    /* 在tail位置插入新条目 */
    uuid_entry->entries[uuid_entry->tail] = gno_entry;
    uuid_entry->tail = (uuid_entry->tail + 1) % uuid_entry->capacity;
    uuid_entry->count++;

    /* 更新min/max */
    if (uuid_entry->count == 1) {
        uuid_entry->min_gno = gno_entry->gno;
        uuid_entry->max_gno = gno_entry->gno;
    } else {
        if (gno_entry->gno < uuid_entry->min_gno) uuid_entry->min_gno = gno_entry->gno;
        if (gno_entry->gno > uuid_entry->max_gno) uuid_entry->max_gno = gno_entry->gno;
    }

    return deleted_old ? GAPLOG_ERR : GAPLOG_OK;
}

/**
 * 环形缓冲区查找
 * 由于gno通常递增，从最新条目向前遍历效率更高
 */
static gtidGnoEntry *gtidUuidEntryFind(gtidUuidEntry *uuid_entry, gno_t gno) {
    if (uuid_entry->count == 0) return NULL;

    /* 快速范围检查 */
    if (gno < uuid_entry->min_gno || gno > uuid_entry->max_gno) return NULL;

    /* 从最新条目向前遍历（gno通常递增，这样更快） */
    for (size_t i = 0; i < uuid_entry->count; i++) {
        size_t idx = (uuid_entry->tail + uuid_entry->capacity - 1 - i) % uuid_entry->capacity;
        if (uuid_entry->entries[idx]->gno == gno) {
            return uuid_entry->entries[idx];
        }
    }
    return NULL;
}

/**
 * 环形缓冲区清理 - 删除gno小于watermark的条目
 * O(n)但只移动head指针，不需要移动数据
 */
static size_t gtidUuidEntryTrimByWatermark(gtidUuidEntry *uuid_entry, gno_t watermark) {
    size_t trimmed = 0;

    while (uuid_entry->count > 0) {
        gtidGnoEntry *oldest = uuid_entry->entries[uuid_entry->head];
        if (oldest->gno >= watermark) break;

        gtidGnoEntryDestroy(oldest);
        uuid_entry->entries[uuid_entry->head] = NULL;
        uuid_entry->head = (uuid_entry->head + 1) % uuid_entry->capacity;
        uuid_entry->count--;
        trimmed++;
    }

    /* 更新min_gno */
    if (uuid_entry->count > 0) {
        uuid_entry->min_gno = uuid_entry->entries[uuid_entry->head]->gno;
    } else {
        uuid_entry->min_gno = 0;
        uuid_entry->max_gno = 0;
    }

    return trimmed;
}

/* ============================================================================
 * Core API Implementation
 * ============================================================================ */

gtidGaplog *gtidGaplogCreate(size_t max_gap) {
    gtidGaplog *gaplog = zmalloc(sizeof(gtidGaplog));
    if (gaplog == NULL) return NULL;

    gaplog->uuid_index = dictCreate(&uuidDictType, NULL);
    if (gaplog->uuid_index == NULL) {
        zfree(gaplog);
        return NULL;
    }

    gaplog->max_gap = max_gap > 0 ? max_gap : GAPLOG_DEFAULT_MAX_GAP;
    gaplog->used_memory = sizeof(gtidGaplog);
    gaplog->hit_count = 0;
    gaplog->miss_count = 0;
    gaplog->total_entries = 0;

    return gaplog;
}

void gtidGaplogDestroy(gtidGaplog *gaplog) {
    if (gaplog == NULL) return;
    dictRelease(gaplog->uuid_index);
    zfree(gaplog);
}

int gtidGaplogAppend(gtidGaplog *gaplog, sds uuid, gno_t gno,
                     robj **keys, robj **subkeys, size_t key_count) {
    if (gaplog == NULL || uuid == NULL || (keys == NULL && key_count > 0)) {
        return GAPLOG_ERR_INVALID_PARAM;
    }

    /* 查找或创建UUID条目 */
    dictEntry *de = dictFind(gaplog->uuid_index, uuid);
    gtidUuidEntry *uuid_entry = NULL;

    if (de == NULL) {
        /* 创建新的UUID条目，使用固定容量的环形缓冲区 */
        uuid_entry = gtidUuidEntryCreate(gaplog->max_gap);
        if (uuid_entry == NULL) return GAPLOG_ERR_MEMORY;

        /* 复制uuid作为dict key */
        sds uuid_key = sdsdup(uuid);
        if (uuid_key == NULL) {
            zfree(uuid_entry->entries);
            zfree(uuid_entry);
            return GAPLOG_ERR_MEMORY;
        }

        if (dictAdd(gaplog->uuid_index, uuid_key, uuid_entry) != DICT_OK) {
            sdsfree(uuid_key);
            zfree(uuid_entry->entries);
            zfree(uuid_entry);
            return GAPLOG_ERR_MEMORY;
        }
    } else {
        uuid_entry = dictGetVal(de);
    }

    /* 创建gno条目 */
    gtidGnoEntry *gno_entry = gtidGnoEntryCreate(gno, keys, subkeys, key_count);
    if (gno_entry == NULL) return GAPLOG_ERR_MEMORY;

    /* 插入环形缓冲区 - O(1) */
    int result = gtidUuidEntryPush(uuid_entry, gno_entry);
    if (result == GAPLOG_ERR_MEMORY) {
        gtidGnoEntryDestroy(gno_entry);
        return GAPLOG_ERR_MEMORY;
    }

    /* 如果返回 GAPLOG_ERR 表示删除了旧条目，total_entries 不变；
     * 如果返回 GAPLOG_OK 表示新插入，total_entries +1 */
    if (result == GAPLOG_OK) {
        gaplog->total_entries++;
    }

    return GAPLOG_OK;
}

gtidGnoEntry *gtidGaplogGet(gtidGaplog *gaplog, sds uuid, gno_t gno) {
    if (gaplog == NULL || uuid == NULL) return NULL;

    dictEntry *de = dictFind(gaplog->uuid_index, uuid);
    if (de == NULL) {
        gaplog->miss_count++;
        return NULL;
    }

    gtidUuidEntry *uuid_entry = dictGetVal(de);
    gtidGnoEntry *entry = gtidUuidEntryFind(uuid_entry, gno);
    if (entry == NULL) {
        gaplog->miss_count++;
        return NULL;
    }

    gaplog->hit_count++;
    return entry;
}

size_t gtidGaplogTrim(gtidGaplog *gaplog, sds uuid, gno_t watermark) {
    if (gaplog == NULL) return 0;

    size_t total_trimmed = 0;

    if (uuid != NULL) {
        dictEntry *de = dictFind(gaplog->uuid_index, uuid);
        if (de == NULL) return 0;

        gtidUuidEntry *uuid_entry = dictGetVal(de);
        total_trimmed = gtidUuidEntryTrimByWatermark(uuid_entry, watermark);
    } else {
        dictIterator *di = dictGetIterator(gaplog->uuid_index);
        dictEntry *de;
        while ((de = dictNext(di)) != NULL) {
            gtidUuidEntry *uuid_entry = dictGetVal(de);
            total_trimmed += gtidUuidEntryTrimByWatermark(uuid_entry, watermark);
        }
        dictReleaseIterator(di);
    }

    gaplog->total_entries -= total_trimmed;
    return total_trimmed;
}

void gtidGaplogClear(gtidGaplog *gaplog) {
    if (gaplog == NULL) return;
    dictEmpty(gaplog->uuid_index, NULL);
    gaplog->total_entries = 0;
    gaplog->used_memory = sizeof(gtidGaplog);
}

void gtidGaplogGetStat(gtidGaplog *gaplog, gtidGaplogStat *stat) {
    if (gaplog == NULL || stat == NULL) return;

    stat->total_entries = gaplog->total_entries;
    stat->uuid_count = dictSize(gaplog->uuid_index);
    stat->hit_count = gaplog->hit_count;
    stat->miss_count = gaplog->miss_count;
    stat->total_memory = gaplog->used_memory;
}

gtidKeyMapping **gtidGaplogList(gtidGaplog *gaplog, sds uuid,
                                 size_t count, size_t *actual_count) {
    if (gaplog == NULL || actual_count == NULL) return NULL;

    if (gaplog->total_entries == 0) {
        *actual_count = 0;
        return NULL;
    }

    size_t max_count = count > 0 ? count : gaplog->total_entries;
    gtidKeyMapping **result = zmalloc(sizeof(gtidKeyMapping*) * max_count);
    if (result == NULL) return NULL;

    size_t result_count = 0;

    if (uuid != NULL) {
        dictEntry *de = dictFind(gaplog->uuid_index, uuid);
        if (de == NULL) {
            zfree(result);
            *actual_count = 0;
            return NULL;
        }
        gtidUuidEntry *uuid_entry = dictGetVal(de);
        sds uuid_sds = dictGetKey(de);

        /* 从head开始遍历环形缓冲区 */
        for (size_t i = 0; i < uuid_entry->count && result_count < max_count; i++) {
            size_t idx = (uuid_entry->head + i) % uuid_entry->capacity;
            gtidGnoEntry *gno_entry = uuid_entry->entries[idx];
            gtidKeyMapping *mapping = zmalloc(sizeof(gtidKeyMapping));
            if (mapping == NULL) break;
            mapping->uuid = uuid_sds;
            mapping->gno = gno_entry->gno;
            /* 将robj转换为sds指针用于输出 */
            if (gno_entry->keys) {
                mapping->keys = zmalloc(sizeof(sds) * gno_entry->key_count);
                for (size_t j = 0; j < gno_entry->key_count; j++) {
                    mapping->keys[j] = gno_entry->keys[j] ? gno_entry->keys[j]->ptr : NULL;
                }
            } else {
                mapping->keys = NULL;
            }
            if (gno_entry->subkeys) {
                mapping->subkeys = zmalloc(sizeof(sds) * gno_entry->key_count);
                for (size_t j = 0; j < gno_entry->key_count; j++) {
                    mapping->subkeys[j] = gno_entry->subkeys[j] ? gno_entry->subkeys[j]->ptr : NULL;
                }
            } else {
                mapping->subkeys = NULL;
            }
            mapping->key_count = gno_entry->key_count;
            mapping->timestamp = gno_entry->timestamp;

            result[result_count++] = mapping;
        }
    } else {
        dictIterator *di = dictGetIterator(gaplog->uuid_index);
        dictEntry *de;
        while ((de = dictNext(di)) != NULL && result_count < max_count) {
            gtidUuidEntry *uuid_entry = dictGetVal(de);
            sds uuid_sds = dictGetKey(de);

            /* 从head开始遍历环形缓冲区 */
            for (size_t i = 0; i < uuid_entry->count && result_count < max_count; i++) {
                size_t idx = (uuid_entry->head + i) % uuid_entry->capacity;
                gtidGnoEntry *gno_entry = uuid_entry->entries[idx];
                gtidKeyMapping *mapping = zmalloc(sizeof(gtidKeyMapping));
                if (mapping == NULL) break;
                mapping->uuid = uuid_sds;
                mapping->gno = gno_entry->gno;
                /* 将robj转换为sds指针用于输出 */
                if (gno_entry->keys) {
                    mapping->keys = zmalloc(sizeof(sds) * gno_entry->key_count);
                    for (size_t j = 0; j < gno_entry->key_count; j++) {
                        mapping->keys[j] = gno_entry->keys[j] ? gno_entry->keys[j]->ptr : NULL;
                    }
                } else {
                    mapping->keys = NULL;
                }
                if (gno_entry->subkeys) {
                    mapping->subkeys = zmalloc(sizeof(sds) * gno_entry->key_count);
                    for (size_t j = 0; j < gno_entry->key_count; j++) {
                        mapping->subkeys[j] = gno_entry->subkeys[j] ? gno_entry->subkeys[j]->ptr : NULL;
                    }
                } else {
                    mapping->subkeys = NULL;
                }
                mapping->key_count = gno_entry->key_count;
                mapping->timestamp = gno_entry->timestamp;

                result[result_count++] = mapping;
            }
        }
        dictReleaseIterator(di);
    }

    *actual_count = result_count;
    return result;
}

size_t gtidGaplogGetCount(gtidGaplog *gaplog) {
    if (gaplog == NULL) return 0;
    return gaplog->total_entries;
}

void gtidGaplogSetMaxGap(gtidGaplog *gaplog, size_t max_gap) {
    if (gaplog == NULL) return;
    /* 注意：环形缓冲区版本，容量在创建时固定，这里只更新配置 */
    gaplog->max_gap = max_gap > 0 ? max_gap : GAPLOG_DEFAULT_MAX_GAP;
}