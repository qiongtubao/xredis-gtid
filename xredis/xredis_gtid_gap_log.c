
#include "server.h"

/* readBacklogCtx 结构体定义（含完整 client，需在 server.h 之后才能编译）。
 * 注意：此结构体同样定义在 xredis_gtid_repl.c 中，两处定义必须完全一致。 */
struct readBacklogCtx {
    client c;                       /* 可复用querybuf的mock client */
    size_t base_backlog_offset;     /* 当前gno在backlog中的起始绝对偏移 */
    size_t readed_backlog_offset;   /* 已从backlog读取到querybuf的字节数（相对偏移） */
};

static void uuidSetFreeWrapper(void *ptr) {
    uuidSet *us = (uuidSet*)ptr;
    if (us) {
        uuidSetFree(us);
    }
}

void gtidGapLogSkiplistDestructor(void *privdata, void *val) {
    UNUSED(privdata);
    skiplist *sl = (skiplist*)val;
    if (sl) {
        freeSkipList(sl);
    }
}

static dictType gtidGapLogDictType = {
    .hashFunction = dictSdsHash,
    .keyCompare = dictSdsKeyCompare,
    .keyDestructor = dictSdsDestructor,
    .valDestructor = gtidGapLogSkiplistDestructor
};
gtidGapLog* createGtidGapLog() {
    gtidGapLog* gaplog =  zmalloc(sizeof(gtidGapLog));
    gaplog->data = dictCreate(&gtidGapLogDictType, NULL);
    gaplog->size = 0;
    gaplog->history = listCreate();
    listSetFreeMethod(gaplog->history, uuidSetFreeWrapper);
    return gaplog;
}

void resetGtidGapLog(gtidGapLog* gaplog) {
    dictEmpty(gaplog->data, NULL);
    gaplog->size = 0;
    listEmpty(gaplog->history);
}
void freeGtidGapLog(gtidGapLog* gaplog) {
    dictRelease(gaplog->data);
    listRelease(gaplog->history);
    gaplog->size = 0;
}

/*gap log keys info*/
gtidGapLogKeys* createGtidGapLogKeys(int max_size) {
    gtidGapLogKeys* infos = zmalloc(sizeof(gtidGapLogKeys));
    infos->size = 0;
    infos->keys = zmalloc(sizeof(gtidGapLogKeys) * max_size);
    return infos;
}

void freeGtidGapLogKeys(void *data) {
    if (data == NULL) return;
    gtidGapLogKeys* keys = (gtidGapLogKeys*)data;  
    for (int i = 0; i < keys->size; i++) {
        freeGtidGapLogKey(keys->keys[i]);
    }
    zfree(keys->keys);
    zfree(keys);
}

/*gap log key info*/
gtidGapLogKey* createGtidGapLogKey(int dbid, int type, sds key, sds* subkeys, int subkeys_count) {
    gtidGapLogKey *ki = zcalloc(sizeof(gtidGapLogKey));
    ki->dbid = dbid;
    ki->key_type = type;
    ki->key = key;           /* move */
    ki->subkeys = subkeys;   /* move */
    ki->subkeys_count = subkeys_count;
    return ki;
}

void freeGtidGapLogKey(gtidGapLogKey* ki) {
    if (ki == NULL) return;
    sdsfree(ki->key);
    for (int i = 0; i < ki->subkeys_count; i++) {
        sdsfree(ki->subkeys[i]);
    }
    zfree(ki->subkeys);
    zfree(ki);
}

gtidGapLogKey** gtidGapLogKeysPrepareBuilder(gtidGapLogKeysBuilder* builder, int add_numkeys) {
    if (!builder->keys_infos) {
        builder->keys_infos = builder->cache;
    }

    if (add_numkeys  + builder->numkeys > builder->size) {
        if (builder->keys_infos != builder->cache) {
            builder->keys_infos = zrealloc(builder->keys_infos, sizeof(gtidGapLogKey*) * ( 2 * builder->size));
        } else {
            builder->keys_infos = zmalloc(sizeof(gtidGapLogKey*) * (2 * builder->size));
            if (builder->numkeys)
                memcpy(builder->keys_infos, builder->cache, sizeof(gtidGapLogKey*) * builder->numkeys);
        }
        builder->size = 2 * builder->size;
    }
    return builder->keys_infos + builder->numkeys;

}

void freeGtidGaplogKeysBuilder(gtidGapLogKeysBuilder* builer) {
    for (int i  = 0; i < builer->numkeys; i++) {
        freeGtidGapLogKey(builer->keys_infos[i]);  
        builer->keys_infos[i] = NULL;
    }
    if (builer && builer->keys_infos != builer->cache) {
        zfree(builer->keys_infos);
    }   
}

gtidGapLogKeys* buildGtidGapLogKeys(gtidGapLogKeysBuilder* builder) {
    gtidGapLogKeys* keys = zmalloc(sizeof(gtidGapLogKeys));
    keys->size = builder->numkeys;
    /*move keys*/
    keys->keys =zmalloc(sizeof(gtidGapLogKey*) * keys->size);
    for(int i = 0; i < builder->numkeys; i++) {
        keys->keys[i] = builder->keys_infos[i];
        builder->keys_infos[i] = NULL;
    }
    builder->numkeys = 0;
    return keys;
}

/* ========== gtidGapLog Data iterator ========== */
void gtidGapLogInitDataIterator(gtidGapLogDataIterator *iter, skiplist *sl, gno_t start_gno) {
    iter->node = findFirstGteSkipList(sl, start_gno);
}

void gtidGapLogDeinitDataIterator(gtidGapLogDataIterator *iter) {
    UNUSED(iter);
}

gno_t gtidGapLogDataGetGno(gtidGapLogDataIterator* iter) {
    if (iter->node == NULL) return -1;
    return (gno_t)iter->node->score;
}

gtidGapLogKeys* gtidGapLogDataNext(gtidGapLogDataIterator* iter) {
    if (iter->node == NULL) return NULL;
    gtidGapLogKeys* keys = (gtidGapLogKeys*)iter->node->value;
    iter->node = iter->node->level[0].forward;
    return keys;
}

/* ========== gtidGapLog History iterator ========== */

void gtidGapLogInitHistoryIterator(gtidGapLogHistoryIterator* iter,
                                    gtidGapLog* gaplog, long long index) {
    iter->list_node = listFirst(gaplog->history);
    iter->interval_node = NULL;
    iter->next_gno = 0;

    if (iter->list_node == NULL) return;

    uuidSet *us = listNodeValue(iter->list_node);
    iter->interval_node = us->intervals->header->forwards[0];

    long long remaining = index;

    while (iter->list_node && remaining > 0) {
        us = listNodeValue(iter->list_node);
        gno_t us_count = uuidSetCount(us);
        if (remaining >= us_count) {
            remaining -= us_count;
            iter->list_node = listNextNode(iter->list_node);
            if (iter->list_node) {
                us = listNodeValue(iter->list_node);
                iter->interval_node = us->intervals->header->forwards[0];
            }
            continue;
        }
        break;
    }

    while (iter->interval_node && remaining > 0) {
        gno_t interval_len = iter->interval_node->end - iter->interval_node->start + 1;
        if (remaining >= interval_len) {
            remaining -= interval_len;
            iter->interval_node = iter->interval_node->forwards[0];
            continue;
        }
        break;
    }

    if (iter->list_node && iter->interval_node) {
        iter->next_gno = iter->interval_node->start + remaining;
    } else {
        iter->list_node = NULL;
    }
}


gno_t gtidGapLogHistoryNext(gtidGapLogHistoryIterator* iter,
                             const char** uuid, size_t* uuid_len) {
    if (iter->list_node == NULL) {
        *uuid = NULL;
        *uuid_len = 0;
        return 0;
    }

    uuidSet *us = listNodeValue(iter->list_node);
    *uuid = us->uuid;
    *uuid_len = us->uuid_len;

    gno_t result = iter->next_gno;

    iter->next_gno++;

    if (iter->next_gno > iter->interval_node->end) {
        iter->interval_node = iter->interval_node->forwards[0];
        if (iter->interval_node) {
            iter->next_gno = iter->interval_node->start;
        } else {
            iter->list_node = listNextNode(iter->list_node);
            if (iter->list_node) {
                us = listNodeValue(iter->list_node);
                iter->interval_node = us->intervals->header->forwards[0];
                iter->next_gno = iter->interval_node->start;
            }
        }
    }

    return result;
}

void gtidGapLogDeinitHistoryIterator(gtidGapLogHistoryIterator* iter) {
    UNUSED(iter);
}


void addReplyGtidGapLogKeys(client* c, gtidGapLogKeys* keys) {
    addReplyArrayLen(c, keys->size);
    for (int i = 0; i < keys->size; i++) {
        gtidGapLogKey *k = keys->keys[i];
        addReplyArrayLen(c, 4);
        addReplyBulkLongLong(c, k->dbid);
        robj o = { .type = k->key_type };
        addReplyBulkCString(c, getObjectTypeName(&o));
        addReplyBulkCBuffer(c, k->key, sdslen(k->key));
        addReplyArrayLen(c, k->subkeys_count);
        for (int j = 0; j < k->subkeys_count; j++) {
            addReplyBulkCBuffer(c, k->subkeys[j], sdslen(k->subkeys[j]));
        }
    }
}

int gapLogTrim(size_t size) {
    size_t count = 0;
    while (count < size) {
        listNode *first_ln = listFirst(server.gtid_gap_log->history);
        if (first_ln == NULL) return count;

        uuidSet *first_uuid_set = (uuidSet*)listNodeValue(first_ln);

        gno_t min_gno = uuidSetNext(first_uuid_set, 0);
        if (min_gno == 0) {
            listDelNode(server.gtid_gap_log->history, first_ln);
            continue;
        }

        sds evict_uuid_sds = sdsnewlen(first_uuid_set->uuid, first_uuid_set->uuid_len);
        dictEntry *de = dictFind(server.gtid_gap_log->data, evict_uuid_sds);
        if (de != NULL) {
            skiplist *sl = dictGetVal(de);
            deleteSkipList(sl, min_gno);
            if (sl->length == 0) {
                dictDelete(server.gtid_gap_log->data, evict_uuid_sds);
            }
        } else {
            serverPanic("not find keysinfo in gtid_gap_log");
        }
        sdsfree(evict_uuid_sds);
        
        uuidSetRemove(first_uuid_set, min_gno, min_gno);
        if (uuidSetCount(first_uuid_set) == 0) {
            listDelNode(server.gtid_gap_log->history, first_ln);
        }

        server.gtid_gap_log->size--;
        count++;
    }
    return count;
}


void saveGapLogFromGtidSet(gtidSet *mlost) {

    /* 性能统计：记录backlog复制+解析耗时 */
    long long total_cmds = 0;
    long long total_parse_us = 0;       /* parseCmdFromBacklog总耗时(us) */
    long long total_entry_us = 0;       /* saveGapLogEntry总耗时(us) */
    long long total_copy_bytes = 0;     /* backlog复制总字节数 */
    long long max_parse_us = 0;
    size_t max_cmd_bytes = 0;

    readBacklogCtx ctx = {
        .c = {0},
        .base_backlog_offset = 0,
        .readed_backlog_offset = 0
    };
    resetMockClient(&ctx.c);  /* 初始化querybuf（sdsempty），后续循环复用 */

    gtidSetIterator gs_iterator;
    gtidSetInitIterator(&gs_iterator, mlost);
    uuidSet *us = NULL;
    while ((us = gtidSetIteratorNext(&gs_iterator)) != NULL) {
        uuidSetIterator us_iterator;
        uuidSetInitIterator(&us_iterator,us);
        
        gtidIntervalNode *node = NULL;
        while ((node = uuidSetIteratorNext(&us_iterator)) != NULL) {
            sds uuid = sdsnewlen(us->uuid, us->uuid_len);
            for (gno_t gno = node->start; gno <= node->end; gno++) {
                long long offset = gtidSeqLookup(server.gtid_seq, uuid, sdslen(uuid), gno);
                if (offset < 0) continue;
                 /* 检查新offset是否已在querybuf已有数据范围内，避免重复拷贝backlog */
                size_t qb_range_start = ctx.base_backlog_offset + ctx.readed_backlog_offset - sdslen(ctx.c.querybuf);
                size_t qb_range_end = ctx.base_backlog_offset + ctx.readed_backlog_offset;
                if (offset >= (long long)qb_range_start && offset < (long long)qb_range_end) {
                    /* offset在querybuf范围内，复用已有数据：
                     * 1. 裁剪querybuf去掉offset之前的部分
                     * 2. 重置qb_pos为0
                     * 3. 更新base_backlog_offset和readed_backlog_offset */
                    size_t new_qb_pos = (size_t)(offset - (long long)qb_range_start);
                    sdsrange(ctx.c.querybuf, new_qb_pos, -1);
                    ctx.c.qb_pos = 0;
                    ctx.base_backlog_offset = (size_t)offset;
                    ctx.readed_backlog_offset = sdslen(ctx.c.querybuf);
                } else {
                    /* offset不在已有范围，清空querybuf重新定位 */
                    resetMockClient(&ctx.c);
                    ctx.base_backlog_offset = (size_t)offset;
                    ctx.readed_backlog_offset = 0;
                }
                long long dbid_from_select = -1;
                
                gtidGapLogKeysBuilder build = GTID_GAPLOG_KEYS_BUILER_INIT;
                while (1) {
                    /* 计时：backlog复制+RESP协议解析 */
                    ustime_t parse_start = ustime();
                    size_t cur_cmd_len = 0;
                    if (parseCmdFromBacklog(&ctx, &cur_cmd_len) < 0) {
                        break;
                    }
                    ustime_t parse_end = ustime();
                    long long parse_us = parse_end - parse_start;
                    total_parse_us += parse_us;
                    if (parse_us > max_parse_us) max_parse_us = parse_us;
                    if (cur_cmd_len > max_cmd_bytes) max_cmd_bytes = cur_cmd_len;
                    total_copy_bytes += cur_cmd_len;

                    sds cmd_name = (sds)ctx.c.argv[0]->ptr;

                    if (!strcasecmp(cmd_name, "select") && ctx.c.argc >= 2) {
                        getLongLongFromObject(ctx.c.argv[1], &dbid_from_select);
                        /* ctx.readed_backlog_offset 已由 parseCmdFromBacklog 自动推进 */
                        freeMockClientArgv(&ctx.c);  /* 释放argv，保留querybuf复用 */
                        // ctx.c.multibulklen = 0;
                        // ctx.c.bulklen = -1;
                        // ctx.c.flags = 0;

                        continue;
                    }

                    if (!strcasecmp(cmd_name, "multi")) {
                        parseMultiCommand(&build, dbid_from_select, &ctx);
                        /* ctx.readed_backlog_offset 已由 parseMultiCommand 内部自动推进 */
                        break;
                    }

                    if (!strcasecmp(cmd_name, "gtid")) {
                        parseGtidCommand(&build, &ctx.c);
                        break;
                    }
                    serverLog(LL_WARNING, "[gaplog] unexpected command %s", cmd_name);
                    serverPanic("[gaplog] unexpected command");
                }
                /* 清理argv和协议状态（不碰querybuf，数据留给下个gno复用） */
                freeMockClientArgv(&ctx.c);
                ctx.c.multibulklen = 0;
                ctx.c.bulklen = -1;
                ctx.c.flags = 0;
                total_cmds++;

                if (build.numkeys > 0) {
                    /* 计时：gaplog条目写入 */
                    ustime_t entry_start = ustime();
                    saveGapLogEntry(uuid, gno, buildGtidGapLogKeys(&build));
                    ustime_t entry_end = ustime();
                    total_entry_us += (entry_end - entry_start);

                    while (server.gtid_gap_log->size > (long long)server.gtid_xsync_max_gap) {
                        gapLogTrim(server.gtid_gap_log->size - (long long)server.gtid_xsync_max_gap);
                    }
                } else {
                    if (build.keys_infos != NULL) {
                        zfree(build.keys_infos);
                    }
                    freeGtidGaplogKeysBuilder(&build);
                }
            }  
            sdsfree(uuid);
        }
        uuidSetDeinitIterator(&us_iterator);
    }
    gtidSetDeinitIterator(&gs_iterator);

    cleanMockClient(&ctx.c);  /* 函数最外层唯一释放点，sdsfree querybuf */

    /* 输出gaplog性能统计：backlog复制+解析耗时 */
    if (total_cmds > 0) {
        serverLog(LL_WARNING,
            "[gaplog-perf] ========== Gaplog解析性能统计 ==========");
        serverLog(LL_WARNING,
            "[gaplog-perf] 总命令数: %lld", total_cmds);
        serverLog(LL_WARNING,
            "[gaplog-perf] backlog复制总字节: %lld bytes (%.2f MB)",
            total_copy_bytes, total_copy_bytes / (1024.0 * 1024.0));
        serverLog(LL_WARNING,
            "[gaplog-perf] 平均每条命令字节: %.1f bytes",
            total_cmds > 0 ? (double)total_copy_bytes / total_cmds : 0);
        serverLog(LL_WARNING,
            "[gaplog-perf] 最大单条命令字节: %zu bytes (%.2f KB)",
            max_cmd_bytes, max_cmd_bytes / 1024.0);
        serverLog(LL_WARNING,
            "[gaplog-perf] parseCmdFromBacklog总耗时: %lld us (%.2f ms)",
            total_parse_us, total_parse_us / 1000.0);
        serverLog(LL_WARNING,
            "[gaplog-perf] parseCmdFromBacklog平均耗时: %.1f us/次",
            total_cmds > 0 ? (double)total_parse_us / total_cmds : 0);
        serverLog(LL_WARNING,
            "[gaplog-perf] parseCmdFromBacklog最大耗时: %lld us (单条%.1f KB)",
            max_parse_us, max_cmd_bytes / 1024.0);
        serverLog(LL_WARNING,
            "[gaplog-perf] saveGapLogEntry总耗时: %lld us (%.2f ms)",
            total_entry_us, total_entry_us / 1000.0);
        serverLog(LL_WARNING,
            "[gaplog-perf] saveGapLogEntry平均耗时: %.1f us/次",
            total_cmds > 0 ? (double)total_entry_us / total_cmds : 0);
        serverLog(LL_WARNING,
            "[gaplog-perf] ================================================");
    }
}

skipType gtid_skip_type = {
    .freeValue = freeGtidGapLogKeys
};

int saveGapLogEntry(sds uuid, gno_t gno, gtidGapLogKeys *kis) {
    if (kis->size == 0) return 0;


    dictEntry *de = dictFind(server.gtid_gap_log->data, uuid);
    skiplist *sl;
    if (de == NULL) {
        sl = createSkipList(&gtid_skip_type);
        sds uuid_key = sdsdup(uuid);
        dictAdd(server.gtid_gap_log->data, uuid_key, sl);
    } else {
        sl = dictGetVal(de);
    }

    if (tryInsertSkipList(sl, gno, kis, 1) == 0) {
        return 0;
    }

    uuidSet *last_uuid_set = NULL;
    listNode *tail_ln = listLast(server.gtid_gap_log->history);
    if (tail_ln != NULL) {
        last_uuid_set = (uuidSet*)listNodeValue(tail_ln);
        if (last_uuid_set->uuid_len != sdslen(uuid) ||
            memcmp(last_uuid_set->uuid, uuid, sdslen(uuid)) != 0) {
            last_uuid_set = NULL;
        }
    }

    if (last_uuid_set != NULL) {
        uuidSetAdd(last_uuid_set, gno, gno);
    } else {
        uuidSet *new_uuid_set = uuidSetNew(uuid, sdslen(uuid));
        uuidSetAdd(new_uuid_set, gno, gno);
        listAddNodeTail(server.gtid_gap_log->history, new_uuid_set);
    }


    server.gtid_gap_log->size++;
    return 1;
}