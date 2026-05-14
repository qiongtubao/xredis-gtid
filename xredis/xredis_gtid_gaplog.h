/* Copyright (c) 2026, ctrip.com
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef __GTID_GAPLOG_H__
#define __GTID_GAPLOG_H__

#include <stdint.h>
#include <stddef.h>
#include "server.h"

/* Error codes */
#define GAPLOG_OK 0
#define GAPLOG_ERR -1
#define GAPLOG_ERR_MEMORY -2
#define GAPLOG_ERR_NOT_FOUND -3
#define GAPLOG_ERR_INVALID_PARAM -4

/* Default configuration */
#define GAPLOG_DEFAULT_MAX_GAP 10000      /* 每个UUID默认保留的最大gap数 */

/* ============================================================================
 * Data Structures - 环形缓冲区版本：O(1)插入和删除，避免数组移动
 * ============================================================================ */

/**
 * 单个gno到key的映射条目
 * 优化：存储robj指针并增加引用计数，避免深拷贝
 */
typedef struct gtidGnoEntry {
    gno_t gno;               /* gno号 */
    robj **keys;             /* key数组（robj指针数组，增加引用计数） */
    robj **subkeys;          /* subkey数组（可选，robj指针数组） */
    size_t key_count;        /* key数量 */
    long long timestamp;     /* 记录时间戳 */
} gtidGnoEntry;

/**
 * UUID索引条目 - 环形缓冲区版本
 * 优化：使用环形缓冲区，插入和删除都是O(1)，不需要移动数组
 *
 * 环形缓冲区原理：
 * - 固定容量数组，head指向最老条目，tail指向下一个插入位置
 * - 插入：entries[tail] = new_entry; tail = (tail + 1) % capacity
 * - 删除最老：destroy(entries[head]); head = (head + 1) % capacity
 * - 查找：由于gno递增，从tail-1向前遍历即可（通常很快找到）
 */
typedef struct gtidUuidEntry {
    gtidGnoEntry **entries;  /* 环形缓冲区：gno条目数组 */
    size_t capacity;         /* 缓冲区容量（固定，创建时确定） */
    size_t head;             /* 头指针：最老条目位置 */
    size_t tail;             /* 尾指针：下一个插入位置 */
    size_t count;            /* 当前条目数（tail - head，考虑环形） */
    gno_t min_gno;           /* 最小gno（entries[head]->gno） */
    gno_t max_gno;           /* 最大gno（entries[(tail-1)%capacity]->gno） */
} gtidUuidEntry;

/**
 * gtid到key的单条映射记录（用于遍历输出）
 * 优化：直接引用内部数据，不深拷贝
 */
typedef struct gtidKeyMapping {
    sds uuid;                /* uuid字符串（引用，不拥有） */
    gno_t gno;               /* gno号 */
    sds *keys;               /* key数组（sds引用，不拥有） */
    sds *subkeys;            /* subkey数组（sds引用，不拥有） */
    size_t key_count;        /* key数量 */
    long long timestamp;     /* 记录时间戳 */
} gtidKeyMapping;

/**
 * Gaplog统计信息
 */
typedef struct gtidGaplogStat {
    size_t total_entries;    /* 总条目数 */
    size_t total_memory;     /* 总内存占用 */
    size_t uuid_count;       /* 不同uuid数量 */
    size_t hit_count;        /* 查询命中次数 */
    size_t miss_count;       /* 查询未命中次数 */
} gtidGaplogStat;

/**
 * Gaplog主结构
 * 优化：使用sds作为dict key，避免额外的字符串拷贝
 */
typedef struct gtidGaplog {
    dict *uuid_index;        /* uuid(sds) -> gtidUuidEntry索引 */
    size_t max_gap;          /* 每个UUID保留的最大gap数 */
    size_t used_memory;      /* 已使用内存 */
    size_t hit_count;        /* 查询命中次数 */
    size_t miss_count;       /* 查询未命中次数 */
    size_t total_entries;    /* 总条目数 */

    /* 优化：缓存上次使用的uuid_entry，避免重复dictFind */
    sds cached_uuid;         /* 缓存的uuid（指针，不拥有） */
    size_t cached_uuid_len;  /* 缓存uuid的长度 */
    struct gtidUuidEntry *cached_entry; /* 缓存的uuid_entry */

    /* 优化：gtidGnoEntry对象池，避免频繁zmalloc/zfree */
    struct gtidGnoEntryExt *entry_pool;  /* 对象池链表头 */
    size_t entry_pool_count;  /* 对象池当前大小 */
} gtidGaplog;

/* ============================================================================
 * Core API Functions
 * ============================================================================ */

/**
 * 创建gaplog实例
 */
gtidGaplog *gtidGaplogCreate(size_t max_gap);

/**
 * 销毁gaplog实例
 */
void gtidGaplogDestroy(gtidGaplog *gaplog);

/**
 * 添加gtid到key的映射（高性能版本）
 * @param gaplog gaplog实例
 * @param uuid uuid字符串（sds，会被引用不拷贝）
 * @param gno gno号
 * @param keys key的robj数组（会增加引用计数）
 * @param subkeys subkey的robj数组（可选，可为NULL）
 * @param key_count key数量
 * @return GAPLOG_OK成功，其他值失败
 */
int gtidGaplogAppend(gtidGaplog *gaplog, sds uuid, gno_t gno,
                     robj **keys, robj **subkeys, size_t key_count);

/**
 * 根据gtid查询对应的key映射
 * @return gno条目指针，未找到返回NULL
 */
gtidGnoEntry *gtidGaplogGet(gtidGaplog *gaplog, sds uuid, gno_t gno);

/**
 * 清理指定UUID中gno小于watermark的条目
 */
size_t gtidGaplogTrim(gtidGaplog *gaplog, sds uuid, gno_t watermark);

/**
 * 清空gaplog
 */
void gtidGaplogClear(gtidGaplog *gaplog);

/**
 * 获取gaplog统计信息
 */
void gtidGaplogGetStat(gtidGaplog *gaplog, gtidGaplogStat *stat);

/**
 * 列出gaplog条目
 */
gtidKeyMapping **gtidGaplogList(gtidGaplog *gaplog, sds uuid,
                                 size_t count, size_t *actual_count);

/**
 * 获取总条目数
 */
size_t gtidGaplogGetCount(gtidGaplog *gaplog);

/**
 * 设置每个UUID保留的最大gap数
 */
void gtidGaplogSetMaxGap(gtidGaplog *gaplog, size_t max_gap);

#endif /* __GTID_GAPLOG_H__ */