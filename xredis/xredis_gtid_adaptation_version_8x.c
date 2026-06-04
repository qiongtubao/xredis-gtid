#include "server.h"
#include "xredis_gtid.h"
#include "xredis_cmdparse.h"

/* version 8.2 */


/* dict */
dict* gtidDictCreate(dictType *type) {
    return dictCreate(type);
}


/* command */

struct redisCommand* gtidLookupCommandBySds(sds name) {
    return lookupCommandBySds(name);
}

/* robj */
char* gtidGetObjectTypeName(int key_type) {
    if (key_type == OBJ_UNKNOWN) {
        return "unknow";
    }
    robj o = {.type = key_type};
    return getObjectTypeName(&o);

}

/* getKeysResult */
int gtidGetKeysResultKeyIndex(getKeysResult* result, int index) {
    return result->keys[index].pos;
}


/* backlog*/
/* 将backlog中offset位置开始的至多size字节数据追加到dst指向的sds末尾
 * 返回实际拷贝的字节数，出错返回(size_t)-1 */
size_t gtidBacklogAppendToSds(long long offset, sds *dst, size_t size) {
    /* 检查backlog是否存在且非空 */
    if (server.repl_backlog == NULL || server.repl_backlog->histlen == 0)
        return -1;

    long long skip = offset - server.repl_backlog->offset;
    /* offset必须在backlog范围内 */
    if (skip < 0 || skip >= server.repl_backlog->histlen) return -1;

    long long available = server.repl_backlog->histlen - skip;
    if (available <= 0) return -1;
    if ((long long)size > available) size = (size_t)available;

    /* 利用blocks_index快速定位到包含offset的近似节点 */
    listNode *node = NULL;
    if (raxSize(server.repl_backlog->blocks_index) > 0) {
        uint64_t encoded_offset = htonu64(offset);
        raxIterator ri;
        raxStart(&ri, server.repl_backlog->blocks_index);
        raxSeek(&ri, ">", (unsigned char*)&encoded_offset, sizeof(uint64_t));
        if (raxEOF(&ri)) {
            /* offset大于所有索引节点，从最后一个索引节点开始 */
            raxSeek(&ri, "$", NULL, 0);
            raxPrev(&ri);
            node = (listNode *)ri.data;
        } else {
            /* 当前节点offset > 目标offset，向前退一个 */
            raxPrev(&ri);
            if (raxPrev(&ri))
                node = (listNode *)ri.data;
            else
                node = server.repl_backlog->ref_repl_buf_node;
        }
        raxStop(&ri);
    } else {
        node = server.repl_backlog->ref_repl_buf_node;
    }

    /* 从近似节点开始精确遍历，找到包含offset的节点 */
    while (node != NULL) {
        replBufBlock *o = listNodeValue(node);
        if (o->repl_offset + (long long)o->used > offset) break;
        node = listNextNode(node);
    }
    if (node == NULL) return -1;

    /* 确保sds有足够空间追加数据 */
    *dst = sdsMakeRoomFor(*dst, size);

    size_t total = 0;
    while (total < size && node != NULL) {
        replBufBlock *o = listNodeValue(node);
        /* 第一个节点需要跳过offset之前的字节，后续节点从头部开始拷贝 */
        size_t block_skip = (total == 0) ? (offset - o->repl_offset) : 0;
        size_t thislen = o->used - block_skip;
        if (thislen > size - total) thislen = size - total;

        memcpy((*dst) + sdslen(*dst) + total, o->buf + block_skip, thislen);
        total += thislen;
        node = listNextNode(node);
    }
    sdsIncrLen(*dst, total);

    return total;
}

// size_t gtidBacklogAppendToSds(long long offset, sds *dst, size_t size) {
//     if (server.repl_backlog == NULL || server.repl_backlog_histlen == 0)
//         return -1;

//     long long skip = offset - server.repl_backlog_off;
//     if (skip < 0 || skip >= server.repl_backlog_histlen) return -1;

//     long long available = server.repl_backlog_histlen - skip;
//     if (available <= 0) return -1;
//     if ((long long)size > available) size = (size_t)available;

//     long long j = (server.repl_backlog_idx +
//                    (server.repl_backlog_size - server.repl_backlog_histlen)) %
//                    server.repl_backlog_size;
//     j = (j + skip) % server.repl_backlog_size;

//     *dst = sdsMakeRoomFor(*dst, size);

//     size_t total = 0;
//     while (total < size) {
//         size_t thislen = server.repl_backlog_size - j;
//         if (thislen > size - total) thislen = size - total;
//         memcpy((*dst) + sdslen(*dst) + total, server.repl_backlog + j, thislen);
//         total += thislen;
//         j = 0;
//     }
//     sdsIncrLen(*dst, total);

//     return (ssize_t)total;
// }


void gtidGaplogKeysBuilderAdd(gtidGaplogKeysBuilder *builder, int dbid, int type, sds key,
                       sds *subkeys, int subkeys_count)
{
    gtidGaplogKeysPrepareBuilder(builder, 1);
    gtidGaplogKey *key_result = gtidGaplogKeyNew(dbid, type, key, subkeys, subkeys_count);
    builder->keys_infos[builder->numkeys++] = key_result;
}

int cmdGetKeyType(struct redisCommand *cmd) {
    if (cmd == NULL) return OBJ_UNKNOWN;
    if (cmd->group == COMMAND_GROUP_STRING) return OBJ_STRING;
    if (cmd->group == COMMAND_GROUP_LIST) return OBJ_LIST;
    if (cmd->group == COMMAND_GROUP_HASH) return OBJ_HASH;
    if (cmd->group == COMMAND_GROUP_SET) return OBJ_SET;
    if (cmd->group == COMMAND_GROUP_SORTED_SET) return OBJ_ZSET;
    if (cmd->group == COMMAND_GROUP_BITMAP) return OBJ_STRING;

    // if (cmd->flags & ACL_CATEGORY_STRING) return OBJ_STRING;
    // if (cmd->flags & ACL_CATEGORY_LIST) return OBJ_LIST;
    // if (cmd->flags & ACL_CATEGORY_HASH) return OBJ_HASH;
    // if (cmd->flags & ACL_CATEGORY_SET) return OBJ_SET;
    // if (cmd->flags & ACL_CATEGORY_SORTEDSET) return OBJ_ZSET;
    // if (cmd->flags & ACL_CATEGORY_BITMAP) return OBJ_STRING;
    return OBJ_UNKNOWN;
}
static void gtidOnKey(void *ctx, int dbid, struct redisCommand* cmd, robj** argv, int argc,  int key_arg_idx,
                      int subkeys_count, int subkeys_start,
                      int subkeys_step, const int *subkey_arg_idxs,
                      const cmdParseKeyExtra *extra)
{
    UNUSED(extra);
    UNUSED(argc);
    gtidGaplogKeysBuilder *builder = ctx;
    sds key = sdsdup((sds)argv[key_arg_idx]->ptr);
    sds *subkeys = subkeys_count > 0 ? zmalloc(sizeof(sds) * subkeys_count) : NULL;
    for (int i = 0; i < subkeys_count; i++) {
        int subkey_idx = subkey_arg_idxs ? subkey_arg_idxs[i] : (subkeys_start + i * subkeys_step);
        subkeys[i] = sdsdup((sds)argv[subkey_idx]->ptr);
    }
    gtidGaplogKeysBuilderAdd(builder, dbid, cmdGetKeyType(cmd), key, subkeys, subkeys_count);
}

void gtidGaplogKeysBuilderAddFromCmd(gtidGaplogKeysBuilder *builder, int dbid, robj **args, int argc) {
    if (argc < 2) return;
    serverAssert( builder != NULL);
    cmdParseKeys(dbid, NULL, args, argc, builder, gtidOnKey);
}



/* client */
void gtidMockClientInit(client* c) {
    c->querybuf = sdsempty();
    c->authenticated = 1;
    c->argv = NULL;
    c->argc = 0;
    c->qb_pos = 0;
    c->flags = 0;
    c->bulklen =- 1;
    c->multibulklen = 0;
    c->argv_len = 0;
}
void gtidMockClientDeinit(client* c) {
    gtidMockClientCleanArgv(c);
    zfree(c->argv);
    c->argv_len = 0;
    sdsfree(c->querybuf);
    c->querybuf = NULL;
    c->qb_pos = 0;
}
void gtidMockClientCleanArgv(client* c) {
    if (c->argv) {
        for (int i = 0; i < c->argc; i++)
            if (c->argv[i]) decrRefCount(c->argv[i]);
        c->argc = 0;
    }
}

void gtidMockClientMoveClientArgv(client* c) {
    c->argc = 0;
    c->argv = NULL;
    c->argv_len = 0;
}


void ctrip_afterErrorReply(client *c, const char *s, size_t len, int flags) {
    afterErrorReply(c,s,len, flags);
    if (server.repl_mode->mode != REPL_MODE_XSYNC) return;
    /* Replica sending wrong type error to master indicates data
     * inconsistent, * force fullresync to fix it. */
    if (getClientType(c) == CLIENT_TYPE_MASTER &&
            isWrongTypeErrorReply(s,len)) {
        server.gtid_xsync_fullresync_indicator++;
    }
}