#include "server.h"
#include "xredis_gtid.h"
#include "xredis_cmdparse.h"
#include "xredis_gtid_adaptation_version.h"

/* version 8.2 */


/* dict */
dict* gtidDictCreate(dictType *type) {
    return dictCreate(type);
}


/* command */

struct redisCommand* gtidLookupCommandBySds(sds name) {
    return lookupCommandBySds(name);
}

char* gtidRedisCommandGetName(struct redisCommand* cmd) {
    return cmd->fullname;
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

long long gtidGetBacklogOffset() {
    if (server.repl_backlog == NULL) return 0;
    return server.repl_backlog->offset;
}

long long gtidGetBacklogHistlen() {
    if (server.repl_backlog == NULL) return 0;
    return server.repl_backlog->histlen;
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

static inline int isWrongTypeErrorReply(const char *s, size_t len) {
    const char *swaperrmsg = "Swap failed (code=-206)";
    const char *wterrmsg = "WRONGTYPE";
    size_t swaplen = 23, wtlen = 9;

    if (len > 0 && s[0] == '-') {
        s++;
        len--;
    }

    if ( (len >= swaplen && !memcmp(s,swaperrmsg,swaplen)) ||
            (len >= wtlen && !memcmp(s,wterrmsg,wtlen)) )
        return 1;
    else
        return 0;
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

/* aof */
/**
 * @brief check feedAppendOnlyFile for more details
 *        gtid expire => gtid expireat
 *        gtid setex =>  set  + gtid expireat
 *        gtid set(ex) => set + gtid expireat
 */
void feedAppendOnlyFileGtid(struct redisCommand* _cmd,int dictid, robj **argv, int argc) {
    struct redisCommand *cmd;
    sds buf = sdsempty();

    cmd = lookupCommandByCString(argv[GTID_COMMAN_ARGC]->ptr);

    if (dictid != -1 && dictid != server.aof_selected_db) {
        char seldb[64];

        snprintf(seldb,sizeof(seldb),"%d",dictid);
        buf = sdscatprintf(buf,"*2\r\n$6\r\nSELECT\r\n$%lu\r\n%s\r\n",
            (unsigned long)strlen(seldb),seldb);
        server.aof_selected_db = dictid;
    }

    /* All commands should be propagated the same way in AOF as in replication.
     * No need for AOF-specific translation. */
    buf = catAppendOnlyGenericCommand(buf,argc,argv);

    if (server.aof_state == AOF_ON ||
        (server.aof_state == AOF_WAIT_REWRITE && server.child_type == CHILD_TYPE_AOF))
        server.aof_buf = sdscatlen(server.aof_buf,buf,sdslen(buf));

    sdsfree(buf);
}

void ctrip_feedAppendOnlyFile(struct redisCommand *cmd, int dictid,
        robj **argv, int argc) {
    if ((cmd != NULL && cmd->proc == gtidCommand) || strcasecmp(argv[0]->ptr, "gtid") == 0)
        feedAppendOnlyFileGtid(cmd, dictid,argv,argc);
    else
        feedAppendOnlyFile(dictid,argv,argc);
    
}



void propagateArgsInit(propagateArgs *pargs, struct redisCommand *cmd,
        int dbid, robj **argv, int argc) {
    pargs->orig_cmd = lookupCommand(argv, argc);
    serverAssert(pargs->orig_cmd != NULL);
    pargs->orig_argv = argv;
    pargs->orig_argc = argc;
    pargs->orig_dbid = dbid;
}

void ctrip_replicationFeedSlavesFromMasterStream(list* _save, char *buf,
        size_t buflen, const char *uuid, size_t uuid_len, gno_t gno, long long offset) {
    int touch_index = uuid != NULL && gno >= GTID_GNO_INITIAL && server.gtid_seq;
    if (touch_index) gtidSeqAppend(server.gtid_seq,uuid,uuid_len,gno,offset);
    replicationFeedStreamFromMasterStream(buf,buflen);
    /* move to repl_backlog->offset change (incrementalTrimReplicationBacklog) */
    // if (touch_index) gtidSeqTrim(server.gtid_seq,server.repl_backlog->offset);

}


int masterReplySyncRequest(client *c, long long psync_offset, syncResult *result) {
    int ret = result->action == SYNC_ACTION_FULL ? C_ERR : C_OK;

    if (result->action == SYNC_ACTION_NOP) {
        serverLog(LL_NOTICE,
                "[%s] Partial sync request from %s handle by vanilla redis.",
                replModeName(result->request_mode), replicationGetSlaveName(c));
        ret = masterTryPartialResynchronization(c, psync_offset);
    } else if (result->action == SYNC_ACTION_XCONTINUE) {
        char *buf;
        int buflen;
        const char *master_uuid;
        size_t master_uuid_len;
        sds gtid_cont_repr = gtidSetQuoteIfEmpty(gtidSetDump(result->xc.gtid_cont));
        sds gtid_lost_repr = gtidSetQuoteIfEmpty(gtidSetDump(server.gtid_lost));

        serverLog(LL_NOTICE,
                "[%s] Partial sync request from %s accepted: %s, "
                "offset=%lld, limit=%lld, gtid.set-cont=%s, gtid.set-lost=%s, master.uuid=%s",
                replModeName(result->request_mode),replicationGetSlaveName(c),
                result->msg, result->offset, result->limit,
                gtid_cont_repr, gtid_lost_repr, server.uuid);

        if (result->xc.delta_lost)
            serverReplStreamUpdateXsync(result->xc.delta_lost,c,NULL,NULL,-1);

        master_uuid = getMasterUuid(&master_uuid_len);

        buflen = sdslen(gtid_cont_repr) + sdslen(gtid_lost_repr) + 256;
        buf = zcalloc(buflen);
        buflen = snprintf(buf,buflen,
                "+XCONTINUE GTID.SET %.*s GTID.LOST %.*s MASTER.UUID %.*s "
                "REPLID %s REPLOFF %lld\r\n",
                (int)sdslen(gtid_cont_repr),gtid_cont_repr,
                (int)sdslen(gtid_lost_repr),gtid_lost_repr,
                (int)master_uuid_len,master_uuid,
                result->xc.replid,result->xc.reploff);
        masterSetupPartialSynchronization(c,result->offset,
                result->limit,buf,buflen);
        sdsfree(gtid_cont_repr);
        sdsfree(gtid_lost_repr);
        zfree(buf);
    } else if (result->action == SYNC_ACTION_CONTINUE) {
        char buf[128];
        int buflen;

        serverLog(LL_NOTICE, "[%s] Partial sync request from %s accepted: %s, "
                "offset=%lld, limit=%lld, cc.replid=%s, cc.reploff=%lld",
                replModeName(result->request_mode),replicationGetSlaveName(c),
                result->msg, result->offset, result->limit,
                result->cc.replid, result->cc.reploff);

        if (result->cc.delta_lost)
            serverReplStreamUpdateXsync(result->cc.delta_lost,c,NULL,NULL,-1);

        if (result->cc.reploff < 0) {
            buflen = snprintf(buf,sizeof(buf),"+CONTINUE %s\r\n",
                    result->cc.replid);
        } else {
            buflen = snprintf(buf,sizeof(buf),"+CONTINUE %s %lld\r\n",
                    result->cc.replid, result->cc.reploff);
        }
        masterSetupPartialSynchronization(c,result->offset,
                result->limit,buf,buflen);
    } else {
        serverLog(LL_NOTICE, "[%s] Partial sync request from %s rejected: %s",
                replModeName(result->request_mode),replicationGetSlaveName(c),
                result->msg);
    }

    return ret;
}

int ctrip_masterTryPartialResynchronization(client *c, long long psync_offset) {
    syncRequest *request = masterParseSyncRequest(c);
    syncResult *result = masterAnaSyncRequest(request);
    int ret = masterReplySyncRequest(c,psync_offset,result);
    syncRequestFree(request);
    syncResultFree(result);
    return ret;
}

void ctrip_resizeReplicationBacklog(long long newsize) {
    long long oldsize = server.repl_backlog_size;
    resizeReplicationBacklog();
    if (server.repl_backlog != NULL && oldsize != server.repl_backlog_size) {
        /* realloc a new gtidSeq to keep gtid_seq sync with backlog, see
         * resizeReplicationBacklog for more details. */
        gtidSeqDestroy(server.gtid_seq);
        server.gtid_seq = gtidSeqCreate();
    }
}

/* Feed the slave 'c' with the replication backlog starting from the
 * specified 'offset' up to the end of the backlog. */
long long consumeReplicationBacklogLimited(long long offset, long long limit,
        consume_cb cb, void *pd) {
    long long skip;
    long long added = 0;
    if (server.repl_backlog->histlen == 0) {
        serverLog(LL_DEBUG, "[PSYNC] Backlog history len is zero");
        return 0;
    }
    /* Compute the amount of bytes we need to discard. */
    skip = offset - server.repl_backlog->offset;
    long long len = server.repl_backlog->histlen - skip;
    len = len < limit ? len: limit;
    /* Iterate recorded blocks, quickly search the approximate node. */
    listNode *node = NULL;
    if (raxSize(server.repl_backlog->blocks_index) > 0) {
        uint64_t encoded_offset = htonu64(offset);
        raxIterator ri;
        raxStart(&ri, server.repl_backlog->blocks_index);
        raxSeek(&ri, ">", (unsigned char*)&encoded_offset, sizeof(uint64_t));
        if (raxEOF(&ri)) {
            /* No found, so search from the last recorded node. */
            raxSeek(&ri, "$", NULL, 0);
            raxPrev(&ri);
            node = (listNode *)ri.data;
        } else {
            raxPrev(&ri); /* Skip the sought node. */
            /* We should search from the prev node since the offset of current
             * sought node exceeds searching offset. */
            if (raxPrev(&ri))
                node = (listNode *)ri.data;
            else
                node = server.repl_backlog->ref_repl_buf_node;
        }
        raxStop(&ri);
    } else {
        /* No recorded blocks, just from the start node to search. */
        node = server.repl_backlog->ref_repl_buf_node;
    }

    /* Search the exact node. */
    while (node != NULL) {
        replBufBlock *o = listNodeValue(node);
        if (o->repl_offset + (long long)o->used >= offset) break;
        node = listNextNode(node);
    }
    serverAssert(node != NULL);

    serverLog(LL_WARNING, "start copy backlog offset(%lld) len(%lld)", offset, len);
    while (len) {
        replBufBlock *o = listNodeValue(node);
        int start = 0;
        if (offset > o->repl_offset) {
            start = offset - o->repl_offset;
        }
        long long thislen = o->used - start  < len?
                o->used - start: len;
        serverLog(LL_WARNING, "block start(%lld), offset(%lld) size(%lld)", o->repl_offset, start, thislen);
        cb(o->buf + start, thislen, pd);
        len -= thislen;
        start = 0;
        added += thislen;
        node = listNextNode(node);
    }
    serverLog(LL_WARNING, "added=%lld", added);
    return added;
}

/* see masterTryPartialResynchronization for more details. */
void masterSetupPartialSynchronization(client *c, long long offset,
        long long limit, char *buf, int buflen) {
    long long sent;

    if (server.repl_backlog == NULL) ctrip_createReplicationBacklog();

    c->flags |= CLIENT_SLAVE;
    c->replstate = SLAVE_STATE_ONLINE;
    c->repl_ack_time = server.unixtime;
    listAddNodeTail(server.slaves,c);

    if (connWrite(c->conn,buf,buflen) != buflen) {
        freeClientAsync(c);
        return;
    }

    serverAssert(offset >= server.repl_backlog->offset);
    if (limit > 0) {
        sent = addReplyReplicationBacklogLimited(c,offset,limit);
    } else {
        sent = addReplyReplicationBacklog(c,offset);
    }

    serverLog(LL_NOTICE,
        "[gtid] Sent %lld bytes of backlog starting from offset %lld limit %lld.",
        sent, offset, limit);


    /* Note that we don't need to set the selected DB at server.slaveseldb
     * to -1 to force the master to emit SELECT:
     * a) xcontinue: db selectd by gtid argv
     * b) continue : db already saved in cached_master */

    refreshGoodSlavesCount();

    moduleFireServerEvent(REDISMODULE_EVENT_REPLICA_CHANGE,
                          REDISMODULE_SUBEVENT_REPLICA_CHANGE_ONLINE,
                          NULL);
}

int gtidIsInMulti() {
    return server.gtid_dbid_at_multi != -1;
}

void gtidFreeReplicationBacklog() {
    freeReplicationBacklog();
}
void gtidCreateReplicationBacklog() {
    createReplicationBacklog();
}

sds sendXsyncCommand(connection *conn) {
    char maxgap[32];
    gtidSet *gtid_slave = NULL;
    sds gtid_slave_repr, gtid_lost_repr;
    gtid_slave = serverGtidSetGet("[xsync]");
    gtid_slave_repr = gtidSetDump(gtid_slave);
    gtid_lost_repr = gtidSetDump(server.gtid_lost);
    const char *uuid_interested = xsyncUuidInterestedGet();

    snprintf(maxgap,sizeof(maxgap),"%lld",server.gtid_xsync_max_gap);
    serverLog(LL_NOTICE, "[xsync] Trying partial xsync with "
            "uuid_interested=%s, gtid.set=%s, gtid.lost=%s, maxgap=%s",
            uuid_interested,gtid_slave_repr,gtid_lost_repr,maxgap);

    sds reply = sendCommand(conn,"XSYNC",uuid_interested,
            gtid_slave_repr,"GTID.LOST",gtid_lost_repr,"MAXGAP",maxgap,NULL);
    gtidSetFree(gtid_slave);
    sdsfree(gtid_slave_repr);
    sdsfree(gtid_lost_repr);
    return reply;
} 