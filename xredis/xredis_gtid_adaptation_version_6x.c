#include "server.h"
#include "xredis_gtid.h"
#include "xredis_cmdparse.h"

/* version 6.x*/

ssize_t gtidBacklogAppendToSds(long long offset, sds *dst, size_t size) {
    if (server.repl_backlog == NULL || server.repl_backlog_histlen == 0)
        return -1;

    long long skip = offset - server.repl_backlog_off;
    if (skip < 0 || skip >= server.repl_backlog_histlen) return -1;

    long long available = server.repl_backlog_histlen - skip;
    if (available <= 0) return -1;
    if ((long long)size > available) size = (size_t)available;

    long long j = (server.repl_backlog_idx +
                   (server.repl_backlog_size - server.repl_backlog_histlen)) %
                   server.repl_backlog_size;
    j = (j + skip) % server.repl_backlog_size;

    *dst = sdsMakeRoomFor(*dst, size);

    size_t total = 0;
    while (total < size) {
        size_t thislen = server.repl_backlog_size - j;
        if (thislen > size - total) thislen = size - total;
        memcpy((*dst) + sdslen(*dst) + total, server.repl_backlog + j, thislen);
        total += thislen;
        j = 0;
    }
    sdsIncrLen(*dst, total);

    return (ssize_t)total;
}


static void gtidGaplogKeysBuilderAdd(gtidGaplogKeysBuilder *builder, int dbid, int type, sds key,
                       sds *subkeys, int subkeys_count)
{
    gtidGaplogKeysPrepareBuilder(builder, 1);
    gtidGaplogKey *key_result = gtidGaplogKeyNew(dbid, type, key, subkeys, subkeys_count);
    builder->keys_infos[builder->numkeys++] = key_result;
}

int cmdGetKeyType(struct redisCommand *cmd) {
    if (cmd == NULL) return OBJ_UNKNOWN;
    if (cmd->flags & CMD_CATEGORY_STRING) return OBJ_STRING;
    if (cmd->flags & CMD_CATEGORY_LIST) return OBJ_LIST;
    if (cmd->flags & CMD_CATEGORY_HASH) return OBJ_HASH;
    if (cmd->flags & CMD_CATEGORY_SET) return OBJ_SET;
    if (cmd->flags & CMD_CATEGORY_SORTEDSET) return OBJ_ZSET;
    if (cmd->flags & CMD_CATEGORY_BITMAP) return OBJ_STRING;
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


void gtidMockClientInit(client* mock) {
    mock->querybuf = sdsempty();
    mock->authenticated = 1;
    mock->argv = NULL;
    mock->argc = 0;
    mock->qb_pos = 0;
    mock->flags = 0;
    mock->bulklen =- 1;
    mock->multibulklen = 0;
}

void gtidMockClientCleanArgv(client* mock) {
    if (mock->argv) {
        for (int i = 0; i < mock->argc; i++)
            if (mock->argv[i]) decrRefCount(mock->argv[i]);
        zfree(mock->argv);
        mock->argv = NULL;
        mock->argc = 0;
    }
}

void gtidMockClientDeinit(client* mock) {
    gtidMockClientCleanArgv(mock);
    sdsfree(mock->querybuf);
    mock->querybuf = NULL;
    mock->qb_pos = 0;
}

/* aof */

/**
 * @brief check feedAppendOnlyFile for more details
 *        gtid expire => gtid expireat
 *        gtid setex =>  set  + gtid expireat
 *        gtid set(ex) => set + gtid expireat
 */
void feedAppendOnlyFileGtid(struct redisCommand *_cmd, int dictid, robj **argv, int argc) {
    struct redisCommand *cmd;
    sds buf = sdsempty();

    serverAssert(_cmd == server.gtidCommand);
    cmd = gtidLookupCommandBySds(argv[GTID_COMMAN_ARGC]->ptr);

    if (dictid != server.aof_selected_db) {
        char seldb[64];

        snprintf(seldb,sizeof(seldb),"%d",dictid);
        buf = sdscatprintf(buf,"*2\r\n$6\r\nSELECT\r\n$%lu\r\n%s\r\n",
            (unsigned long)strlen(seldb),seldb);
        server.aof_selected_db = dictid;
    }

    if (cmd->proc == expireCommand || cmd->proc == pexpireCommand ||
            cmd->proc == expireatCommand) {
        buf = catAppendOnlyGtidExpireAtCommand(buf,argv[1],argv[2],NULL,cmd,
                argv[GTID_COMMAN_ARGC+1],argv[GTID_COMMAN_ARGC+2]);
    } else if (cmd->proc == setCommand && argc > 3 + GTID_COMMAN_ARGC) {
        robj *pxarg = NULL;
        if (!strcasecmp(argv[3 + GTID_COMMAN_ARGC]->ptr, "px")) {
            pxarg = argv[4 + GTID_COMMAN_ARGC];
        }
        if (pxarg) {
            robj *millisecond = getDecodedObject(pxarg);
            long long when = strtoll(millisecond->ptr,NULL,10);
            when += mstime();

            decrRefCount(millisecond);

            robj *newargs[5 + GTID_COMMAN_ARGC];
            for(int i = 0, len = 3 + GTID_COMMAN_ARGC; i < len; i++) {
                newargs[i] = argv[i];
            }
            newargs[3 + GTID_COMMAN_ARGC] = shared.pxat;
            newargs[4 + GTID_COMMAN_ARGC] = createStringObjectFromLongLong(when);
            buf = catAppendOnlyGenericCommand(buf,5 + GTID_COMMAN_ARGC,newargs);
            decrRefCount(newargs[4 + GTID_COMMAN_ARGC]);
        } else {
            buf = catAppendOnlyGenericCommand(buf,argc,argv);
        }
    } else {
        buf = catAppendOnlyGenericCommand(buf,argc,argv);
    }

    if (server.aof_state == AOF_ON)
        server.aof_buf = sdscatlen(server.aof_buf,buf,sdslen(buf));

    if (server.child_type == CHILD_TYPE_AOF)
        aofRewriteBufferAppend((unsigned char*)buf,sdslen(buf));

    sdsfree(buf);
}

void ctrip_feedAppendOnlyFile(struct redisCommand *cmd, int dictid,
        robj **argv, int argc) {
    if (cmd == server.gtidCommand || strcasecmp(argv[0]->ptr, "gtid") == 0)
        feedAppendOnlyFileGtid(dictid,argv,argc);
    else
        feedAppendOnlyFile(cmd, dictid,argv,argc);
    
}


void propagateArgsInit(propagateArgs *pargs, struct redisCommand *cmd,
        int dbid, robj **argv, int argc) {
    pargs->orig_cmd = cmd;
    pargs->orig_argv = argv;
    pargs->orig_argc = argc;
    pargs->orig_dbid = dbid;
}

void ctrip_replicationFeedSlavesFromMasterStream(list *slaves, char *buf,
        size_t buflen, const char *uuid, size_t uuid_len, gno_t gno, long long offset) {
    int touch_index = uuid != NULL && gno >= GTID_GNO_INITIAL && server.gtid_seq;
    if (touch_index) gtidSeqAppend(server.gtid_seq,uuid,uuid_len,gno,offset);
    replicationFeedSlavesFromMasterStream(slaves,buf,buflen);
    if (touch_index) gtidSeqTrim(server.gtid_seq,server.repl_backlog_off);
}




int masterReplySyncRequest(client *c, syncResult *result) {
    int ret = result->action == SYNC_ACTION_FULL ? C_ERR : C_OK;

    if (result->action == SYNC_ACTION_NOP) {
        serverLog(LL_NOTICE,
                "[%s] Partial sync request from %s handle by vanilla redis.",
                replModeName(result->request_mode), replicationGetSlaveName(c));
        ret = masterTryPartialResynchronization(c);
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
    int ret = masterReplySyncRequest(c,result);
    syncRequestFree(request);
    syncResultFree(result);
    return ret;
}

long long gtidGetBacklogOffset() {
    return server.repl_backlog_off;
}

long long gtidGetBacklogHistlen() {
    return server.repl_backlog_histlen;
}

/* Check adReplyReplicationBacklog for more details */
long long consumeReplicationBacklogLimited(long long offset, long long limit,
         consume_cb cb, void *pd) {
    long long added = 0, j, skip, len;
    serverAssert(limit >= 0 && offset >= gtidGetBacklogOffset());
    if (gtidGetBacklogHistlen() == 0) return 0;
    skip = offset - gtidGetBacklogOffset();
    j = (server.repl_backlog_idx +
        (server.repl_backlog_size-gtidGetBacklogHistlen())) %
        server.repl_backlog_size;
    j = (j + skip) % server.repl_backlog_size;
    len = gtidGetBacklogHistlen() - skip;
    len = len < limit ? len : limit; /* limit bytes to copy */
    while(len) {
        long long thislen =
            ((server.repl_backlog_size - j) < len) ?
            (server.repl_backlog_size - j) : len;
        cb(server.repl_backlog + j, thislen, pd);
        len -= thislen;
        j = 0;
        added += thislen;
    }

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
    c->repl_put_online_on_ack = 0;
    listAddNodeTail(server.slaves,c);

    if (connWrite(c->conn,buf,buflen) != buflen) {
        freeClientAsync(c);
        return;
    }

    if (limit > 0) {
        sent = addReplyReplicationBacklogLimited(c,offset,limit);
    } else {
        sent = addReplyReplicationBacklog(c,offset);
    }

    serverLog(LL_NOTICE,
        "[gtid] Sent %lld bytes of backlog starting from offset %lld limit %lld.",
        sent, offset, limit);

    if (limit > 0) {
        c->flags |= CLIENT_CLOSE_AFTER_REPLY;
        serverLog(LL_NOTICE,
                "[gtid] Disconnect slave %s to notify repl mode switched.",
                replicationGetSlaveName(c));
        return;
    }

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
    zfree(server.repl_backlog);
}
void gtidCreateReplicationBacklog() {
    server.repl_backlog = zmalloc(server.repl_backlog_size);
    server.repl_backlog_histlen = 0;
    server.repl_backlog_idx = 0;
    server.repl_backlog_off = server.master_repl_offset+1;
}

void ctrip_resizeReplicationBacklog(long long newsize) {
    long long oldsize = server.repl_backlog_size;
    resizeReplicationBacklog(newsize);
    if (server.repl_backlog != NULL && oldsize != server.repl_backlog_size) {
        /* realloc a new gtidSeq to keep gtid_seq sync with backlog, see
         * resizeReplicationBacklog for more details. */
        gtidSeqDestroy(server.gtid_seq);
        server.gtid_seq = gtidSeqCreate();
    }
}