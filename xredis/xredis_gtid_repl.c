/* Copyright (c) 2025, ctrip.com
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

#include "server.h"
#include <gtid.h>
#include <ctype.h>

/* gtidGapLogGnoEntry: 内部结构体（定义在 xredis_gtid.c 中） */
typedef struct gtidGapLogGnoEntry {
    long long gno;
    struct gtidGapLogKeysInfos *keys_infos;
} gtidGapLogGnoEntry;

/* 内部函数声明（定义在 xredis_gtid.c 中） */
extern gtidGapLogGnoEntry *gtidGapLogGnoEntryCreate(long long gno, struct gtidGapLogKeysInfos *keys_infos);
extern void gtidGapLogGnoEntryFree(void *ptr);

int replicationSetupSlaveForXFullResync(client *slave, long long offset) {
    int ret = C_OK;
    sds gtid_lost_repr = NULL, repr = NULL;
    size_t master_uuid_len = 0;
    const char *master_uuid = getMasterUuid(&master_uuid_len);

    repr = sdsnew("+XFULLRESYNC");

    gtid_lost_repr = gtidSetQuoteIfEmpty(gtidSetDump(server.gtid_lost));
    repr = sdscat(repr," GTID.LOST ");
    repr = sdscatlen(repr,gtid_lost_repr,sdslen(gtid_lost_repr));

    repr = sdscat(repr," MASTER.UUID ");
    repr = sdscatlen(repr,master_uuid,master_uuid_len);

    repr = sdscat(repr," REPLID ");
    repr = sdscat(repr,server.replid);

    repr = sdscat(repr," REPLOFF ");
    sds reploff = sdsfromlonglong(ctrip_getMasterReploff());
    repr = sdscatsds(repr,reploff);
    sdsfree(reploff);

    repr = sdscat(repr, "\r\n");

    slave->psync_initial_offset = offset;
    slave->replstate = SLAVE_STATE_WAIT_BGSAVE_END;
    /* We are going to accumulate the incremental changes for this
     * slave as well. Set slaveseldb to -1 in order to force to re-emit
     * a SELECT statement in the replication stream. */
    server.slaveseldb = -1;

    /* Don't send this reply to slaves that approached us with
     * the old SYNC command. */
    if (!(slave->flags & CLIENT_PRE_PSYNC)) {
        if (connWrite(slave->conn,repr,sdslen(repr)) != (int)sdslen(repr)) {
            freeClientAsync(slave);
            ret = C_ERR;
            goto end;
        }
    }

end:
    sdsfree(gtid_lost_repr), sdsfree(repr);
    return ret;
}

int ctrip_replicationSetupSlaveForFullResync(client *slave, long long offset) {
    if (server.repl_mode->mode != REPL_MODE_XSYNC)
        return replicationSetupSlaveForFullResync(slave, offset);
    else
        return replicationSetupSlaveForXFullResync(slave, offset);
}

#define GTID_XSYNC_MAX_REPLY_SIZE (64*1024)

/* XCONTINUE reply could exceed 256 byte. */
char *ctrip_receiveSynchronousResponse(connection *conn) {
    char *buf = zcalloc(GTID_XSYNC_MAX_REPLY_SIZE);
    if (connSyncReadLine(conn,buf,GTID_XSYNC_MAX_REPLY_SIZE,
                server.repl_syncio_timeout*1000) == -1)
    {
        zfree(buf);
        return sdscatprintf(sdsempty(),"-Reading from master: %s",
                strerror(errno));
    }
    server.repl_transfer_lastio = server.unixtime;
    sds response = sdsnew(buf);
    zfree(buf);
    return response;
}

typedef struct syncRequest {
    int mode;
    union {
        struct {
            sds replid;
            long long offset;
        } p; /* psync */
        struct {
            sds uuid_interested;
            gtidSet *gtid_slave;
            gtidSet *gtid_lost;
            long long maxgap;
        } x; /* xsync */
        struct {
            sds msg;
        } i; /* invalid */
    };
} syncRequest;

syncRequest *syncRequestNew() {
    syncRequest *request = zcalloc(sizeof(syncRequest));
    request->mode = REPL_MODE_UNSET;
    return request;
}

void syncRequestFree(syncRequest *request) {
    if (request == NULL) return;
    switch (request->mode) {
    case REPL_MODE_PSYNC:
        sdsfree(request->p.replid);
        break;
    case REPL_MODE_XSYNC:
        sdsfree(request->x.uuid_interested);
        gtidSetFree(request->x.gtid_slave);
        gtidSetFree(request->x.gtid_lost);
        break;
    case REPL_MODE_UNSET:
        sdsfree(request->i.msg);
        break;
    default:
        serverPanic("unexpected repl mode");
        break;
    }
    zfree(request);
}

void masterParsePsyncRequest(syncRequest *request, robj *replid, robj *offset) {
    long long value;
    if (getLongLongFromObject(offset,&value) != C_OK) {
        request->mode = REPL_MODE_UNSET;
        request->i.msg = sdscatprintf(sdsempty(),"offset %s invalid",(sds)offset->ptr);
    } else {
        request->mode = REPL_MODE_PSYNC;
        request->p.replid = sdsdup(replid->ptr);
        request->p.offset = value;
    }
}

void masterParseXsyncRequest(syncRequest *request, robj *uuid, robj *gtidset,
        int optargc, robj **optargv) {
    long long maxgap = 0;
    gtidSet *gtid_slave = NULL, *gtid_lost = NULL;
    sds gtid_repr = gtidset->ptr, msg = NULL;

    if ((gtid_slave = gtidSetDecode(gtid_repr,sdslen(gtid_repr))) == NULL) {
        msg = sdscatprintf(sdsempty(), "invalid gtid.set %s", gtid_repr);
        goto invalid;
    }

    for (int i = 0; i+1 < optargc; i += 2) {
        if (!strcasecmp(optargv[i]->ptr,"maxgap")) {
            if (getLongLongFromObject(optargv[i+1],&maxgap) != C_OK) {
                maxgap = 0;
                serverLog(LL_NOTICE, "Ignored invalid xsync maxgap option: %s",
                        (sds)optargv[i+1]->ptr);
            }
        } else if (!strcasecmp(optargv[i]->ptr,"gtid.lost")) {
            if ((gtid_lost = gtidSetDecode(optargv[i+1]->ptr,
                            sdslen(optargv[i+1]->ptr))) == NULL) {
                serverLog(LL_WARNING, "Invalid xsync gtid.lost option: %s",
                        (sds)optargv[i+1]->ptr);
                goto invalid;
            }
        } else {
            serverLog(LL_NOTICE, "Ignored invalid xsync option %s",
                    (sds)optargv[i]->ptr);
        }
    }

    request->mode = REPL_MODE_XSYNC;
    request->x.gtid_slave = gtid_slave;
    request->x.gtid_lost = gtid_lost;
    if (request->x.gtid_lost == NULL) {
        serverLog(LL_NOTICE, "gtid.lost unspecified, default to empty");
        request->x.gtid_lost = gtidSetNew();
    }
    request->x.maxgap = maxgap;
    request->x.uuid_interested = sdsnew(uuid->ptr);
    return;

invalid:
    if (gtid_slave) gtidSetFree(gtid_slave);
    if (gtid_lost) gtidSetFree(gtid_lost);
    request->mode = REPL_MODE_UNSET;
    request->i.msg = msg;
    return;
}

syncRequest *masterParseSyncRequest(client *c) {
    syncRequest *request = syncRequestNew();
    char *mode = c->argv[0]->ptr;
    sds cmdrepr = sdsempty();

    for (int i = 0; i < c->argc; i++) {
        if (c->argv[i]->encoding == OBJ_ENCODING_INT) {
            cmdrepr = sdscatprintf(cmdrepr, "\"%ld\"", (long)c->argv[i]->ptr);
        } else {
            cmdrepr = sdscatrepr(cmdrepr,(char*)c->argv[i]->ptr,
                        sdslen(c->argv[i]->ptr));
        }
        if (i != c->argc-1) cmdrepr = sdscatlen(cmdrepr," ",1);
    }
    serverLog(LL_NOTICE,
            "[gtid] replica %s asks for synchronization with request: %s",
            replicationGetSlaveName(c), cmdrepr);
    sdsfree(cmdrepr);

    if (!strcasecmp(mode,"psync")) {
        masterParsePsyncRequest(request,c->argv[1],c->argv[2]);
    } else if (!strcasecmp(mode,"xsync")) {
        masterParseXsyncRequest(request,c->argv[1],c->argv[2],c->argc-3,c->argv+3);
    } else {
        request->mode = REPL_MODE_UNSET;
        request->i.msg = sdscatprintf(sdsempty(), "invalid repl mode: %s", mode);
    }
    return request;
}

#define SYNC_ACTION_NOP         0
#define SYNC_ACTION_XCONTINUE   1
#define SYNC_ACTION_CONTINUE    2
#define SYNC_ACTION_FULL        3

typedef struct syncResult {
    int request_mode;
    int action;
    long long offset; /* send offset start */
    long long limit; /* send offset limit */
    sds msg;
    union {
        /* Note that there's no need to save replid/gtid.lost for fullresync
         * or xfullresync, fullresync should use current replid/gtid.lost
         * save in server instead of replid/gtid snapshot when ana reqeust. */
        struct {
            sds replid;
            long long reploff;
            gtidSet *gtid_cont;
            gtidSet *delta_lost;
        } xc; /* xcontinue */
        struct {
            sds replid;
            /* reploff that slave has logically received. will be propagate
             * to slave when xcontinue => continue to align repl reploff
             * of master and slave.
             * Note that to keep compatible with origin replication proptocol,
             * when reploff < 0, it will not be propragte to slave. */
            long long reploff;
            /* gtid.set-lost might exists when xsync => psync. */
            gtidSet *delta_lost;
        } cc; /* continue */
    };
} syncResult;

syncResult *syncResultNew() {
    syncResult *result = zcalloc(sizeof(syncResult));
    return result;
}

void syncResultFree(syncResult *result) {
    if (result == NULL) return;
    switch (result->action) {
    case SYNC_ACTION_NOP:
        break;
    case SYNC_ACTION_XCONTINUE:
        sdsfree(result->xc.replid);
        gtidSetFree(result->xc.gtid_cont);
        gtidSetFree(result->xc.delta_lost);
        break;
    case SYNC_ACTION_CONTINUE:
        sdsfree(result->cc.replid);
        gtidSetFree(result->cc.delta_lost);
        break;
    case SYNC_ACTION_FULL:
        break;
    }
    sdsfree(result->msg);
    zfree(result);
}

void masterAnaPsyncRequest(syncResult *result, syncRequest *request) {
    syncLocateResult slr;
    sds psync_replid = request->p.replid;
    long long psync_offset = request->p.offset;

    if (request->p.replid[0] == '?') {
        result->action = SYNC_ACTION_FULL;
        result->msg = sdsnew("fullresync request");
        return;
    }

    if (!server.repl_backlog ||
        psync_offset < server.repl_backlog_off ||
        psync_offset > (server.repl_backlog_off+server.repl_backlog_histlen)) {
        result->action = SYNC_ACTION_FULL;
        result->msg = sdscatprintf(sdsempty(),
                "psync offset(%lld) not in backlog [%lld,%lld)",
                psync_offset, server.repl_backlog_off,
                server.repl_backlog_off+server.repl_backlog_histlen);
        return;
    }

    result->offset = psync_offset;

    syncLocateResultInit(&slr);
    locateServerReplMode(REPL_MODE_PSYNC,psync_offset,&slr);

    if (slr.locate_type == LOCATE_TYPE_INVALID) {
        result->action = SYNC_ACTION_FULL;
        result->msg = slr.i.msg, slr.i.msg = NULL;
    } else if (slr.locate_type == LOCATE_TYPE_PREV ||
            slr.locate_type == LOCATE_TYPE_SWITCH) {
        serverAssert(server.prev_repl_mode->mode == REPL_MODE_PSYNC);
        const char *replid1 = server.prev_repl_mode->psync.replid;
        const char *replid2 = server.prev_repl_mode->psync.replid2;
        long long offset1 = server.repl_mode->from;
        long long offset2 = server.prev_repl_mode->psync.second_replid_offset;
        if ((!strcasecmp(psync_replid, replid1) && psync_offset <= offset1) ||
            (!strcasecmp(psync_replid, replid2) && psync_offset <= offset2)) {
            if (slr.locate_type == LOCATE_TYPE_PREV) {
                result->action = SYNC_ACTION_CONTINUE;
                result->limit = slr.p.limit;
                result->cc.replid = sdsnew(replid1);
                result->cc.reploff = -1; /* no need to align reploff */
                result->msg = sdsnew("prior psync => xsync");
            } else {
                gtidSet *gtid_master, *gtid_cont, *gtid_xsync;
                sds gtid_master_repr, gtid_continue_repr, gtid_xsync_repr;

                gtid_master = serverGtidSetGet("[psync] [ana]");
                gtid_master_repr = gtidSetDump(gtid_master);

                gtid_xsync = gtidSeqPsync(server.gtid_seq,psync_offset);
                gtid_cont = gtid_master, gtid_master = NULL;
                gtidSetDiff(gtid_cont,gtid_xsync);

                gtid_continue_repr = gtidSetDump(gtid_cont);
                gtid_xsync_repr = gtidSetDump(gtid_xsync);
                serverLog(LL_NOTICE, "[psync] gtid.set-continue(%s) ="
                        " gtid.set-master(%s) - gtid.set-xsync(%s)",
                        gtid_continue_repr,gtid_master_repr,gtid_xsync_repr);

                result->action = SYNC_ACTION_XCONTINUE;
                result->xc.replid = sdsnew(serverReplModeGetCurReplIdOff(
                            psync_offset-1,&result->xc.reploff));
                result->xc.gtid_cont = gtid_cont, gtid_cont = NULL;
                result->xc.delta_lost = gtidSetNew();
                result->msg = sdsnew("psync => xsync");

                sdsfree(gtid_master_repr);
                sdsfree(gtid_continue_repr);
                sdsfree(gtid_xsync_repr);

                gtidSetFree(gtid_xsync);
            }
        } else {
            result->action = SYNC_ACTION_FULL;
            result->msg = sdscatprintf(sdsempty(),
                    "(%s:%lld) can't continue in {(%s:%lld),(%s:%lld)}",
                    psync_replid, psync_offset,
                    replid1, offset1, replid2, offset2);
        }
    } else {
        /* Let origin redis hanle this psync request */
        serverAssert(slr.locate_type == LOCATE_TYPE_CUR);
        result->action = SYNC_ACTION_NOP;
    }

    syncLocateResultDeinit(&slr);
}

void masterAnaXsyncRequest(syncResult *result, syncRequest *request) {
    syncLocateResult slr;
    long long psync_offset, maxgap = request->x.maxgap;
    gtidSet *gtid_slave = request->x.gtid_slave;
    gtidSet *gtid_master = NULL, *gtid_cont = NULL, *gtid_xsync = NULL,
            *gtid_gap = NULL, *gtid_mlost = NULL, *gtid_slost = NULL,
            *gtid_mexec = NULL, *gtid_sexec = NULL,
            *gtid_mexec_gap = NULL, *gtid_sexec_gap = NULL;
    sds gtid_master_repr = NULL, gtid_continue_repr = NULL,
        gtid_xsync_repr = NULL, gtid_slave_repr = NULL,
        gtid_mlost_repr = NULL, gtid_slost_repr = NULL,
        gtid_mexec_repr = NULL, gtid_sexec_repr = NULL,
        gtid_mgap_repr = NULL, gtid_sgap_repr = NULL,
        gtid_lost_repr = NULL, gtid_executed_repr = NULL;

    syncLocateResultInit(&slr);

    if (!strcmp(request->x.uuid_interested,
                GTID_XSYNC_UUID_INTERESTED_FULLRESYNC)) {
        result->action = SYNC_ACTION_FULL;
        result->msg = sdsnew("xfullresync requested");
        goto end;
    }

    gtid_master = serverGtidSetGet("[xsync] [ana]");
    gtid_master_repr = gtidSetDump(gtid_master);
    gtid_slave_repr = gtidSetDump(gtid_slave);

    /* FullResync if gtidSet not related, for example:
     *   empty slave asks for xsync
     *   instance of another shard asks for xsync */
    if (!gtidSetRelated(gtid_master,gtid_slave)) {
        result->action = SYNC_ACTION_FULL;
        result->msg = sdscatprintf(sdsempty(),
                "gtid.set-master(%s) and gtid.set-slave(%s) not related",
                gtid_master_repr, gtid_slave_repr);
        goto end;
    }

    if (server.gtid_seq == NULL) {
        gtid_xsync = gtidSetNew();
        psync_offset = server.master_repl_offset+1;
        serverLog(LL_NOTICE, "[xsync] [ana] continue point defaults"
                " to backlog tail: gtid.seq not exists.");
    } else {
        psync_offset = gtidSeqXsync(server.gtid_seq,gtid_slave,&gtid_xsync);
    }

    gtid_xsync_repr = gtidSetDump(gtid_xsync);
    serverLog(LL_NOTICE, "[xsync] [ana] continue point locate at offset=%lld,"
            " gtid.set-xsync=%s", psync_offset, gtid_xsync_repr);

    if (psync_offset < 0) {
        if (server.repl_mode->mode == REPL_MODE_XSYNC) {
            psync_offset = server.master_repl_offset+1;
            serverLog(LL_NOTICE, "[xsync] [ana] continue point adjust to"
                    " backlog tail: offset=%lld", psync_offset);
        } else {
            psync_offset = server.repl_mode->from;
            serverLog(LL_NOTICE, "[xsync] [ana] continue point adjust to"
                    " psync from: offset=%lld", psync_offset);
        }
    }

    result->offset = psync_offset;
    locateServerReplMode(REPL_MODE_XSYNC,psync_offset,&slr);

    if (slr.locate_type == LOCATE_TYPE_INVALID) {
        result->action = SYNC_ACTION_FULL;
        result->msg = slr.i.msg, slr.i.msg = NULL;
        goto end;
    }

    gtidSetDiff(gtid_master,gtid_xsync);
    gtid_cont = gtid_master, gtid_master = NULL;

    gtid_continue_repr = gtidSetDump(gtid_cont);
    serverLog(LL_NOTICE, "[xsync] [ana] gtid.set-continue(%s) ="
            " gtid.set-master(%s) - gtid.set-xsync(%s)",
            gtid_continue_repr,gtid_master_repr,gtid_xsync_repr);

    gtid_slost = gtidSetDup(gtid_cont);
    gtidSetDiff(gtid_slost,gtid_slave);

    gtid_slost_repr = gtidSetDump(gtid_slost);
    serverLog(LL_NOTICE, "[xsync] [ana] gtid.set-slost(%s) ="
            " gtid.set-continue(%s) - gtid.set-slave(%s)",
            gtid_slost_repr,gtid_continue_repr,gtid_slave_repr);

    gtid_mlost = gtidSetDup(gtid_slave);
    gtidSetDiff(gtid_mlost,gtid_cont);

    gtid_mlost_repr = gtidSetDump(gtid_mlost);
    serverLog(LL_NOTICE, "[xsync] [ana] gtid.set-mlost(%s) ="
            " gtid.set-slave(%s) - gtid.set-continue(%s)",
            gtid_mlost_repr,gtid_slave_repr,gtid_continue_repr);

    gtid_executed_repr = gtidSetDump(server.gtid_executed);

    gtid_mexec = gtidSetDup(server.gtid_executed);
    gtidSetDiff(gtid_mexec, gtid_xsync);

    gtid_mexec_repr = gtidSetDump(gtid_mexec);
    serverLog(LL_NOTICE, "[xsync] [ana] gtid.set-mexec(%s) ="
            " gtid.set-executed(%s) - gtid.set-xsync(%s)",
            gtid_mexec_repr,gtid_executed_repr,gtid_xsync_repr);

    gtid_lost_repr = gtidSetDump(request->x.gtid_lost);

    gtid_sexec = gtidSetDup(gtid_slave);
    gtidSetDiff(gtid_sexec, request->x.gtid_lost);
    gtid_sexec_repr = gtidSetDump(gtid_sexec);
    serverLog(LL_NOTICE, "[xsync] [ana] gtid.set-sexec(%s) ="
            " gtid.set-slave(%s) - gtid.set-lost(%s)",
            gtid_sexec_repr, gtid_slave_repr, gtid_lost_repr);

    gtid_mexec_gap = gtidSetDup(gtid_mexec);
    gtidSetDiff(gtid_mexec_gap,gtid_sexec);
    gtid_mgap_repr = gtidSetDump(gtid_mexec_gap);
    serverLog(LL_NOTICE, "[xsync] [ana] gtid.set-mgap(%s) ="
            " gtid.set-mexec(%s) - gtid.set-sexec(%s)",
            gtid_mgap_repr, gtid_mexec_repr, gtid_sexec_repr);

    gtid_sexec_gap = gtidSetDup(gtid_sexec);
    gtidSetDiff(gtid_sexec_gap,gtid_mexec);
    gtid_sgap_repr = gtidSetDump(gtid_sexec_gap);
    serverLog(LL_NOTICE, "[xsync] [ana] gtid.set-sgap(%s) ="
            " gtid.set-sexec(%s) - gtid.set-mexec(%s)",
            gtid_sgap_repr, gtid_sexec_repr, gtid_mexec_repr);

    gno_t gap = gtidSetCount(gtid_mexec_gap) + gtidSetCount(gtid_sexec_gap);
    if (gap > maxgap) {
        result->action = SYNC_ACTION_FULL;
        result->msg = sdscatprintf(sdsempty(), "gap=%lld > maxgap=%lld",
                gap, maxgap);
        goto end;
    }

    if (slr.locate_type == LOCATE_TYPE_PREV) {
        result->action = SYNC_ACTION_XCONTINUE;
        result->limit = slr.p.limit;
        result->xc.replid = sdsnew(serverReplModeGetPrevReplIdOff(
                    psync_offset-1,&result->xc.reploff));
        result->xc.gtid_cont = gtid_cont, gtid_cont = NULL;
        result->xc.delta_lost = gtid_mlost, gtid_mlost = NULL;
        result->msg = sdscatprintf(sdsempty(),
                "gap=%lld <= maxgap=%lld",gap,maxgap);
    } else if (slr.locate_type == LOCATE_TYPE_SWITCH) {
        result->action = SYNC_ACTION_CONTINUE;
        result->cc.replid = sdsnew(server.replid);
        result->cc.reploff = psync_offset-1;
        result->cc.delta_lost = gtid_mlost, gtid_mlost = NULL;
        result->msg = sdsnew("xsync => psync");
    } else {
        serverAssert(slr.locate_type == LOCATE_TYPE_CUR);
        result->action = SYNC_ACTION_XCONTINUE;
        result->xc.replid = sdsnew(serverReplModeGetCurReplIdOff(
                    psync_offset-1,&result->xc.reploff));
        result->xc.gtid_cont = gtid_cont, gtid_cont = NULL;
        result->xc.delta_lost = gtid_mlost, gtid_mlost = NULL;
        result->msg = sdscatprintf(sdsempty(),
                "gap=%lld <= maxgap=%lld",gap,maxgap);
    }

end:
    syncLocateResultDeinit(&slr);

    sdsfree(gtid_master_repr), sdsfree(gtid_continue_repr);
    sdsfree(gtid_xsync_repr), sdsfree(gtid_slave_repr);
    sdsfree(gtid_mlost_repr), sdsfree(gtid_slost_repr);
    sdsfree(gtid_mexec_repr), sdsfree(gtid_sexec_repr);
    sdsfree(gtid_mgap_repr), sdsfree(gtid_sgap_repr);
    sdsfree(gtid_lost_repr), sdsfree(gtid_executed_repr);

    gtidSetFree(gtid_master), gtidSetFree(gtid_cont), gtidSetFree(gtid_xsync);
    gtidSetFree(gtid_gap), gtidSetFree(gtid_mlost), gtidSetFree(gtid_slost);
    gtidSetFree(gtid_mexec), gtidSetFree(gtid_sexec);
    gtidSetFree(gtid_mexec_gap), gtidSetFree(gtid_sexec_gap);
}

syncResult *masterAnaSyncRequest(syncRequest *request) {
    syncResult *result = syncResultNew();
    switch (request->mode) {
    case REPL_MODE_PSYNC:
        result->request_mode = REPL_MODE_PSYNC;
        masterAnaPsyncRequest(result,request);
        break;
    case REPL_MODE_XSYNC:
        result->request_mode = REPL_MODE_XSYNC;
        masterAnaXsyncRequest(result,request);
        break;
    case REPL_MODE_UNSET:
        result->request_mode = REPL_MODE_UNSET;
        result->action = SYNC_ACTION_FULL;
        result->msg = request->i.msg, request->i.msg = NULL;
        break;
    }
    return result;
}

typedef void (*consume_cb)(char *p, long long thislen, void *pd);

/* Check adReplyReplicationBacklog for more details */
long long consumeReplicationBacklogLimited(long long offset, long long limit,
         consume_cb cb, void *pd) {
    long long added = 0, j, skip, len;
    serverAssert(limit >= 0 && offset >= server.repl_backlog_off);
    if (server.repl_backlog_histlen == 0) return 0;
    skip = offset - server.repl_backlog_off;
    j = (server.repl_backlog_idx +
        (server.repl_backlog_size-server.repl_backlog_histlen)) %
        server.repl_backlog_size;
    j = (j + skip) % server.repl_backlog_size;
    len = server.repl_backlog_histlen - skip;
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

static void consumeReplicationBacklogLimitedAddReplyCb(char *p,
        long long thislen, void *pd) {
    addReplySds((client*)pd, sdsnewlen(p,thislen));
}

long long addReplyReplicationBacklogLimited(client *c, long long offset,
        long long limit) {
    return consumeReplicationBacklogLimited(offset,limit,
            consumeReplicationBacklogLimitedAddReplyCb,c);
}

typedef struct copyCbPrivData {
    char *buf;
    long long added;
} copyCbPrivData;

static void consumeReplicationBacklogLimitedCopyCb(char *p,
        long long thislen, void *_pd) {
    copyCbPrivData *pd = _pd;
    memcpy(pd->buf + pd->added, p, thislen);
    pd->added += thislen;
}

/* Check adReplyReplicationBacklog for more details */
long long copyReplicationBacklogLimited(char *buf, long long offset,
        long long limit) {
    copyCbPrivData pd = {buf, 0};
    return consumeReplicationBacklogLimited(offset,limit,
            consumeReplicationBacklogLimitedCopyCb,&pd);
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

int ctrip_masterTryPartialResynchronization(client *c) {
    syncRequest *request = masterParseSyncRequest(c);
    syncResult *result = masterAnaSyncRequest(request);
    int ret = masterReplySyncRequest(c,result);
    syncRequestFree(request);
    syncResultFree(result);
    return ret;
}

const char *xsyncUuidInterestedGet(void);

int ctrip_slaveTryPartialResynchronizationWrite(connection *conn) {
    gtidSet *gtid_slave = NULL;
    char maxgap[32];
    int result = PSYNC_WAIT_REPLY;
    sds gtid_slave_repr, gtid_lost_repr;

    serverLog(LL_NOTICE, "[gtid] Trying parital sync in (%s) mode.",
            replModeName(server.repl_mode->mode));

    if (server.repl_mode->mode != REPL_MODE_XSYNC) return -1;

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
    if (reply != NULL) {
        serverLog(LL_WARNING,"[xsync] Unable to send XSYNC: %s", reply);
        sdsfree(reply);
        connSetReadHandler(conn, NULL);
        result = PSYNC_WRITE_ERROR;
    }

    gtidSetFree(gtid_slave);
    sdsfree(gtid_slave_repr);
    sdsfree(gtid_lost_repr);

    return result;
}

#define SYNC_REPLY_INVALID      0
#define SYNC_REPLY_FULLRESYNC   1
#define SYNC_REPLY_CONTINUE     2
#define SYNC_REPLY_XFULLRESYNC  3
#define SYNC_REPLY_XCONTINUE    4
#define SYNC_REPLY_TRANSERR     5
#define SYNC_REPLY_TRANSERR2    6

typedef struct parsedSyncReply {
    int type;
    union {
        struct {
            sds replid;
            long long reploff;
        } fullresync;
        struct {
            sds replid;
            int reploff_is_set;
            long long reploff;
        } pcontinue;
        struct {
            gtidSet *gtid_lost;
            sds master_uuid;
            sds replid;
            long long reploff;
        } xfullresync;
        struct {
            gtidSet *gtid_cont;
            gtidSet *gtid_lost; /* default to empty if no specified */
            sds master_uuid;
            sds replid;
            long long reploff;
        } xcontinue;
        struct {
            sds errmsg;
        } invalid;
    };
} parsedSyncReply;

parsedSyncReply *parsedSyncReplyNew() {
    parsedSyncReply *parsed = zcalloc(sizeof(parsedSyncReply));
    return parsed;
}

void parsedSyncReplyFree(parsedSyncReply *parsed) {
    if (parsed == NULL) return;
    switch (parsed->type) {
    case SYNC_REPLY_FULLRESYNC:
        sdsfree(parsed->fullresync.replid);
        parsed->fullresync.replid = NULL;
        break;
    case SYNC_REPLY_CONTINUE:
        sdsfree(parsed->pcontinue.replid);
        parsed->pcontinue.replid = NULL;
        break;
    case SYNC_REPLY_XFULLRESYNC:
        gtidSetFree(parsed->xfullresync.gtid_lost);
        parsed->xfullresync.gtid_lost = NULL;
        sdsfree(parsed->xfullresync.master_uuid);
        parsed->xfullresync.master_uuid = NULL;
        sdsfree(parsed->xfullresync.replid);
        parsed->xfullresync.replid = NULL;
        break;
    case SYNC_REPLY_XCONTINUE:
        gtidSetFree(parsed->xcontinue.gtid_cont);
        parsed->xcontinue.gtid_cont = NULL;
        gtidSetFree(parsed->xcontinue.gtid_lost);
        parsed->xcontinue.gtid_lost = NULL;
        sdsfree(parsed->xcontinue.master_uuid);
        parsed->xcontinue.master_uuid = NULL;
        sdsfree(parsed->xcontinue.replid);
        parsed->xcontinue.replid = NULL;
        break;
    case SYNC_REPLY_INVALID:
        sdsfree(parsed->invalid.errmsg);
        parsed->invalid.errmsg = NULL;
        break;
    default:
        break;
    }
    zfree(parsed);
}

/* +FULLRESYNC <replid> <reploff> */
static void parseSyncReplyFullresync(sds reply, parsedSyncReply *parsed) {
    char *replid = NULL, *offset = NULL;
    sds errmsg = NULL;
    long long parsed_offset = -1;

    /* FULL RESYNC, parse the reply in order to extract the replid
     * and the replication offset. */
    replid = strchr(reply,' ');
    if (replid) {
        replid++;
        offset = strchr(replid,' ');
        if (offset) offset++;
    }
    if (!replid || !offset || (offset-replid-1) != CONFIG_RUN_ID_SIZE) {
        errmsg = sdscatprintf(sdsempty(),"replid invalid(%s)",reply);
        goto invalid;
    } else {
        parsed_offset = strtoll(offset,NULL,10);
        if (parsed_offset < 0) {
            errmsg = sdscatprintf(sdsempty(),"offset invalid(%s)",reply);
            goto invalid;
        }
    }

    parsed->type = SYNC_REPLY_FULLRESYNC;
    parsed->fullresync.replid =sdsnewlen(replid,offset-replid-1);
    parsed->fullresync.reploff = parsed_offset;
    return;

invalid:
    parsed->type = SYNC_REPLY_INVALID;
    parsed->invalid.errmsg = errmsg;
}

/* +CONTINUE [<replid>] [<reloff>] */
static void parseSyncReplyContinue(sds reply, parsedSyncReply *parsed) {
    sds errmsg = NULL;
    char *start, *end;
    long long reploff = 0;
    int reploff_is_set = 0;
    sds replid = NULL;

    start = reply+9;
    while(start[0] == ' ' || start[0] == '\t') start++;
    end = start;
    while(end[0] != ' ' && end[0] != '\t' &&
            end[0] != '\r' && end[0] != '\n' && end[0] != '\0') end++;

    if (end == start) goto end; /* +continue */
    if (end-start != CONFIG_RUN_ID_SIZE) goto invalid;
    replid = sdsnewlen(start,CONFIG_RUN_ID_SIZE);

    start = end;
    while(start[0] == ' ' || start[0] == '\t') start++;
    end = start;
    while(end[0] != '\r' && end[0] != '\n' && end[0] != '\0') end++;

    if (end == start) goto end; /* +continue replid */
    if (string2ll(start,end-start,&reploff) == 0) {
        errmsg = sdscatprintf(sdsempty(),"reploff invalid(%s)",reply);
        sdsfree(replid);
        goto invalid;
    }
    reploff_is_set = 1; /* +continue replid offset */

end:
    parsed->type = SYNC_REPLY_CONTINUE;
    parsed->pcontinue.replid = replid;
    parsed->pcontinue.reploff_is_set = reploff_is_set;
    parsed->pcontinue.reploff = reploff;
    return;

invalid:
    parsed->type = SYNC_REPLY_INVALID;
    parsed->invalid.errmsg = errmsg;
}

/* +XFULLRESYNC GTID.LOST <gtid.lost> MASTER.UUID <master-uuid>
 * REPLID <replid> REPLOFF <reploff> */
static void parseSyncReplyXfullresync(sds reply, parsedSyncReply *parsed) {
    sds *tokens, errmsg = NULL, replid = NULL, master_uuid = NULL;
    size_t token_off = 12;
    int i, ntoken;
    gtidSet *gtid_lost = NULL;
    long long reploff = -1;

    while (token_off < sdslen(reply) && isspace(reply[token_off]))
        token_off++;
    tokens = sdssplitargs(reply+token_off,&ntoken);

    for (i = 0; i+1 < ntoken; i += 2) {
        if (!strncasecmp(tokens[i], "gtid.lost", sdslen(tokens[i]))) {
            gtid_lost = gtidSetDecode(tokens[i+1],sdslen(tokens[i+1]));
            if (gtid_lost == NULL) {
                errmsg = sdscatprintf(sdsempty(),"invalid gtid.set-lost(%s)",
                        tokens[i+1]);
                goto invalid;
            }
        } else if (!strncasecmp(tokens[i], "master.uuid", sdslen(tokens[i]))) {
            master_uuid = sdsdup(tokens[i+1]);
        } else if (!strncasecmp(tokens[i], "replid", sdslen(tokens[i]))) {
            if (sdslen(tokens[i+1]) != CONFIG_RUN_ID_SIZE) {
                errmsg = sdscatprintf(sdsempty(),"invalid replid(%s)",
                        tokens[i+1]);
                goto invalid;
            }
            replid = sdsdup(tokens[i+1]);
        } else if (!strncasecmp(tokens[i], "reploff", sdslen(tokens[i]))) {
            reploff = strtoll(tokens[i+1],NULL,10);
            if (reploff < 0) {
                errmsg = sdscatprintf(sdsempty(),"invalid reploff(%s)",
                        tokens[i+1]);
                goto invalid;
            }
        } else {
            serverLog(LL_NOTICE,
                    "Ignore unrecognized xfullresync option: %s", tokens[i]);
        }
    }

    if (!master_uuid) {
        errmsg = sdsnew("master.uuid unspecified");
        goto invalid;
    }

    if (!gtid_lost) {
        errmsg = sdsnew("gtid.lost unspecified");
        goto invalid;
    }

    if (!replid) {
        errmsg = sdsnew("replid invalid or unspecified");
        goto invalid;
    }

    if (reploff < 0) {
        errmsg = sdsnew("reploff invalid or unspecified");
        goto invalid;
    }

    parsed->type = SYNC_REPLY_XFULLRESYNC;
    parsed->xfullresync.master_uuid = master_uuid;
    parsed->xfullresync.gtid_lost = gtid_lost;
    parsed->xfullresync.replid = replid;
    parsed->xfullresync.reploff = reploff;

    sdsfreesplitres(tokens,ntoken);
    return;

invalid:
    parsed->type = SYNC_REPLY_INVALID;
    parsed->invalid.errmsg = errmsg;

    sdsfreesplitres(tokens,ntoken);
    if (replid) sdsfree(replid);
    if (master_uuid) sdsfree(master_uuid);
    if (gtid_lost) gtidSetFree(gtid_lost);
}

/* +XCONTINUE GTID.SET <gtid.set-continue> [GTID.LOST <gtid.set-lost>]
 * MASTER.UUID <master-uuid> REPLID <replid> REPLOFF <reploff> */
static void parseSyncReplyXcontinue(sds reply, parsedSyncReply *parsed) {
    sds *tokens, errmsg = NULL, replid = NULL, master_uuid = NULL;
    size_t token_off = 10;
    int i, ntoken;
    gtidSet *gtid_cont = NULL, *gtid_lost = NULL;
    long long reploff = -1;

    while (token_off < sdslen(reply) && isspace(reply[token_off]))
        token_off++;
    tokens = sdssplitlen(reply+token_off,
            sdslen(reply)-token_off, " ",1,&ntoken);

    for (i = 0; i+1 < ntoken; i += 2) {
        if (!strncasecmp(tokens[i], "gtid.set", sdslen(tokens[i]))) {
            gtid_cont = gtidSetDecode(tokens[i+1],sdslen(tokens[i+1]));
            if (gtid_cont == NULL) {
                errmsg = sdscatprintf(sdsempty(),"invalid gtid.set-cont(%s)",
                        tokens[i+1]);
                goto invalid;
            }
        } else if (!strncasecmp(tokens[i], "gtid.lost", sdslen(tokens[i]))) {
            gtid_lost = gtidSetDecode(tokens[i+1],sdslen(tokens[i+1]));
            if (gtid_lost == NULL) {
                errmsg = sdscatprintf(sdsempty(),"invalid gtid.set-lost(%s)",
                        tokens[i+1]);
                goto invalid;
            }
        } else if (!strncasecmp(tokens[i], "master.uuid", sdslen(tokens[i]))) {
            master_uuid = sdsdup(tokens[i+1]);
        } else if (!strncasecmp(tokens[i], "replid", sdslen(tokens[i]))) {
            if (sdslen(tokens[i+1]) != CONFIG_RUN_ID_SIZE) {
                errmsg = sdscatprintf(sdsempty(),"invalid replid(%s)",
                        tokens[i+1]);
                goto invalid;
            }
            replid = sdsdup(tokens[i+1]);
        } else if (!strncasecmp(tokens[i], "reploff", sdslen(tokens[i]))) {
            reploff = strtoll(tokens[i+1],NULL,10);
            if (reploff < 0) {
                errmsg = sdscatprintf(sdsempty(),"invalid reploff(%s)",
                        tokens[i+1]);
                goto invalid;
            }
        } else {
            serverLog(LL_NOTICE,
                    "Ignore unrecognized xcontinue option: %s", tokens[i]);
        }
    }

    if (!master_uuid) {
        errmsg = sdsnew("master.uuid unspecified");
        goto invalid;
    }

    if (!gtid_cont) {
        errmsg = sdsnew("gtid.set unspecified");
        goto invalid;
    }

    if (!replid) {
        errmsg = sdsnew("replid invalid or unspecified");
        goto invalid;
    }

    if (reploff < 0) {
        errmsg = sdsnew("reploff invalid or unspecified");
        goto invalid;
    }

    parsed->type = SYNC_REPLY_XCONTINUE;
    parsed->xcontinue.master_uuid = master_uuid;
    parsed->xcontinue.gtid_cont = gtid_cont;
    if (gtid_lost == NULL) {
        parsed->xcontinue.gtid_lost = gtidSetNew();
    } else {
        parsed->xcontinue.gtid_lost = gtid_lost;
        gtid_lost = NULL;
    }
    parsed->xcontinue.replid = replid;
    parsed->xcontinue.reploff = reploff;

    sdsfreesplitres(tokens,ntoken);
    return;

invalid:
    parsed->type = SYNC_REPLY_INVALID;
    parsed->invalid.errmsg = errmsg;

    sdsfreesplitres(tokens,ntoken);
    if (replid) sdsfree(replid);
    if (master_uuid) sdsfree(master_uuid);
    if (gtid_cont) gtidSetFree(gtid_cont);
    if (gtid_lost) gtidSetFree(gtid_lost);
}

/* Move parsed xfullresync reply to server.gtid_initial */
static void parsedSyncReplySetupGtidInital(parsedSyncReply *parsed) {
    serverAssert(parsed->type == SYNC_REPLY_XFULLRESYNC);
    gtidInitialInfoSetup(server.gtid_initial,
            parsed->xfullresync.gtid_lost,parsed->xfullresync.master_uuid,
            parsed->xfullresync.replid,parsed->xfullresync.reploff);
    parsed->xfullresync.gtid_lost = NULL;
    parsed->xfullresync.master_uuid = NULL;
    parsed->xfullresync.replid = NULL;
}

static parsedSyncReply *parseSyncReply(sds reply) {
    parsedSyncReply *parsed = parsedSyncReplyNew();

    if (!strncmp(reply,"+XFULLRESYNC",12)) {
        parseSyncReplyXfullresync(reply,parsed);
    } else if (!strncmp(reply,"+XCONTINUE",10)) {
        parseSyncReplyXcontinue(reply,parsed);
    } else if (!strncmp(reply,"+FULLRESYNC",11)) {
        parseSyncReplyFullresync(reply,parsed);
    } else if (!strncmp(reply,"+CONTINUE",9)) {
        parseSyncReplyContinue(reply,parsed);
    } else if (!strncmp(reply,"-NOMASTERLINK",13) ||
        !strncmp(reply,"-LOADING",8)) {
        parsed->type = SYNC_REPLY_TRANSERR;
    } else if (!strncmp(reply,"-Reading from master:",21)) {
        parsed->type = SYNC_REPLY_TRANSERR2;
    } else {
        parsed->type = SYNC_REPLY_INVALID;
        parsed->invalid.errmsg = sdsnew("invalid sync reply type");
    }

    return parsed;
}

/* ============================================================================
 * Gaplog - 通过mock client复用processMultibulkBuffer解析backlog命令
 * ============================================================================ */

/* processMultibulkBuffer定义在networking.c，非static，linker可见 */
extern int processMultibulkBuffer(client *c);

/* 最大gaplog缓冲上限（1MB，足够容纳任何正常命令） */
#define GAPLOG_MAX_BUF_SIZE (1024 * 1024)

/* 清理mock client的argv和querybuf */
static void mockClientCleanup(client *c) {
    if (c->argv) {
        for (int i = 0; i < c->argc; i++)
            if (c->argv[i]) decrRefCount(c->argv[i]);
        zfree(c->argv);
    }
    sdsfree(c->querybuf);
}

/* 解析后的单条命令（从mock client转移所有权） */
typedef struct {
    robj **argv;
    int argc;
    sds querybuf;  /* 用于robj->ptr引用的底层数据 */
    size_t qb_pos; /* 命令在querybuf中的位置，用于计算下一个命令的偏移量 */
} gtidParsedCmd;

/* 解析后的多条命令列表（支持MULTI/EXEC事务） */
typedef struct {
    gtidParsedCmd *cmds;
    int num_cmds;
    int capacity;
} gtidParsedCmdList;

/* 从已解析的mock client转移命令所有权到gtidParsedCmdList
 * 转移后客户端的argv/querybuf被置空，避免被mockClientCleanup double-free */
static void gtidParsedCmdListAdd(gtidParsedCmdList *list, client *c) {
    if (list->num_cmds >= list->capacity) {
        list->capacity = list->capacity ? list->capacity * 2 : 8;
        list->cmds = zrealloc(list->cmds,
                              sizeof(gtidParsedCmd) * list->capacity);
    }
    gtidParsedCmd *cmd = &list->cmds[list->num_cmds++];
    cmd->argv = c->argv;
    cmd->argc = c->argc;
    cmd->querybuf = c->querybuf;
    cmd->qb_pos = c->qb_pos;  /* 保存命令位置，用于计算下一个命令偏移量 */
    /* 转移所有权，防止后续mockClientCleanup double-free */
    c->argv = NULL;
    c->argc = 0;
    c->querybuf = NULL;
}

/* 释放gtidParsedCmdList中的所有命令（类似mockClientCleanup的语义） */
static void gtidParsedCmdListCleanup(gtidParsedCmdList *list) {
    for (int i = 0; i < list->num_cmds; i++) {
        if (list->cmds[i].argv) {
            for (int j = 0; j < list->cmds[i].argc; j++)
                if (list->cmds[i].argv[j]) decrRefCount(list->cmds[i].argv[j]);
            zfree(list->cmds[i].argv);
        }
        sdsfree(list->cmds[i].querybuf);
    }
    zfree(list->cmds);
}

/* 通过gtidSeq查找指定(uuid, gno)对应的backlog偏移量
 * 遍历segment链表（从尾部开始），匹配uuid和gno范围
 * 返回绝对偏移量，-1表示未找到 */
static long long gtidSeqLookup(gtidSeq *seq, const char *uuid,
                                size_t uuid_len, gno_t gno) {
    if (seq == NULL) return -1;

    serverLog(LL_WARNING, "[gaplog] gtidSeqLookup: looking for uuid=%.*s, gno=%lld", (int)uuid_len, uuid, (long long)gno);

    gtidSegment *seg = seq->lastseg;
    int seg_count = 0;
    while (seg) {
        seg_count++;
        serverLog(LL_WARNING, "[gaplog]   seg[%d]: uuid=%.*s, base_gno=%lld, tgno=%d, ngno=%d, base_offset=%lld",
                  seg_count, (int)seg->uuid_len, seg->uuid, (long long)seg->base_gno, seg->tgno, seg->ngno, seg->base_offset);

        if (seg->uuid_len == uuid_len &&
            memcmp(seg->uuid, uuid, uuid_len) == 0 &&
            gno >= seg->base_gno + (gno_t)seg->tgno &&
            gno < seg->base_gno + (gno_t)seg->ngno) {
            size_t idx = (size_t)(gno - seg->base_gno);
            long long result = seg->base_offset + seg->deltas[idx];
            serverLog(LL_WARNING, "[gaplog]   FOUND: idx=%zu, delta=%lld, result=%lld", idx, seg->deltas[idx], result);
            return result;
        }
        seg = seg->prev;
    }
    serverLog(LL_WARNING, "[gaplog]   NOT FOUND");
    return -1;
}

/* 从backlog环形缓冲区中读取指定偏移量的数据到线性缓冲区
 * offset: 绝对偏移量（gtidSeq中存储的值，指向RESP命令起始位置）
 * buf: 输出缓冲区
 * size: 要读取的最大字节数
 * 返回实际读取的字节数，-1表示数据已不在backlog中 */
static ssize_t backlogReadAt(long long offset, char *buf, size_t size) {
    if (server.repl_backlog == NULL || server.repl_backlog_histlen == 0)
        return -1;

    long long skip = offset - server.repl_backlog_off;
    if (skip < 0 || skip >= server.repl_backlog_histlen) return -1;

    long long available = server.repl_backlog_histlen - skip;
    if (available <= 0) return -1;
    if ((long long)size > available) size = (size_t)available;

    /* 计算offset在环形缓冲区中的位置（参考addReplyReplicationBacklog） */
    long long j = (server.repl_backlog_idx +
                   (server.repl_backlog_size - server.repl_backlog_histlen)) %
                   server.repl_backlog_size;
    j = (j + skip) % server.repl_backlog_size;

    /* 将环形缓冲区的数据拷贝到线性缓冲区，处理回绕 */
    size_t total = 0;
    while (total < size) {
        size_t thislen = server.repl_backlog_size - j;
        if (thislen > size - total) thislen = size - total;
        memcpy(buf + total, server.repl_backlog + j, thislen);
        total += thislen;
        j = 0; /* 回绕到缓冲区开头 */
    }
    return (ssize_t)total;
}

/* 从backlog offset读取一个GTID包装的命令，通过mock client+processMultibulkBuffer解析
 *
 * 尝试从8KB开始读取，数据不足时缓冲区翻倍重试，最大GAPLOG_MAX_BUF_SIZE
 * 返回0成功，-1失败（backlog覆盖/协议错误）
 * 成功时*mock已填充（调用者负责mockClientCleanup）
 * 内部的原始缓冲区在解析成功后即刻释放（数据已拷贝到mock->querybuf） */
/* 从 backlog 解析 GTID 命令
 * 如果遇到 SELECT 命令，跳过它并继续解析下一个命令
 *
 * 输入：
 *   offset   - backlog 中的绝对偏移量
 *   mock     - 输出：解析结果填入此 mock client（调用者负责 mockClientCleanup）
 *   cmd_len  - 输出：本次解析消耗的字节数（包含跳过的 SELECT），NULL 则不输出
 * 返回 0 成功，-1 失败 */
static int parseGtidCmdFromBacklog(long long offset, client *mock, size_t *cmd_len) {
    size_t buf_size = 8192;
    char *buf = NULL;

    while (buf_size <= GAPLOG_MAX_BUF_SIZE) {
        buf = zrealloc(buf, buf_size);
        ssize_t nread = backlogReadAt(offset, buf, buf_size);
        if (nread <= 0) {
            serverLog(LL_WARNING, "[gaplog] backlogReadAt failed at offset %lld, nread=%zd", offset, nread);
            zfree(buf);
            return -1;
        }

        memset(mock, 0, sizeof(*mock));
        mock->querybuf = sdsnewlen(buf, nread);
        mock->authenticated = 1;

        serverLog(LL_WARNING, "[gaplog] backlog data at offset %lld (first 100 bytes): '%.*s'",
                  offset, (int)(nread > 100 ? 100 : nread), buf);

        /* 循环解析，跳过 SELECT 命令 */
        size_t consumed = 0;  /* 已消耗的字节数（含跳过的 SELECT） */
        while (mock->qb_pos < sdslen(mock->querybuf)) {
            /* 先释放之前解析的 argv（如果有） */
            if (mock->argv) {
                for (int j = 0; j < mock->argc; j++) decrRefCount(mock->argv[j]);
                zfree(mock->argv);
                mock->argv = NULL;
            }
            /* 重置 argc 和 multibulklen 以解析下一个命令 */
            mock->argc = 0;
            mock->multibulklen = 0;
            mock->bulklen = -1;

            /* 记录解析前 querybuf 总长和 qb_pos，用于计算本次命令消耗字节数
             * 注意：processMultibulkBuffer 内部可能触发 querybuf trim（qb_pos 归零），
             * 需要同时记录 before_len 和 before_qb_pos 才能正确计算步进量 */
            size_t before_len = sdslen(mock->querybuf);
            size_t before_qb_pos = mock->qb_pos;

            if (processMultibulkBuffer(mock) != C_OK) {
                /* 协议错误（非数据不足），放弃 */
                if (mock->flags & CLIENT_PROTOCOL_ERROR) {
                    serverLog(LL_WARNING, "[gaplog] protocol error at offset %lld, qb_pos=%zu, flags=%d",
                              offset, mock->qb_pos, mock->flags);
                    zfree(buf);
                    return -1;
                }
                /* 数据不足，需要更大的 buffer */
                break;
            }

            /* 本次命令消耗字节数：
             * 无 trim：qb_pos_after - qb_pos_before
             * 有 trim：(before_len - qb_pos_before) - (sdslen_after - qb_pos_after)
             *          即"解析前剩余字节" - "解析后剩余字节" */
            size_t remaining_before = before_len - before_qb_pos;
            size_t remaining_after = sdslen(mock->querybuf) - mock->qb_pos;
            size_t this_cmd_len = remaining_before - remaining_after;

            /* 检查是否是 SELECT 命令 */
            if (mock->argc >= 1 && mock->argv[0] != NULL) {
                sds cmd = (sds)mock->argv[0]->ptr;
                if (!strcasecmp(cmd, "select")) {
                    serverLog(LL_WARNING, "[gaplog] skipping SELECT command at offset %lld, this_cmd_len=%zu",
                              offset, this_cmd_len);
                    consumed += this_cmd_len;
                    /* 释放 SELECT 命令的 argv，继续解析下一个命令 */
                    for (int j = 0; j < mock->argc; j++) decrRefCount(mock->argv[j]);
                    zfree(mock->argv);
                    mock->argv = NULL;
                    mock->argc = 0;
                    continue;
                }
            }

            /* 不是 SELECT，找到了目标命令 */
            consumed += this_cmd_len;
            serverLog(LL_WARNING, "[gaplog] found GTID command at offset %lld, argc=%d, consumed=%zu",
                      offset, mock->argc, consumed);
            if (cmd_len) *cmd_len = consumed;
            zfree(buf);
            return 0;
        }

        // mockClientCleanup(mock);
        buf_size *= 2;
    }

    zfree(buf);
    return -1;
}

/* 从 backlog 解析命令（不跳过 SELECT）
 * 用于 parseMultiCommand 解析 MULTI 内部命令
 *
 * 输入：
 *   offset   - backlog 中的绝对偏移量
 *   mock     - 输出：解析结果填入此 mock client（调用者负责 mockClientCleanup）
 *   cmd_len  - 输出：本次解析消耗的字节数，NULL 则不输出
 * 返回 0 成功，-1 失败 */
static int parseCmdFromBacklogNoSkip(long long offset, client *mock, size_t *cmd_len) {
    size_t buf_size = 8192;
    char *buf = NULL;
    size_t total_read = 0;  /* 已读取的总字节数 */

    /* 初始化 mock client */
    memset(mock, 0, sizeof(*mock));
    mock->authenticated = 1;

    while (buf_size <= GAPLOG_MAX_BUF_SIZE) {
        /* 读取更多数据 */
        buf = zrealloc(buf, buf_size);
        ssize_t nread = backlogReadAt(offset + total_read, buf, buf_size);
        if (nread <= 0) {
            serverLog(LL_WARNING, "[gaplog] backlogReadAt failed at offset %lld, nread=%zd",
                      offset + total_read, nread);
            zfree(buf);
            return -1;
        }

        /* 追加到 querybuf */
        if (mock->querybuf == NULL) {
            mock->querybuf = sdsnewlen(buf, nread);
        } else {
            mock->querybuf = sdscatlen(mock->querybuf, buf, nread);
        }
        total_read += nread;

        serverLog(LL_WARNING, "[gaplog] MULTI inner cmd at offset %lld, total_read=%zu, qb_pos=%zu (first 100 bytes): '%.*s'",
                  offset, total_read, mock->qb_pos,
                  (int)(sdslen(mock->querybuf) > 100 ? 100 : sdslen(mock->querybuf)), mock->querybuf);

        /* 尝试解析命令 */
        while (mock->qb_pos < sdslen(mock->querybuf)) {
            /* 如果还没有开始解析新命令，重置状态 */
            if (mock->multibulklen == 0 && mock->argc == 0) {
                if (mock->argv) {
                    for (int j = 0; j < mock->argc; j++) decrRefCount(mock->argv[j]);
                    zfree(mock->argv);
                    mock->argv = NULL;
                }
                mock->argc = 0;
                mock->bulklen = -1;
            }

            /* 记录解析前 qb_pos，用于计算本次命令消耗字节数 */
            size_t before_len = sdslen(mock->querybuf);
            size_t before_qb_pos = mock->qb_pos;
            (void)before_len;

            if (processMultibulkBuffer(mock) != C_OK) {
                /* 协议错误（非数据不足），放弃 */
                if (mock->flags & CLIENT_PROTOCOL_ERROR) {
                    serverLog(LL_WARNING, "[gaplog] protocol error at offset %lld, qb_pos=%zu, flags=%lu",
                              offset, mock->qb_pos, (unsigned long)mock->flags);
                    zfree(buf);
                    return -1;
                }
                /* 数据不足，需要读取更多数据 */
                break;
            }

            /* consumed = 本轮总读入字节 - querybuf 中未解析剩余字节
             * 等价于从 offset 到命令结束的字节数，兼容多轮读取和 trim 场景 */
            size_t consumed = total_read - (sdslen(mock->querybuf) - mock->qb_pos);
            (void)before_qb_pos;
            serverLog(LL_WARNING, "[gaplog] parsed MULTI inner cmd at offset %lld, argc=%d, consumed=%zu",
                      offset, mock->argc, consumed);
            if (cmd_len) *cmd_len = consumed;
            zfree(buf);
            return 0;
        }

        /* 数据不足，翻倍 buffer 大小继续读取 */
        buf_size *= 2;
    }

    mockClientCleanup(mock);
    zfree(buf);
    return -1;
}

/* 从命令参数中提取 key/subkey 信息，直接添加到 gtidGapLogKeysInfos
 *
 * args[0] = 命令名, args[1] = key, args[2..] = 其他参数
 * kis: 目标 gtidGapLogKeysInfos
 * dbid: 数据库编号
 *
 * 支持的命令格式：
 *   - 普通命令（SET, LPUSH等）: 单个 key
 *   - Hash命令（HSET key f1 v1 f2 v2）: key + fields 作为 subkeys
 *   - Set命令（SADD key member1 member2）: key + members 作为 subkeys
 *   - Sorted Set命令（ZADD key score member）: key + members 作为 subkeys
 *   - 多key命令（DEL key1 key2）: 每个 key 作为独立条目
 */
static void addKeyInfoToKeysInfos(gtidGapLogKeysInfos *kis, int dbid, robj **args, int argc) {
    if (argc < 2 || kis == NULL) return;

    sds cmd = (sds)args[0]->ptr;

    /* === 多key命令: DEL/EXISTS/UNLINK key1 key2 ... ===
     * 每个 key 作为独立条目添加 */
    if (!strcasecmp(cmd, "del") || !strcasecmp(cmd, "exists") ||
        !strcasecmp(cmd, "unlink") || !strcasecmp(cmd, "mget")) {
        for (int i = 1; i < argc; i++) {
            robj *key_obj = createStringObject((sds)args[i]->ptr, sdslen((sds)args[i]->ptr));
            gtidGapLogKeyInfo *ki = gtidGapLogKeyInfoCreate(dbid, key_obj, NULL, 0);
            decrRefCount(key_obj);
            kis->size++;
            kis->keys = zrealloc(kis->keys, sizeof(gtidGapLogKeyInfo*) * kis->size);
            kis->keys[kis->size - 1] = ki;
        }
        return;
    }

    /* === MSET: key1 v1 key2 v2 ... (keys在奇数位置) ===
     * 每个 key 作为独立条目添加 */
    if (!strcasecmp(cmd, "mset")) {
        for (int i = 1; i < argc; i += 2) {
            robj *key_obj = createStringObject((sds)args[i]->ptr, sdslen((sds)args[i]->ptr));
            gtidGapLogKeyInfo *ki = gtidGapLogKeyInfoCreate(dbid, key_obj, NULL, 0);
            decrRefCount(key_obj);
            kis->size++;
            kis->keys = zrealloc(kis->keys, sizeof(gtidGapLogKeyInfo*) * kis->size);
            kis->keys[kis->size - 1] = ki;
        }
        return;
    }

    /* === HSET/HMSET: key f1 v1 f2 v2 ... (fields在奇数位置) === */
    if (!strcasecmp(cmd, "hset") || !strcasecmp(cmd, "hmset")) {
        robj *key_obj = createStringObject((sds)args[1]->ptr, sdslen((sds)args[1]->ptr));
        int subkeys_count = (argc - 2) / 2;
        robj **subkeys = NULL;
        if (subkeys_count > 0) {
            subkeys = zmalloc(sizeof(robj*) * subkeys_count);
            for (int i = 0; i < subkeys_count; i++) {
                subkeys[i] = createStringObject((sds)args[2 + i * 2]->ptr, sdslen((sds)args[2 + i * 2]->ptr));
            }
        }
        gtidGapLogKeyInfo *ki = gtidGapLogKeyInfoCreate(dbid, key_obj, subkeys, subkeys_count);
        decrRefCount(key_obj);
        for (int i = 0; i < subkeys_count; i++) decrRefCount(subkeys[i]);
        zfree(subkeys);
        kis->size++;
        kis->keys = zrealloc(kis->keys, sizeof(gtidGapLogKeyInfo*) * kis->size);
        kis->keys[kis->size - 1] = ki;
        return;
    }

    /* === HDEL/HEXISTS/HMGET: key f1 f2 ... (fields在连续位置) === */
    if (!strcasecmp(cmd, "hdel") || !strcasecmp(cmd, "hexists") ||
        !strcasecmp(cmd, "hmget")) {
        robj *key_obj = createStringObject((sds)args[1]->ptr, sdslen((sds)args[1]->ptr));
        int subkeys_count = argc - 2;
        robj **subkeys = NULL;
        if (subkeys_count > 0) {
            subkeys = zmalloc(sizeof(robj*) * subkeys_count);
            for (int i = 0; i < subkeys_count; i++) {
                subkeys[i] = createStringObject((sds)args[2 + i]->ptr, sdslen((sds)args[2 + i]->ptr));
            }
        }
        gtidGapLogKeyInfo *ki = gtidGapLogKeyInfoCreate(dbid, key_obj, subkeys, subkeys_count);
        decrRefCount(key_obj);
        for (int i = 0; i < subkeys_count; i++) decrRefCount(subkeys[i]);
        zfree(subkeys);
        kis->size++;
        kis->keys = zrealloc(kis->keys, sizeof(gtidGapLogKeyInfo*) * kis->size);
        kis->keys[kis->size - 1] = ki;
        return;
    }

    /* === 单field Hash命令: HGET/HSETNX/HINCRBY/HINCRBYFLOAT key field === */
    if (!strcasecmp(cmd, "hget") || !strcasecmp(cmd, "hsetnx") ||
        !strcasecmp(cmd, "hincrby") || !strcasecmp(cmd, "hincrbyfloat") ||
        !strcasecmp(cmd, "hlen")) {
        robj *key_obj = createStringObject((sds)args[1]->ptr, sdslen((sds)args[1]->ptr));
        robj **subkeys = NULL;
        int subkeys_count = 0;
        if (argc >= 3) {
            subkeys_count = 1;
            subkeys = zmalloc(sizeof(robj*));
            subkeys[0] = createStringObject((sds)args[2]->ptr, sdslen((sds)args[2]->ptr));
        }
        gtidGapLogKeyInfo *ki = gtidGapLogKeyInfoCreate(dbid, key_obj, subkeys, subkeys_count);
        decrRefCount(key_obj);
        if (subkeys) decrRefCount(subkeys[0]);
        zfree(subkeys);
        kis->size++;
        kis->keys = zrealloc(kis->keys, sizeof(gtidGapLogKeyInfo*) * kis->size);
        kis->keys[kis->size - 1] = ki;
        return;
    }

    /* === Set命令: SADD/SREM key member1 member2 ... === */
    if (!strcasecmp(cmd, "sadd") || !strcasecmp(cmd, "srem")) {
        robj *key_obj = createStringObject((sds)args[1]->ptr, sdslen((sds)args[1]->ptr));
        int subkeys_count = argc - 2;
        robj **subkeys = NULL;
        if (subkeys_count > 0) {
            subkeys = zmalloc(sizeof(robj*) * subkeys_count);
            for (int i = 0; i < subkeys_count; i++) {
                subkeys[i] = createStringObject((sds)args[2 + i]->ptr, sdslen((sds)args[2 + i]->ptr));
            }
        }
        gtidGapLogKeyInfo *ki = gtidGapLogKeyInfoCreate(dbid, key_obj, subkeys, subkeys_count);
        decrRefCount(key_obj);
        for (int i = 0; i < subkeys_count; i++) decrRefCount(subkeys[i]);
        zfree(subkeys);
        kis->size++;
        kis->keys = zrealloc(kis->keys, sizeof(gtidGapLogKeyInfo*) * kis->size);
        kis->keys[kis->size - 1] = ki;
        return;
    }

    /* === Sorted Set命令: ZADD key score1 member1 score2 member2 ... === */
    if (!strcasecmp(cmd, "zadd")) {
        robj *key_obj = createStringObject((sds)args[1]->ptr, sdslen((sds)args[1]->ptr));
        /* ZADD格式: ZADD key [NX|XX] [CH] [INCR] score member [score member ...]
         * 需要跳过可选参数NX/XX/CH/INCR */
        int i = 2;
        while (i < argc) {
            sds arg = (sds)args[i]->ptr;
            if (!strcasecmp(arg, "nx") || !strcasecmp(arg, "xx") ||
                !strcasecmp(arg, "ch") || !strcasecmp(arg, "incr")) {
                i++;
            } else {
                break;
            }
        }
        /* 现在 i 指向第一个 score，之后是 member */
        int subkeys_count = (argc - i) / 2;
        robj **subkeys = NULL;
        if (subkeys_count > 0) {
            subkeys = zmalloc(sizeof(robj*) * subkeys_count);
            for (int j = 0; j < subkeys_count; j++) {
                subkeys[j] = createStringObject((sds)args[i + 1 + j * 2]->ptr, sdslen((sds)args[i + 1 + j * 2]->ptr));
            }
        }
        gtidGapLogKeyInfo *ki = gtidGapLogKeyInfoCreate(dbid, key_obj, subkeys, subkeys_count);
        decrRefCount(key_obj);
        for (int j = 0; j < subkeys_count; j++) decrRefCount(subkeys[j]);
        zfree(subkeys);
        kis->size++;
        kis->keys = zrealloc(kis->keys, sizeof(gtidGapLogKeyInfo*) * kis->size);
        kis->keys[kis->size - 1] = ki;
        return;
    }

    /* === ZREM key member1 member2 ... === */
    if (!strcasecmp(cmd, "zrem")) {
        robj *key_obj = createStringObject((sds)args[1]->ptr, sdslen((sds)args[1]->ptr));
        int subkeys_count = argc - 2;
        robj **subkeys = NULL;
        if (subkeys_count > 0) {
            subkeys = zmalloc(sizeof(robj*) * subkeys_count);
            for (int i = 0; i < subkeys_count; i++) {
                subkeys[i] = createStringObject((sds)args[2 + i]->ptr, sdslen((sds)args[2 + i]->ptr));
            }
        }
        gtidGapLogKeyInfo *ki = gtidGapLogKeyInfoCreate(dbid, key_obj, subkeys, subkeys_count);
        decrRefCount(key_obj);
        for (int i = 0; i < subkeys_count; i++) decrRefCount(subkeys[i]);
        zfree(subkeys);
        kis->size++;
        kis->keys = zrealloc(kis->keys, sizeof(gtidGapLogKeyInfo*) * kis->size);
        kis->keys[kis->size - 1] = ki;
        return;
    }

    /* === ZINCRBY key increment member === */
    if (!strcasecmp(cmd, "zincrby")) {
        robj *key_obj = createStringObject((sds)args[1]->ptr, sdslen((sds)args[1]->ptr));
        robj **subkeys = NULL;
        int subkeys_count = 0;
        if (argc >= 4) {
            subkeys_count = 1;
            subkeys = zmalloc(sizeof(robj*));
            subkeys[0] = createStringObject((sds)args[3]->ptr, sdslen((sds)args[3]->ptr));
        }
        gtidGapLogKeyInfo *ki = gtidGapLogKeyInfoCreate(dbid, key_obj, subkeys, subkeys_count);
        decrRefCount(key_obj);
        if (subkeys) decrRefCount(subkeys[0]);
        zfree(subkeys);
        kis->size++;
        kis->keys = zrealloc(kis->keys, sizeof(gtidGapLogKeyInfo*) * kis->size);
        kis->keys[kis->size - 1] = ki;
        return;
    }

    /* === 其他所有命令：第一个参数是key，无 subkey === */
    robj *key_obj = createStringObject((sds)args[1]->ptr, sdslen((sds)args[1]->ptr));
    gtidGapLogKeyInfo *ki = gtidGapLogKeyInfoCreate(dbid, key_obj, NULL, 0);
    decrRefCount(key_obj);
    kis->size++;
    kis->keys = zrealloc(kis->keys, sizeof(gtidGapLogKeyInfo*) * kis->size);
    kis->keys[kis->size - 1] = ki;
}

/* =====================================================
 * Gaplog 解析辅助函数
 * ===================================================== */

/**
 * 解析普通 GTID 包装的命令
 * GTID 格式: GTID <uuid:gno> <dbid> <command> [args...]
 *
 * 参数:
 *   mock - 已解析的 mock client
 *   kis  - 用于存储 key 信息的结构
 * 返回:
 *   0 成功, -1 失败
 */
static int parseGtidCommand(client *mock, gtidGapLogKeysInfos *kis) {
    int dbid = 0;

    /* 安全检查：GTID 命令格式: GTID <uuid:gno> <dbid> <command> [args...] */
    if (mock->argc < 4 || mock->argv == NULL || mock->argv[2] == NULL) {
        serverLog(LL_WARNING, "[gaplog] invalid GTID command, argc=%d", mock->argc);
        return -1;
    }

    getLongLongFromObject(mock->argv[2], (long long*)&dbid);

    /* 跳过 GTID 头部（argv[0]=GTID, argv[1]=uuid:gno, argv[2]=dbid） */
    addKeyInfoToKeysInfos(kis, dbid, mock->argv + 3, mock->argc - 3);
    return 0;
}

/**
 * 解析 MULTI/EXEC 事务命令
 * backlog 格式: MULTI -> [SELECT db] -> [SET k v]... -> GTID <uuid:gno> <dbid> EXEC
 *
 * 注意: 调用此函数时，mock 已经解析了 MULTI 命令
 *
 * 解析流程:
 * 1. 从 MULTI 之后开始解析命令，直到 GTID EXEC
 * 2. 使用 select_dbid 作为初始 dbid（如果有），否则从 GTID EXEC 获取
 * 3. 遍历所有命令，如果有 SELECT 就更新 dbid
 *
 * 参数:
 *   mock            - 已解析 MULTI 命令的 mock client
 *   multi_end_off   - MULTI 命令结束后的 backlog 偏移量（即第一条内部命令的起始位置）
 *   select_dbid     - MULTI 前面的 SELECT 命令记录的 dbid（-1 表示没有）
 *   kis             - 用于存储 key 信息的结构
 * 返回:
 *   0 成功, -1 失败
 */
static int parseMultiCommand(client *mock, long long multi_end_off, long long select_dbid, gtidGapLogKeysInfos *kis) {
    long long next_off = multi_end_off;

    serverLog(LL_WARNING, "[gaplog] parseMultiCommand: first inner cmd at offset %lld", next_off);

    /* 解析所有命令，收集到列表中 */
    gtidParsedCmdList cmdlist = {0};

    while (1) {
        client inner_c;
        size_t inner_cmd_len = 0;
        if (parseCmdFromBacklogNoSkip(next_off, &inner_c, &inner_cmd_len) < 0)
            break;

        if (inner_c.argc < 1 || inner_c.argv == NULL || inner_c.argv[0] == NULL) {
            // mockClientCleanup(&inner_c);
            break;
        }

        /* 检查是否是 GTID EXEC */
        sds inner_cmd = (sds)inner_c.argv[0]->ptr;
        int is_gtid_wrapped = !strcasecmp(inner_cmd, "gtid");
        sds actual_cmd_name = inner_cmd;

        if (is_gtid_wrapped && inner_c.argc >= 4 && inner_c.argv[3] != NULL) {
            actual_cmd_name = (sds)inner_c.argv[3]->ptr;
        }

        int is_exec = !strcasecmp(actual_cmd_name, "exec");

        /* 添加到命令列表 */
        gtidParsedCmdListAdd(&cmdlist, &inner_c);

        if (is_exec) {
            break;  /* 到达 EXEC，停止解析 */
        }
        next_off += inner_cmd_len;
    }

    if (cmdlist.num_cmds == 0) {
        serverLog(LL_WARNING, "[gaplog] no commands found after MULTI at offset %lld", next_off);
        return -1;
    }

    /* 获取初始 dbid：优先使用 select_dbid，否则从 GTID EXEC 获取 */
    int dbid = 0;
    if (select_dbid >= 0) {
        dbid = (int)select_dbid;
    } else {
        gtidParsedCmd *last_cmd = &cmdlist.cmds[cmdlist.num_cmds - 1];
        if (last_cmd->argv != NULL && last_cmd->argv[0] != NULL) {
            sds last_cmd_name = (sds)last_cmd->argv[0]->ptr;
            if (!strcasecmp(last_cmd_name, "gtid") && last_cmd->argc >= 3 && last_cmd->argv[2] != NULL) {
                getLongLongFromObject(last_cmd->argv[2], (long long*)&dbid);
            }
        }
    }

    /* 第二遍：处理所有命令（跳过最后一个 EXEC），生成 keyinfos */
    for (int i = 0; i < cmdlist.num_cmds - 1; i++) {
        gtidParsedCmd *cmd = &cmdlist.cmds[i];
        if (cmd->argv == NULL || cmd->argv[0] == NULL) continue;

        sds cmd_name = (sds)cmd->argv[0]->ptr;

        /* SELECT 命令更新 dbid */
        if (!strcasecmp(cmd_name, "select") && cmd->argc >= 2 && cmd->argv[1] != NULL) {
            getLongLongFromObject(cmd->argv[1], (long long*)&dbid);
            continue;
        }

        /* 非 SELECT 命令，提取 keys */
        addKeyInfoToKeysInfos(kis, dbid, cmd->argv, cmd->argc);
    }

    gtidParsedCmdListCleanup(&cmdlist);
    return 0;
}

/**
 * 保存 gaplog 条目到数据结构
 *
 * 参数:
 *   uuid     - GTID 的 uuid
 *   uuid_len - uuid 长度
 *   gno      - GTID 的 gno
 *   kis      - key 信息结构
 * 返回:
 *   0 成功, -1 失败
 */
static int saveGapLogEntry(char *uuid, size_t uuid_len, gno_t gno, gtidGapLogKeysInfos *kis) {
    if (kis->size == 0) return 0;

    sds uuid_sds = sdsnewlen(uuid, uuid_len);

    /* 创建 gno entry */
    gtidGapLogGnoEntry *gno_entry = gtidGapLogGnoEntryCreate(gno, kis);

    /* 1. 处理 gtid_gap_log_list (FIFO 顺序) */
    uuidSet *last_uuid_set = NULL;
    listNode *tail_ln = listLast(server.gtid_gap_log_list);
    if (tail_ln != NULL) {
        last_uuid_set = (uuidSet*)listNodeValue(tail_ln);
        if (last_uuid_set->uuid_len != uuid_len ||
            memcmp(last_uuid_set->uuid, uuid, uuid_len) != 0) {
            last_uuid_set = NULL;
        }
    }

    if (last_uuid_set != NULL) {
        uuidSetAdd(last_uuid_set, gno, gno);
    } else {
        uuidSet *new_uuid_set = uuidSetNew(uuid, uuid_len);
        uuidSetAdd(new_uuid_set, gno, gno);
        listAddNodeTail(server.gtid_gap_log_list, new_uuid_set);
    }

    /* 2. 处理 gtid_gap_log 字典 (按 gno 升序存储) */
    dictEntry *de = dictFind(server.gtid_gap_log, uuid_sds);
    list *gno_list;
    if (de == NULL) {
        gno_list = listCreate();
        listSetFreeMethod(gno_list, gtidGapLogGnoEntryFree);
        sds uuid_key = sdsdup(uuid_sds);
        dictAdd(server.gtid_gap_log, uuid_key, gno_list);
    } else {
        gno_list = dictGetVal(de);
    }

    /* 按 gno 升序插入到 list 中 */
    listIter li;
    listNode *ln;
    listRewind(gno_list, &li);
    listNode *insert_after = NULL;
    while ((ln = listNext(&li)) != NULL) {
        gtidGapLogGnoEntry *entry = (gtidGapLogGnoEntry*)listNodeValue(ln);
        if (entry->gno > gno) {
            break;
        }
        insert_after = ln;
    }
    if (insert_after == NULL) {
        listAddNodeHead(gno_list, gno_entry);
    } else if (ln == NULL) {
        listAddNodeTail(gno_list, gno_entry);
    } else {
        listInsertNode(gno_list, insert_after, gno_entry, 1);
    }

    sdsfree(uuid_sds);
    server.gtid_gaplog_entry_count++;
    return 0;
}

/**
 * 逐出最老的 gaplog 条目（FIFO 策略）
 *
 * 返回:
 *   逐出的条目数
 */
static int evictOldestGapLogEntry(void) {
    listNode *first_ln = listFirst(server.gtid_gap_log_list);
    if (first_ln == NULL) return 0;

    uuidSet *first_uuid_set = (uuidSet*)listNodeValue(first_ln);

    /* 从 uuidSet 中获取最小 gno */
    gno_t min_gno = uuidSetNext(first_uuid_set, 0);
    if (min_gno == 0) {
        /* uuidSet 为空，删除整个 node */
        listDelNode(server.gtid_gap_log_list, first_ln);
        return 0;
    }

    /* 从 uuidSet 中删除最小 gno */
    uuidSetRemove(first_uuid_set, min_gno, min_gno);

    /* 从 gtid_gap_log 字典中删除对应的 keys_infos */
    sds evict_uuid_sds = sdsnewlen(first_uuid_set->uuid, first_uuid_set->uuid_len);
    dictEntry *de = dictFind(server.gtid_gap_log, evict_uuid_sds);
    if (de != NULL) {
        list *gno_list = dictGetVal(de);
        listNode *first_gno_ln = listFirst(gno_list);
        if (first_gno_ln != NULL) {
            gtidGapLogGnoEntry *entry = (gtidGapLogGnoEntry*)listNodeValue(first_gno_ln);
            if (entry->gno == min_gno) {
                listDelNode(gno_list, first_gno_ln);
            }
        }
        /* 如果 list 为空，删除字典条目 */
        if (listLength(gno_list) == 0) {
            dictDelete(server.gtid_gap_log, evict_uuid_sds);
        }
    }
    sdsfree(evict_uuid_sds);

    /* 如果 uuidSet 为空，删除整个 node */
    if (uuidSetCount(first_uuid_set) == 0) {
        listDelNode(server.gtid_gap_log_list, first_ln);
    }

    server.gtid_gaplog_entry_count--;
    return 1;
}

void saveGapLogFromGtidSet(gtidSet *mlost) {
    if (mlost == NULL || server.gtid_seq == NULL) return;
    if (server.gtid_gap_log_list == NULL) return;

    size_t saved_count = 0;
    uuidSet *us = mlost->header;

    serverLog(LL_NOTICE, "[gaplog] saveGapLogFromGtidSet called, gtid_seq=%p", (void*)server.gtid_seq);

    while (us) {
        gtidIntervalNode *node = us->intervals->header->forwards[0];
        while (node) {
            for (gno_t gno = node->start; gno <= node->end; gno++) {
                /* 1. 通过gtidSeq查找backlog偏移量 */
                long long offset = gtidSeqLookup(server.gtid_seq, us->uuid,
                                                  us->uuid_len, gno);
                serverLog(LL_NOTICE, "[gaplog] gtidSeqLookup(uuid=%.*s, gno=%lld) = %lld",
                          (int)us->uuid_len, us->uuid, (long long)gno, offset);
                if (offset < 0) continue;

                /* 2. 解析backlog命令，可能需要跳过 SELECT 找到 MULTI */
                client mock;
                long long cur_offset = offset;
                int dbid_from_select = -1;  /* SELECT 命令记录的 dbid */
                /* 3. 创建 keys_infos 结构 */
                gtidGapLogKeysInfos *kis = gtidGapLogKeysInfosCreate();

                while (1) {
                    size_t cur_cmd_len = 0;
                    if (parseGtidCmdFromBacklog(cur_offset, &mock, &cur_cmd_len) < 0) {
                        serverLog(LL_WARNING, "[gaplog] parseGtidCmdFromBacklog failed at offset %lld", cur_offset);
                        break;
                    }
                    if (mock.argc < 1 || mock.argv == NULL || mock.argv[0] == NULL) {
                        serverLog(LL_WARNING, "[gaplog] invalid mock client at offset %lld, argc=%d", cur_offset, mock.argc);
                        // mockClientCleanup(&mock);
                        break;
                    }

                    sds cmd_name = (sds)mock.argv[0]->ptr;

                    /* SELECT 命令：记录 dbid，继续解析下一个命令 */
                    if (!strcasecmp(cmd_name, "select") && mock.argc >= 2) {
                        getLongLongFromObject(mock.argv[1], (long long*)&dbid_from_select);
                        serverLog(LL_WARNING, "[gaplog] found SELECT %d at offset %lld, continue", dbid_from_select, cur_offset);
                        cur_offset = cur_offset + cur_cmd_len;
                        // mockClientCleanup(&mock);
                        continue;
                    }

                    /* MULTI 命令：正确入口 */
                    if (!strcasecmp(cmd_name, "multi")) {
                        /* 4. 调用 parseMultiCommand 解析 MULTI 事务
                         * cur_offset 是 SELECT 的起始，cur_cmd_len 是 SELECT+MULTI 的总长度
                         * MULTI 之后的第一条命令在 cur_offset + cur_cmd_len */
                        serverLog(LL_WARNING, "[gaplog] found multi %d at offset %lld, cur_cmd_len=%zu", dbid_from_select, cur_offset, cur_cmd_len);
                        parseMultiCommand(&mock, cur_offset + cur_cmd_len, dbid_from_select, kis);
                        break;
                    }

                    /* 处理GTID 命令 */
                    if (!strcasecmp(cmd_name, "gtid")) {
                        serverAssert(dbid_from_select == -1);
                        parseGtidCommand(&mock, kis);
                        break;
                    }

                    /* 其他命令：不应该出现 */
                    serverLog(LL_WARNING, "[gaplog] unexpected command '%s' at offset %lld, expected SELECT or MULTI", cmd_name, cur_offset);
                    mockClientCleanup(&mock);
                    serverAssert("gtidSeqLookup error" && 0);
                }


                

                

                mockClientCleanup(&mock);

                /* 5. 保存到 gaplog 数据结构 */
                if (kis->size > 0) {
                    saveGapLogEntry(us->uuid, us->uuid_len, gno, kis);
                    saved_count++;

                    /* 6. FIFO逐出: 若超过最大条目数,从最旧的开始逐出 */
                    while (server.gtid_gaplog_entry_count > (long long)server.gtid_xsync_max_gap) {
                        evictOldestGapLogEntry();
                    }
                } else {
                    gtidGapLogKeysInfosFree(kis);
                }
            }
            node = node->forwards[0];
        }
        us = us->next;
    }

    if (saved_count > 0) {
        serverLog(LL_NOTICE, "[gaplog] saved %zu gap log entries from %llu lost GTIDs",
                  saved_count, (unsigned long long)gtidSetCount(mlost));
    }
}
int ctrip_slaveTryPartialResynchronizationRead(connection *conn, sds reply) {
    int result = PSYNC_BY_REDIS;

    serverLog(LL_NOTICE, "[gtid] got sync reply: %s",reply);

    parsedSyncReply *parsed = parseSyncReply(reply);

    if (parsed->type == SYNC_REPLY_TRANSERR) goto by_redis;
    if (parsed->type == SYNC_REPLY_TRANSERR2) {
        serverLog(LL_NOTICE,
                "[%s] Treat %s as transient error too, try psync again.",
                replModeName(server.repl_mode->mode), reply);
        result = PSYNC_TRY_LATER;
        goto end;
    }

    if (parsed->type == SYNC_REPLY_INVALID) {
        serverLog(LL_WARNING, "[%s] Parsed invalid reply(%s): "
                "fallback to fullresync",replModeName(server.repl_mode->mode),
                parsed->invalid.errmsg);
        serverReplStreamMasterLinkBroken();
        result = PSYNC_TRY_LATER;
        goto end;
    }

    if (server.repl_mode->mode != REPL_MODE_XSYNC) {
        if (parsed->type == SYNC_REPLY_XFULLRESYNC) {
            /* PSYNC => XFULLRESYNC */
            server.gtid_sync_stat[GTID_SYNC_PSYNC_XFULLRESYNC]++;
            serverLog(LL_NOTICE,
                    "[psync] repl mode switch: psync => xsync (xfullresync)");
            /* Repl mode will be reset on rdb loading */
            parsedSyncReplySetupGtidInital(parsed);
            replicationDiscardCachedMaster();
            result = PSYNC_FULLRESYNC;
        } else if (parsed->type == SYNC_REPLY_XCONTINUE) {
            /* PSYNC => XCONTINUE */
            server.gtid_sync_stat[GTID_SYNC_PSYNC_XCONTINUE]++;
            serverLog(LL_NOTICE,
                    "[psync] repl mode switch: psync => xsync (xcontinue)");

            /* align gtid.set with master */
            gtidSet *reply_executed = gtidSetDup(parsed->xcontinue.gtid_cont);
            gtidSetDiff(reply_executed,parsed->xcontinue.gtid_lost);

            sds gtid_executed_repr = gtidSetDump(server.gtid_executed);
            sds gtid_lost_repr = gtidSetDump(server.gtid_lost);
            sds reply_cont_repr = gtidSetDump(parsed->xcontinue.gtid_cont);
            sds reply_executed_repr = gtidSetDump(reply_executed);
            sds reply_lost_repr = gtidSetDump(parsed->xcontinue.gtid_lost);

            serverLog(LL_NOTICE,
                    "[psync] reply gtid.executed(%s) = gtid.cont(%s) - gtid.lost(%s)",
                    reply_executed_repr,reply_cont_repr,reply_lost_repr);

            serverLog(LL_NOTICE,"[psync] align my gtid.sets with master, "
                    "gtid.executed: %s => %s, gtid.lost: %s => %s",
                    gtid_executed_repr,reply_executed_repr,gtid_lost_repr,reply_lost_repr);

            sdsfree(reply_lost_repr);
            sdsfree(reply_executed_repr);
            sdsfree(reply_cont_repr);
            sdsfree(gtid_lost_repr);
            sdsfree(gtid_executed_repr);

            serverReplStreamSwitch2Xsync(
                    parsed->xcontinue.replid,parsed->xcontinue.reploff,
                    parsed->xcontinue.master_uuid,
                    reply_executed,parsed->xcontinue.gtid_lost,RS_UPDATE_DOWN,
                    "slave psync=>xcontinue");
            serverReplStreamResurrectCreate(conn,-1,
                    parsed->xcontinue.replid,parsed->xcontinue.reploff);
            result = PSYNC_CONTINUE;

            gtidSetFree(reply_executed);
        } else {
            /* psync => fullresync, psync => continue handled by redis. */
            if (parsed->type == SYNC_REPLY_FULLRESYNC) {
                server.gtid_sync_stat[GTID_SYNC_PSYNC_FULLRESYNC]++;
            }
            if (parsed->type == SYNC_REPLY_CONTINUE) {
                server.gtid_sync_stat[GTID_SYNC_PSYNC_CONTINUE]++;
            }
            goto by_redis;
        }
    } else {
        serverAssert(!server.cached_master && !server.master);
        if (parsed->type == SYNC_REPLY_FULLRESYNC) {
            /* XSYNC => FULLRESYNC */
            server.gtid_sync_stat[GTID_SYNC_XSYNC_FULLRESYNC]++;
            serverLog(LL_NOTICE,
                    "[xsync] repl mode switch: xsync => psync (fullresync)");
            goto by_redis;
        } else if (parsed->type == SYNC_REPLY_CONTINUE) {
            /* XSYNC => CONTINUE */
            server.gtid_sync_stat[GTID_SYNC_XSYNC_CONTINUE]++;
            serverLog(LL_NOTICE,
                    "[xsync] repl mode switch: xsync => psync (continue)");
            sds replid = parsed->pcontinue.replid;
            long long reploff = parsed->pcontinue.reploff;
            serverReplStreamSwitch2Psync(replid,reploff,RS_UPDATE_DOWN,
                    "slave xsync=>continue");
            serverReplStreamResurrectCreate(conn,-1,replid,reploff);
            result = PSYNC_CONTINUE;
        } else if (parsed->type == SYNC_REPLY_XFULLRESYNC) {
            /* XSYNC => XFULLRESYNC */
            server.gtid_sync_stat[GTID_SYNC_XSYNC_XFULLRESYNC]++;
            serverLog(LL_NOTICE,"[xsync] XFullResync from master: %s.", reply);
            parsedSyncReplySetupGtidInital(parsed);
            result = PSYNC_FULLRESYNC;
        } else if (parsed->type == SYNC_REPLY_XCONTINUE) {
            /* XSYNC => XCONTINUE */
            server.gtid_sync_stat[GTID_SYNC_XSYNC_XCONTINUE]++;
            serverLog(LL_NOTICE,
                    "[xsync] Successful partial xsync with master: %s", reply);

            gtidSet *gtid_cont = parsed->xcontinue.gtid_cont;
            gtidSet *gtid_slost = NULL, *gtid_slave = NULL, *gtid_mlost = NULL;
            sds gtid_cont_repr, gtid_slave_repr, gtid_slost_repr;

            gtid_slave = serverGtidSetGet("[xsync]");
            gtid_slave_repr = gtidSetDump(gtid_slave);
            gtid_slost = gtidSetDup(gtid_cont);
            gtidSetDiff(gtid_slost,gtid_slave);
            gtid_cont_repr = gtidSetDump(gtid_cont);
            gtid_slost_repr = gtidSetDump(gtid_slost);
            serverLog(LL_NOTICE, "[xsync] gtid.set-slost(%s) = "
                    "gtid.set-continue(%s) - gtid.set-slave(%s)",
                    gtid_slost_repr,gtid_cont_repr,gtid_slave_repr);
            gtid_mlost = gtidSetDup(gtid_slave);
            gtidSetDiff(gtid_mlost, gtid_cont);
            sds gtid_mlost_repr = gtidSetDump(gtid_mlost);
            serverLog(LL_WARNING, "[gaplog] gtid_mlost = %s, count = %d",
                      gtid_mlost_repr, (int)gtidSetCount(gtid_mlost));
            sdsfree(gtid_mlost_repr);
            if (gtidSetCount(gtid_mlost) > 0) {
                saveGapLogFromGtidSet(gtid_mlost);
            }

            /* Update gtid lost, master.uuid or replid/reploff. */
            serverReplStreamUpdateXsync(gtid_slost,NULL,
                    parsed->xcontinue.master_uuid,
                    parsed->xcontinue.replid,parsed->xcontinue.reploff);
            serverReplStreamResurrectCreate(conn,-1,
                    parsed->xcontinue.replid,parsed->xcontinue.reploff);

            sdsfree(gtid_cont_repr), sdsfree(gtid_slave_repr),
                sdsfree(gtid_slost_repr);
            gtidSetFree(gtid_slost), gtidSetFree(gtid_slave), gtidSetFree(gtid_mlost);

            result = PSYNC_CONTINUE;
        }
    }

end:
    sdsfree(reply);
    parsedSyncReplyFree(parsed);
    return result;

by_redis:
    parsedSyncReplyFree(parsed);
    return PSYNC_BY_REDIS;
}

#ifdef REDIS_TEST
int gtidTest(int argc, char **argv, int accurate) {
    UNUSED(argc), UNUSED(argv), UNUSED(accurate);

    int error = 0;
    server.maxmemory_policy = MAXMEMORY_FLAG_LFU;
    if (!server.logfile) server.logfile = zstrdup(CONFIG_DEFAULT_LOGFILE);

    TEST("gtid - parse xsync request") {
        syncRequest *request = syncRequestNew();
        robj *optargv[4];
        robj *gtidset = createStringObject("A:1-100,B", 9);
        robj *uuid_interested = createStringObject("*",1);
        optargv[0] = createStringObject("GTID.LOST",9);
        optargv[1] = createStringObject("A:81-100", 8);
        optargv[2] = createStringObject("MAXGAP",6);
        optargv[3] = createStringObject("10000", 5);
        masterParseXsyncRequest(request,uuid_interested,gtidset,4,optargv);
        test_assert(request->mode == REPL_MODE_XSYNC);
        test_assert(request->x.maxgap == 10000);
        test_assert(gtidSetCount(request->x.gtid_slave) == 100);
        test_assert(gtidSetCount(request->x.gtid_lost) == 20);
        decrRefCount(optargv[0]);
        decrRefCount(optargv[1]);
        decrRefCount(optargv[2]);
        decrRefCount(optargv[3]);
        decrRefCount(gtidset);
        decrRefCount(uuid_interested);
        syncRequestFree(request);
    }

    TEST("gtid - parse invalid xsync request") {
        syncRequest *request = syncRequestNew();
        robj *gtidset = createStringObject("hello:world", 11);
        robj *uuid_interested = createStringObject("*",1);
        masterParseXsyncRequest(request,uuid_interested,gtidset,0,NULL);
        test_assert(request->mode == REPL_MODE_UNSET);
        test_assert(!strcmp(request->i.msg, "invalid gtid.set hello:world"));
        decrRefCount(gtidset);
        decrRefCount(uuid_interested);
        syncRequestFree(request);
    }

    TEST("gtid - locate sync request") {
        syncLocateResult slr[1];

        /* {(xsync:2000), (psync:1000)}*/
        server.prev_repl_mode->from = 1000, server.prev_repl_mode->mode = REPL_MODE_PSYNC;
        server.repl_mode->from = 2000, server.repl_mode->mode = REPL_MODE_XSYNC;

        syncLocateResultInit(slr);
        locateServerReplMode(REPL_MODE_XSYNC,3000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_CUR);
        test_assert(slr->locate_mode == REPL_MODE_XSYNC);
        syncLocateResultDeinit(slr);

        syncLocateResultInit(slr);
        locateServerReplMode(REPL_MODE_XSYNC,2000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_CUR);
        test_assert(slr->locate_mode == REPL_MODE_XSYNC);
        syncLocateResultDeinit(slr);

        syncLocateResultInit(slr);
        locateServerReplMode(REPL_MODE_PSYNC,2000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_SWITCH);
        test_assert(slr->locate_mode == REPL_MODE_XSYNC);
        syncLocateResultDeinit(slr);

        syncLocateResultInit(slr);
        locateServerReplMode(REPL_MODE_XSYNC,1000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_INVALID);
        syncLocateResultDeinit(slr);

        syncLocateResultInit(slr);
        locateServerReplMode(REPL_MODE_PSYNC,1000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_PREV);
        test_assert(slr->locate_mode == REPL_MODE_PSYNC);
        test_assert(slr->p.limit == 1000);
        syncLocateResultDeinit(slr);

        syncLocateResultInit(slr);
        locateServerReplMode(REPL_MODE_XSYNC,500,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_INVALID);
        syncLocateResultDeinit(slr);

        /* {(psync:2000), (xsync:1000)}*/
        server.prev_repl_mode->from = 1000, server.prev_repl_mode->mode = REPL_MODE_XSYNC;
        server.repl_mode->from = 2000, server.repl_mode->mode = REPL_MODE_PSYNC;

        syncLocateResultInit(slr);
        locateServerReplMode(REPL_MODE_PSYNC,3000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_CUR);
        test_assert(slr->locate_mode == REPL_MODE_PSYNC);
        syncLocateResultDeinit(slr);

        syncLocateResultInit(slr);
        locateServerReplMode(REPL_MODE_PSYNC,2000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_CUR);
        test_assert(slr->locate_mode == REPL_MODE_PSYNC);
        syncLocateResultDeinit(slr);

        syncLocateResultInit(slr);
        locateServerReplMode(REPL_MODE_XSYNC,2000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_SWITCH);
        test_assert(slr->locate_mode == REPL_MODE_PSYNC);
        syncLocateResultDeinit(slr);

        syncLocateResultInit(slr);
        locateServerReplMode(REPL_MODE_PSYNC,1000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_INVALID);
        syncLocateResultDeinit(slr);

        syncLocateResultInit(slr);
        locateServerReplMode(REPL_MODE_XSYNC,1000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_PREV);
        test_assert(slr->locate_mode == REPL_MODE_XSYNC);
        test_assert(slr->p.limit == 1000);
        syncLocateResultDeinit(slr);

        syncLocateResultInit(slr);
        locateServerReplMode(REPL_MODE_XSYNC,500,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_INVALID);
        syncLocateResultDeinit(slr);

        /* {(xsync:2000), {?:1000}}*/
        server.prev_repl_mode->from = 1000, server.prev_repl_mode->mode = REPL_MODE_UNSET;
        server.repl_mode->from = 2000, server.repl_mode->mode = REPL_MODE_XSYNC;

        syncLocateResultInit(slr);
        locateServerReplMode(REPL_MODE_XSYNC,3000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_CUR);
        test_assert(slr->locate_mode == REPL_MODE_XSYNC);
        syncLocateResultDeinit(slr);

        syncLocateResultInit(slr);
        locateServerReplMode(REPL_MODE_XSYNC,2000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_CUR);
        test_assert(slr->locate_mode == REPL_MODE_XSYNC);
        syncLocateResultDeinit(slr);

        syncLocateResultInit(slr);
        locateServerReplMode(REPL_MODE_PSYNC,2000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_INVALID);
        syncLocateResultDeinit(slr);

        syncLocateResultInit(slr);
        locateServerReplMode(REPL_MODE_PSYNC,1000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_INVALID);
        syncLocateResultDeinit(slr);

        /* {(psync:1000), (xsync:1000)}*/
        server.prev_repl_mode->from = 1000, server.prev_repl_mode->mode = REPL_MODE_XSYNC;
        server.repl_mode->from = 1000, server.repl_mode->mode = REPL_MODE_PSYNC;

        syncLocateResultInit(slr);
        locateServerReplMode(REPL_MODE_PSYNC,1000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_CUR);
        syncLocateResultDeinit(slr);

        syncLocateResultInit(slr);
        locateServerReplMode(REPL_MODE_XSYNC,1000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_SWITCH);
        syncLocateResultDeinit(slr);

        syncLocateResultInit(slr);
        locateServerReplMode(REPL_MODE_XSYNC,900,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_INVALID);
        syncLocateResultDeinit(slr);
    }

    TEST("gtid - parse sync reply") {
        parsedSyncReply *parsed;
        sds reply, replid = sdsnew("0123456789012345678901234567890123456789"),
            master_uuid = sdsnew("A");
        gtidSet *gtid_cont = gtidSetDecode("A:1,B:2",7);
        gtidSet *gtid_lost = gtidSetDecode("C:3",3);

        reply = sdsnew("+FULLRESYNC invalid_replid 10");
        parsed = parsedSyncReplyNew();
        parseSyncReplyFullresync(reply,parsed);
        test_assert(parsed->type == SYNC_REPLY_INVALID);
        parsedSyncReplyFree(parsed), sdsfree(reply);

        reply = sdsnew("+FULLRESYNC 0123456789012345678901234567890123456789 1000");
        parsed = parsedSyncReplyNew();
        parseSyncReplyFullresync(reply,parsed);
        test_assert(parsed->type = SYNC_REPLY_FULLRESYNC);
        test_assert(sdscmp(parsed->fullresync.replid, replid) == 0);
        test_assert(parsed->fullresync.reploff == 1000);
        parsedSyncReplyFree(parsed), sdsfree(reply);

        reply = sdsnew("+CONTINUE");
        parsed = parsedSyncReplyNew();
        parseSyncReplyContinue(reply,parsed);
        test_assert(parsed->type == SYNC_REPLY_CONTINUE);
        test_assert(parsed->pcontinue.replid == NULL);
        test_assert(parsed->pcontinue.reploff_is_set == 0);
        parsedSyncReplyFree(parsed), sdsfree(reply);

        reply = sdsnew("+CONTINUE 0123456789012345678901234567890123456789");
        parsed = parsedSyncReplyNew();
        parseSyncReplyContinue(reply,parsed);
        test_assert(parsed->type == SYNC_REPLY_CONTINUE);
        test_assert(sdscmp(parsed->pcontinue.replid,replid) == 0);
        test_assert(parsed->pcontinue.reploff_is_set == 0);
        parsedSyncReplyFree(parsed), sdsfree(reply);

        reply = sdsnew("+CONTINUE 0123456789012345678901234567890123456789 1234");
        parsed = parsedSyncReplyNew();
        parseSyncReplyContinue(reply,parsed);
        test_assert(parsed->type == SYNC_REPLY_CONTINUE);
        test_assert(sdscmp(parsed->pcontinue.replid,replid) == 0);
        test_assert(parsed->pcontinue.reploff_is_set == 1);
        test_assert(parsed->pcontinue.reploff == 1234);
        parsedSyncReplyFree(parsed), sdsfree(reply);

        reply = sdsnew("+CONTINUE invalid_replid 1234");
        parsed = parsedSyncReplyNew();
        parseSyncReplyContinue(reply,parsed);
        test_assert(parsed->type == SYNC_REPLY_INVALID);
        parsedSyncReplyFree(parsed), sdsfree(reply);

        reply = sdsnew("+XFULLRESYNC");
        parsed = parsedSyncReplyNew();
        parseSyncReplyXfullresync(reply,parsed);
        test_assert(parsed->type == SYNC_REPLY_INVALID);
        parsedSyncReplyFree(parsed), sdsfree(reply);

        reply = sdsnew("+XFULLRESYNC GTID.LOST \"\" MASTER.UUID master-uuid");
        parsed = parsedSyncReplyNew();
        parseSyncReplyXfullresync(reply,parsed);
        test_assert(parsed->type == SYNC_REPLY_INVALID);
        parsedSyncReplyFree(parsed), sdsfree(reply);

        reply = sdsnew("+XFULLRESYNC GTID.LOST \"\" MASTER.UUID master-uuid REPLID 0123456789012345678901234567890123456789 REPLOFF 1234 FOO BAR\r\n");
        parsed = parsedSyncReplyNew();
        parseSyncReplyXfullresync(reply,parsed);
        test_assert(parsed->type == SYNC_REPLY_XFULLRESYNC);
        parsedSyncReplyFree(parsed), sdsfree(reply);

        reply = sdsnew("+XCONTINUE");
        parsed = parsedSyncReplyNew();
        parseSyncReplyXcontinue(reply,parsed);
        test_assert(parsed->type == SYNC_REPLY_INVALID);
        parsedSyncReplyFree(parsed), sdsfree(reply);

        reply = sdsnew("+XCONTINUE GTID.SET A:1,B:2 MASTER.UUID A");
        parsed = parsedSyncReplyNew();
        parseSyncReplyXcontinue(reply,parsed);
        test_assert(parsed->type == SYNC_REPLY_INVALID);
        parsedSyncReplyFree(parsed), sdsfree(reply);

        reply = sdsnew("+XCONTINUE REPLID 0123456789012345678901234567890123456789 REPLOFF 1234 GTID.SET A:1,B:2 MASTER.UUID A FOO BAR");
        parsed = parsedSyncReplyNew();
        parseSyncReplyXcontinue(reply,parsed);
        test_assert(parsed->type == SYNC_REPLY_XCONTINUE);
        test_assert(gtidSetEqual(parsed->xcontinue.gtid_cont,gtid_cont));
        test_assert(gtidSetCount(parsed->xcontinue.gtid_lost) == 0);
        test_assert(sdscmp(parsed->xcontinue.master_uuid,master_uuid) == 0);
        test_assert(sdscmp(parsed->xcontinue.replid,replid) == 0);
        test_assert(parsed->xcontinue.reploff == 1234);
        parsedSyncReplyFree(parsed), sdsfree(reply);

        reply = sdsnew("+XCONTINUE REPLID 0123456789012345678901234567890123456789 REPLOFF 1234 GTID.SET A:1,B:2 GTID.LOST C:3 MASTER.UUID A FOO BAR");
        parsed = parsedSyncReplyNew();
        parseSyncReplyXcontinue(reply,parsed);
        test_assert(parsed->type == SYNC_REPLY_XCONTINUE);
        test_assert(gtidSetEqual(parsed->xcontinue.gtid_cont,gtid_cont));
        test_assert(gtidSetEqual(parsed->xcontinue.gtid_lost,gtid_lost));
        test_assert(sdscmp(parsed->xcontinue.master_uuid,master_uuid) == 0);
        test_assert(sdscmp(parsed->xcontinue.replid,replid) == 0);
        test_assert(parsed->xcontinue.reploff == 1234);
        parsedSyncReplyFree(parsed), sdsfree(reply);

        sdsfree(replid), sdsfree(master_uuid);
        gtidSetFree(gtid_cont);
        gtidSetFree(gtid_lost);
    }

    return error;
}

#endif


