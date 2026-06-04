



#ifndef GTID_ADAPTATION_VERSION_H
#define GTID_ADAPTATION_VERSION_H
#include "server.h"

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
/* dict */
dict* gtidDictCreate(dictType *type);

/* command */
struct redisCommand* gtidLookupCommandBySds(sds name);
char* gtidRedisCommandGetName(struct redisCommand* cmd);


/* obj  */
char* gtidGetObjectTypeName(int key_type);


/* getKeysResult*/
int gtidGetKeysResultKeyIndex(getKeysResult* result, int index);

/* backlog */
size_t gtidBacklogAppendToSds(long long offset, sds *dst, size_t size);
long long gtidGetBacklogOffset();
long long gtidGetBacklogHistlen();
void gtidFreeReplicationBacklog();
void gtidCreateReplicationBacklog();

syncRequest *masterParseSyncRequest(client *c);
syncResult *masterAnaSyncRequest(syncRequest *request);
int masterReplySyncRequest(client *c, long long psync_offset, syncResult *result);
void syncRequestFree(syncRequest *request);
void syncResultFree(syncResult *result);
long long addReplyReplicationBacklogLimited(client *c, long long offset, long long limit);
int masterTryPartialResynchronization(client *c, long long psync_offset);
const char *xsyncUuidInterestedGet(void);
typedef void (*consume_cb)(char *p, long long thislen, void *pd);
long long consumeReplicationBacklogLimited(long long offset, long long limit,
         consume_cb cb, void *pd) ;
void masterSetupPartialSynchronization(client *c, long long offset,
        long long limit, char *buf, int buflen);

/* client */
void gtidMockClientInit(client* c);
void gtidMockClientDeinit(client* c);
void gtidMockClientCleanArgv(client* c);
void gtidMockClientMoveClientArgv(client *c);
void gtidAfterErrorReply(client *c, const char *s, size_t len);
int gtidIsInMulti();

/* error */
void ctrip_afterErrorReply(client *c, const char *s, size_t len, int flags);

/* aof */
void ctrip_feedAppendOnlyFile(struct redisCommand *cmd, int dictid,
        robj **argv, int argc);

void ctrip_replicationFeedSlavesFromMasterStream(list *slaves, 
    char *buf, size_t buflen, const char *uuid, 
    size_t uuid_len, gno_t gno, long long offset);
int ctrip_masterTryPartialResynchronization(client *c, long long psync_offset);
void ctrip_resizeReplicationBacklog(long long newsize);

void feedAppendOnlyFileGtid(struct redisCommand* _cmd,int dictid, robj **argv, int argc);
sds sendXsyncCommand(connection *conn);
#endif