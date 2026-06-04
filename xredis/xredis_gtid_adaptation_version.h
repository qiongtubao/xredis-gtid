



#ifndef GTID_ADAPTATION_VERSION_H
#define GTID_ADAPTATION_VERSION_H
#include "server.h"

/* dict */
dict* gtidDictCreate(dictType *type);

/* command */
struct redisCommand* gtidLookupCommandBySds(sds name);

/* obj  */
char* gtidGetObjectTypeName(int key_type);


/* getKeysResult*/
int gtidGetKeysResultKeyIndex(getKeysResult* result, int index);

/* backlog */
size_t gtidBacklogAppendToSds(long long offset, sds *dst, size_t size);

/* client */
void gtidMockClientInit(client* c);
void gtidMockClientDeinit(client* c);
void gtidMockClientCleanArgv(client* c);
void gtidMockClientMoveClientArgv(client *c);
void gtidAfterErrorReply(client *c, const char *s, size_t len);

/* error */
void ctrip_afterErrorReply(client *c, const char *s, size_t len, int flags);

#endif