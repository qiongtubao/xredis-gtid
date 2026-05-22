#include "server.h"


/* version 6.x*/

ssize_t backlogAppendToSds(long long offset, sds *dst, size_t size) {
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
    sdsIncrLen(*dst, (int)total);
    return (ssize_t)total;
}

static void addKeyInfo(gtidGapLogKeysInfos *kis, int dbid, int type, sds key,
                       sds *subkeys, int subkeys_count)
{
    gtidGapLogKeyInfo *ki = createGtidGapLogKeyInfo(dbid, type, key, subkeys, subkeys_count);
    kis->keys[kis->size++] = ki;
}

void addKeyInfoToKeysInfos(gtidGapLogKeysInfos *kis, int dbid, robj **args, int argc) {
    if (argc < 2 || kis == NULL) return;

    sds cmd = (sds)args[0]->ptr;

    if (!strcasecmp(cmd, "del") ||
        !strcasecmp(cmd, "unlink")) {
        for (int i = 1; i < argc; i++) {
            addKeyInfo(kis, dbid, OBJ_UNKNOWN, sdsdup((sds)args[i]->ptr), NULL, 0);
        }
        return;
    }

    if (!strcasecmp(cmd, "set") || !strcasecmp(cmd, "setnx") ||
        !strcasecmp(cmd, "setex") || !strcasecmp(cmd, "psetex") ||
        !strcasecmp(cmd, "incr") || !strcasecmp(cmd, "decr") ||
        !strcasecmp(cmd, "incrby") || !strcasecmp(cmd, "decrby") ||
        !strcasecmp(cmd, "incrbyfloat") || !strcasecmp(cmd, "decrbyfloat") ||
        !strcasecmp(cmd, "append") || !strcasecmp(cmd, "getset") ||
        !strcasecmp(cmd, "setrange") || !strcasecmp(cmd, "setbit") ||
        !strcasecmp(cmd, "bitfield")) {
        addKeyInfo(kis, dbid, OBJ_STRING, sdsdup((sds)args[1]->ptr), NULL, 0);
        return;
    }

    if (!strcasecmp(cmd, "mset") || !strcasecmp(cmd, "msetnx")) {
        for (int i = 1; i < argc; i += 2) {
            addKeyInfo(kis, dbid, OBJ_STRING, sdsdup((sds)args[i]->ptr), NULL, 0);
        }
        return;
    }

    if (!strcasecmp(cmd, "hset") || !strcasecmp(cmd, "hmset")) {
        int subkeys_count = (argc - 2) / 2;
        sds *subkeys = NULL;
        if (subkeys_count > 0) {
            subkeys = zmalloc(sizeof(sds) * subkeys_count);
            for (int i = 0; i < subkeys_count; i++) {
                subkeys[i] = sdsdup((sds)args[2 + i * 2]->ptr);
            }
        }
        addKeyInfo(kis, dbid, OBJ_HASH, sdsdup((sds)args[1]->ptr), subkeys, subkeys_count);
        return;
    }

    if (!strcasecmp(cmd, "hdel")) {
        int subkeys_count = argc - 2;
        sds *subkeys = NULL;
        if (subkeys_count > 0) {
            subkeys = zmalloc(sizeof(sds) * subkeys_count);
            for (int i = 0; i < subkeys_count; i++) {
                subkeys[i] = sdsdup((sds)args[2 + i]->ptr);
            }
        }
        addKeyInfo(kis, dbid, OBJ_HASH, sdsdup((sds)args[1]->ptr), subkeys, subkeys_count);
        return;
    }

    if (!strcasecmp(cmd, "hsetnx") ||
        !strcasecmp(cmd, "hincrby") || !strcasecmp(cmd, "hincrbyfloat")) {
        sds *subkeys = NULL;
        int subkeys_count = 0;
        if (argc >= 3) {
            subkeys_count = 1;
            subkeys = zmalloc(sizeof(sds));
            subkeys[0] = sdsdup((sds)args[2]->ptr);
        }
        addKeyInfo(kis, dbid, OBJ_HASH, sdsdup((sds)args[1]->ptr), subkeys, subkeys_count);
        return;
    }

    if (!strcasecmp(cmd, "sadd") || !strcasecmp(cmd, "srem") || !strcasecmp(cmd, "spop")) {
        int subkeys_count = argc - 2;
        sds *subkeys = NULL;
        if (subkeys_count > 0) {
            subkeys = zmalloc(sizeof(sds) * subkeys_count);
            for (int i = 0; i < subkeys_count; i++) {
                subkeys[i] = sdsdup((sds)args[2 + i]->ptr);
            }
        }
        addKeyInfo(kis, dbid, OBJ_SET, sdsdup((sds)args[1]->ptr), subkeys, subkeys_count);
        return;
    }

    if (!strcasecmp(cmd, "smove")) {
        /* smove source destination member: both source and dest are modified */
        if (argc >= 3) {
            addKeyInfo(kis, dbid, OBJ_SET, sdsdup((sds)args[1]->ptr), NULL, 0);
            addKeyInfo(kis, dbid, OBJ_SET, sdsdup((sds)args[2]->ptr), NULL, 0);
        } else if (argc >= 2) {
            addKeyInfo(kis, dbid, OBJ_SET, sdsdup((sds)args[1]->ptr), NULL, 0);
        }
        return;
    }

    if (!strcasecmp(cmd, "zadd")) {
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
        int subkeys_count = (argc - i) / 2;
        sds *subkeys = NULL;
        if (subkeys_count > 0) {
            subkeys = zmalloc(sizeof(sds) * subkeys_count);
            for (int j = 0; j < subkeys_count; j++) {
                subkeys[j] = sdsdup((sds)args[i + 1 + j * 2]->ptr);
            }
        }
        addKeyInfo(kis, dbid, OBJ_ZSET, sdsdup((sds)args[1]->ptr), subkeys, subkeys_count);
        return;
    }

    if (!strcasecmp(cmd, "zrem")) {
        int subkeys_count = argc - 2;
        sds *subkeys = NULL;
        if (subkeys_count > 0) {
            subkeys = zmalloc(sizeof(sds) * subkeys_count);
            for (int i = 0; i < subkeys_count; i++) {
                subkeys[i] = sdsdup((sds)args[2 + i]->ptr);
            }
        }
        addKeyInfo(kis, dbid, OBJ_ZSET, sdsdup((sds)args[1]->ptr), subkeys, subkeys_count);
        return;
    }

    if (!strcasecmp(cmd, "zincrby")) {
        sds *subkeys = NULL;
        int subkeys_count = 0;
        if (argc >= 4) {
            subkeys_count = 1;
            subkeys = zmalloc(sizeof(sds));
            subkeys[0] = sdsdup((sds)args[3]->ptr);
        }
        addKeyInfo(kis, dbid, OBJ_ZSET, sdsdup((sds)args[1]->ptr), subkeys, subkeys_count);
        return;
    }

    if (!strcasecmp(cmd, "zremrangebyrank") ||
        !strcasecmp(cmd, "zremrangebyscore") ||
        !strcasecmp(cmd, "zremrangebylex")) {
        addKeyInfo(kis, dbid, OBJ_ZSET, sdsdup((sds)args[1]->ptr), NULL, 0);
        return;
    }

    if (!strcasecmp(cmd, "lpush") || !strcasecmp(cmd, "rpush") ||
        !strcasecmp(cmd, "lpushx") || !strcasecmp(cmd, "rpushx") ||
        !strcasecmp(cmd, "lpop") || !strcasecmp(cmd, "rpop") ||
        !strcasecmp(cmd, "lrem") || !strcasecmp(cmd, "linsert") ||
        !strcasecmp(cmd, "lset") || !strcasecmp(cmd, "ltrim")) {
        addKeyInfo(kis, dbid, OBJ_LIST, sdsdup((sds)args[1]->ptr), NULL, 0);
        return;
    }

    if (!strcasecmp(cmd, "rename") || !strcasecmp(cmd, "renamenx")) {
        if (argc >= 3) {
            addKeyInfo(kis, dbid, OBJ_UNKNOWN, sdsdup((sds)args[1]->ptr), NULL, 0);
            addKeyInfo(kis, dbid, OBJ_UNKNOWN, sdsdup((sds)args[2]->ptr), NULL, 0);
        } else if (argc >= 2) {
            addKeyInfo(kis, dbid, OBJ_UNKNOWN, sdsdup((sds)args[1]->ptr), NULL, 0);
        }
        return;
    }

    if (!strcasecmp(cmd, "move") ||
        !strcasecmp(cmd, "persist") ||
        !strcasecmp(cmd, "expire") || !strcasecmp(cmd, "pexpire") ||
        !strcasecmp(cmd, "expireat") || !strcasecmp(cmd, "pexpireat")) {
        addKeyInfo(kis, dbid, OBJ_UNKNOWN, sdsdup((sds)args[1]->ptr), NULL, 0);
        return;
    }

    /* unknown command, fallback: use first arg as key to avoid crash */
    if (argc >= 2) {
        serverLog(LL_WARNING, "Unknown command '%s' for key propagation", cmd);
        addKeyInfo(kis, dbid, OBJ_UNKNOWN, sdsdup((sds)args[1]->ptr), NULL, 0);
    }
}