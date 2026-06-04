#include "xredis_cmdparse.h"
#include "server.h"





/* --- hset / hmset：one key + subkeys（field）, step 2, from argv[2] start  --- */
static void cmdParseHset(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    int subkeys_count = (argc - 2) / 2;
    on_key(ctx, dbid, cmd, argv, argc, 1, subkeys_count, 2, 2, NULL, NULL);
}

/* ---one key + member subkeys, step 1, from argv[2] start --- */
static void cmdParseSingleKeySubkeys(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid);
    int subkeys_count = argc - 2;
    on_key(ctx, dbid, cmd, argv, argc, 1, subkeys_count, 2, 1, NULL, NULL);
}

/* --- hsetnx / hincrby / hincrbyfloat：one key + one subkey --- */
static void cmdParseOneSubkey(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid);
    serverAssert(argc >= 3);
    on_key(ctx, dbid, cmd, argv, argc, 1, 1, 2, 1, NULL, NULL);
}



/* --- zadd：one key + member subkeys, step 2 --- */
static void cmdParseZadd(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid);
    int i = 2;
    while (i < argc) {
        sds arg = (sds)argv[i]->ptr;
        if (!strcasecmp(arg, "nx") || !strcasecmp(arg, "xx") ||
            !strcasecmp(arg, "ch") || !strcasecmp(arg, "incr") ||
            !strcasecmp(arg, "gt") || !strcasecmp(arg, "lt")) {
            i++;
        } else {
            break;
        }
    }
    int subkeys_count = (argc - i) / 2;
    on_key(ctx, dbid, cmd, argv, argc, 1, subkeys_count, i + 1, 2, NULL, NULL);
}


/* --- zincrby：one key + one subkey --- */
static void cmdParseZincrby(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid);
    serverAssert(argc >= 4);
    on_key(ctx, dbid, cmd, argv, argc, 1, 1, 3, 1, NULL, NULL);
}


/* 3 step */
static void cmdParseGeoAdd(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    int i = 2;
    while (i < argc) {
        sds arg = (sds)argv[i]->ptr;
        if (!strcasecmp(arg, "nx") || !strcasecmp(arg, "xx") || !strcasecmp(arg, "ch")) {
            i++;
        } else {
            break;
        }
    }
    int subkeys_count = (argc - i) / 3;  /* lon/lat/member  */
    on_key(ctx, dbid, cmd, argv, argc, 1, subkeys_count, i + 2, 3, NULL, NULL); /* member starts at argv[i+2] */
}

/* geodist：key + member1 + member2（ 2  subkey） */
static void cmdParseGeoDist(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    serverAssert(argc >= 4);
    on_key(ctx, dbid, cmd, argv, argc, 1, 2, 2, 1, NULL, NULL);
}




#define cmdParseZrevrange cmdParseZrange

#include "xredis_commands.def"


void cmdParseKeys(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    if (argc < 1) return;
    if (cmd == NULL) {
        cmd = lookupCommand(argv[0]->ptr);
    }
    serverAssert(cmd != NULL);

    if (cmd->cmdparse_parse != NULL) {
        cmd->cmdparse_parse(dbid, cmd, argv, argc, ctx, on_key);
        return;
    }
    
    getKeysResult keys = GETKEYS_RESULT_INIT;
    int numkeys = getKeysFromCommand(cmd, argv, argc, &keys);
    for (int i = 0; i < numkeys; i++) {
        on_key(ctx, dbid, cmd, argv, argc, keys.keys[i], 0, 0, 0, NULL, NULL);
    }
    getKeysFreeResult(&keys);
    return;
}

void cmdParseBindToCommands(void) {
    int i;
    for (i = 0; cmd_parse_commands[i].name != NULL; i++) {
        sds name = sdsnew(cmd_parse_commands[i].name);
        struct redisCommand *cmd = lookupCommand(name);
        sdsfree(name);
        if (cmd != NULL) {
            cmd->cmdparse_parse = cmd_parse_commands[i].parse;
        }
    }
}


void (*cmdParseGetParseFunc(const char *cmd_name))(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    if (cmd_name == NULL) return NULL;
    struct redisCommand *cmd = lookupCommandByCString(cmd_name);
    if (cmd != NULL) {
        return cmd->cmdparse_parse;
    }
    return NULL;
}
