#include "xredis_cmdparse.h"
#include "server.h"

/* ================================================================
 * Helper: sds to uppercase (server.commands keys are uppercase)
 * ================================================================ */
static sds sdsdupupper(sds s) {
    sds result = sdsdup(s);
    for (size_t i = 0; i < sdslen(result); i++) {
        result[i] = toupper((unsigned char)result[i]);
    }
    return result;
}

/* ================================================================
 * Per-command count/parse implementation
 * ================================================================ */

/* --- del / unlink: multiple keys --- */
static int cmdCountDel(robj **argv, int argc) {
    UNUSED(argv);
    return argc > 1 ? argc - 1 : 0;
}
static void cmdParseDel(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid);
    for (int i = 1; i < argc; i++) {
        on_key(ctx, OBJ_UNKNOWN, i, 0, 0, 0, NULL);
    }
}

/* --- Single key, no subkeys (type inferred from command name)--- */
static int cmdCountSingleKey(robj **argv, int argc) {
    UNUSED(argv); UNUSED(argc);
    return 1;
}
static int cmdKeyTypeFromCommand(struct redisCommand *cmd) {
    if (cmd->flags & CMD_CATEGORY_STRING) return OBJ_STRING;
    if (cmd->flags & CMD_CATEGORY_LIST) return OBJ_LIST;
    if (cmd->flags & CMD_CATEGORY_HASH) return OBJ_HASH;
    if (cmd->flags & CMD_CATEGORY_SET) return OBJ_SET;
    if (cmd->flags & CMD_CATEGORY_SORTEDSET) return OBJ_ZSET;
    if (cmd->flags & CMD_CATEGORY_BITMAP) return OBJ_STRING;
    return OBJ_UNKNOWN;
}
static void cmdParseSingleKey(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid); UNUSED(argc);
    on_key(ctx, cmdKeyTypeFromCommand(cmd), 1, 0, 0, 0, NULL);
}

/* --- mset / msetnx: multiple keys, spaced --- */
static int cmdCountMset(robj **argv, int argc) {
    UNUSED(argv);
    return argc > 1 ? (argc - 1) / 2 : 0;
}
static void cmdParseMset(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid); UNUSED(argv);
    for (int i = 1; i < argc; i += 2) {
        on_key(ctx, OBJ_STRING, i, 0, 0, 0, NULL);
    }
}

/* --- hset / hmset: single key + subkeys (field), stride 2, starting at argv[2] --- */
static void cmdParseHset(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid);
    int subkeys_count = (argc - 2) / 2;
    on_key(ctx, OBJ_HASH, 1, subkeys_count, 2, 2, NULL);
}

/* --- hdel: single key + multiple subkeys, stride 1, starting at argv[2] --- */
static void cmdParseHdel(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid);
    int subkeys_count = argc - 2;
    on_key(ctx, OBJ_HASH, 1, subkeys_count, 2, 1, NULL);
}

/* --- hsetnx / hincrby / hincrbyfloat: single key + 1 subkey --- */
static void cmdParseHsetnx(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid);
    if (argc >= 3) {
        on_key(ctx, OBJ_HASH, 1, 1, 2, 1, NULL);
    } else {
        on_key(ctx, OBJ_HASH, 1, 0, 0, 0, NULL);
    }
}

/* --- sadd / srem / spop: single key + multiple subkeys, stride 1, starting at argv[2] --- */
static void cmdParseSadd(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid);
    int subkeys_count = argc - 2;
    on_key(ctx, OBJ_SET, 1, subkeys_count, 2, 1, NULL);
}

/* --- smove: 2 keys, each with 1 subkey (member=argv[3])--- */
static int cmdCountRename(robj **argv, int argc) {
    UNUSED(argv);
    return argc >= 3 ? 2 : (argc >= 2 ? 1 : 0);
}
static void cmdParseSmove(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid); UNUSED(argv);
    if (argc >= 4) {
        on_key(ctx, OBJ_SET, 1, 1, 3, 1, NULL);
        on_key(ctx, OBJ_SET, 2, 1, 3, 1, NULL);
    } else if (argc >= 3) {
        on_key(ctx, OBJ_SET, 1, 0, 0, 0, NULL);
        on_key(ctx, OBJ_SET, 2, 0, 0, 0, NULL);
    } else if (argc >= 2) {
        on_key(ctx, OBJ_SET, 1, 0, 0, 0, NULL);
    }
}

/* --- zadd: single key + member subkeys, stride 2 --- */
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
    on_key(ctx, OBJ_ZSET, 1, subkeys_count, i + 1, 2, NULL);
}

/* --- zrem: single key + multiple subkeys, stride 1, starting at argv[2] --- */
static void cmdParseZrem(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid);
    int subkeys_count = argc - 2;
    on_key(ctx, OBJ_ZSET, 1, subkeys_count, 2, 1, NULL);
}

/* --- zincrby: single key + 1 subkey --- */
static void cmdParseZincrby(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid);
    if (argc >= 4) {
        on_key(ctx, OBJ_ZSET, 1, 1, 3, 1, NULL);
    } else {
        on_key(ctx, OBJ_ZSET, 1, 0, 0, 0, NULL);
    }
}

/* --- rename / renamenx: 2 keys --- */
static void cmdParseRename(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid);
    if (argc >= 3) {
        on_key(ctx, OBJ_UNKNOWN, 1, 0, 0, 0, NULL);
        on_key(ctx, OBJ_UNKNOWN, 2, 0, 0, 0, NULL);
    } else if (argc >= 2) {
        on_key(ctx, OBJ_UNKNOWN, 1, 0, 0, 0, NULL);
    }
}

/* ================================================================
 * Import auto-generated command registry from generate_cmdparse_commands.py
 * ================================================================ */
#include "xredis_commands.def"

/* ================================================================
 * Public interface: lookup via server.commands
 * ================================================================ */

/* Count keys in command (via lookupCommand from server.commands) */
int cmdParseCountKeys(robj **argv, int argc) {
    if (argc < 1) return 0;
    sds cmd_upper = sdsdupupper((sds)argv[0]->ptr);
    struct redisCommand *cmd = lookupCommand(cmd_upper);
    sdsfree(cmd_upper);
    if (cmd != NULL && cmd->cmdparse_count != NULL) {
        return cmd->cmdparse_count(argv, argc);
    }
    /* unknown command fallback */
    return argc >= 2 ? 1 : 0;
}

/* Parse command, notify each key position via callback (lookupCommand from server.commands) */
void cmdParseKeys(int dbid, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    if (argc < 1) return;
    sds cmd_upper = sdsdupupper((sds)argv[0]->ptr);
    struct redisCommand *cmd = lookupCommand(cmd_upper);
    sdsfree(cmd_upper);
    if (cmd != NULL && cmd->cmdparse_parse != NULL) {
        cmd->cmdparse_parse(dbid, cmd, argv, argc, ctx, on_key);
    } else if (argc >= 2) {
        /* unknown command fallback */
        serverLog(LL_WARNING, "Unknown command '%s' for key propagation", (sds)argv[0]->ptr);
        // on_key(ctx, OBJ_UNKNOWN, 1, 0, 0, 0, NULL);
        serverPanic("unknown command fallback");
    }
}

/* Bind cmdparse functions to all commands in server.commands */
void cmdParseBindToCommands(void) {
    int i;
    for (i = 0; cmd_parse_commands[i].name != NULL; i++) {
        sds name = sdsnew(cmd_parse_commands[i].name);
        for (size_t j = 0; j < sdslen(name); j++) {
            name[j] = toupper((unsigned char)name[j]);
        }
        struct redisCommand *cmd = lookupCommand(name);
        sdsfree(name);
        if (cmd != NULL) {
            cmd->cmdparse_count = cmd_parse_commands[i].count;
            cmd->cmdparse_parse = cmd_parse_commands[i].parse;
        }
    }
}

/* Lookup count function by command name (from server.commands) */
int (*cmdParseGetCountFunc(const char *cmd_name))(robj **argv, int argc) {
    if (cmd_name == NULL) return NULL;
    struct redisCommand *cmd = lookupCommandByCString(cmd_name);
    if (cmd != NULL) {
        return cmd->cmdparse_count;
    }
    return NULL;
}

/* Lookup parse function by command name (from server.commands) */
void (*cmdParseGetParseFunc(const char *cmd_name))(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    if (cmd_name == NULL) return NULL;
    struct redisCommand *cmd = lookupCommandByCString(cmd_name);
    if (cmd != NULL) {
        return cmd->cmdparse_parse;
    }
    return NULL;
}
