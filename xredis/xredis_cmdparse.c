#include "xredis_cmdparse.h"
#include "server.h"

/* ================================================================
 * 各命令的 count / parse 实现
 * ================================================================ */

/* 从 redisCommand flags 推断 key 类型（公共函数） */
int cmdParseKeyTypeFromCommand(struct redisCommand *cmd) {
    if (cmd == NULL) return OBJ_UNKNOWN;
    if (cmd->flags & CMD_CATEGORY_STRING) return OBJ_STRING;
    if (cmd->flags & CMD_CATEGORY_LIST) return OBJ_LIST;
    if (cmd->flags & CMD_CATEGORY_HASH) return OBJ_HASH;
    if (cmd->flags & CMD_CATEGORY_SET) return OBJ_SET;
    if (cmd->flags & CMD_CATEGORY_SORTEDSET) return OBJ_ZSET;
    if (cmd->flags & CMD_CATEGORY_BITMAP) return OBJ_STRING;
    return OBJ_UNKNOWN;
}
static void cmdParseSingleKey(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    on_key(ctx, dbid, cmd, argv, argc, 1, 0, 0, 0, NULL, NULL);
}


/* --- hset / hmset：单个 key + subkeys（field）, 步长为2, 从argv[2]开始 --- */
static void cmdParseHset(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    int subkeys_count = (argc - 2) / 2;
    on_key(ctx, dbid, cmd, argv, argc, 1, subkeys_count, 2, 2, NULL, NULL);
}

/* --- 单个 key + 多个 subkeys, 步长为1, 从argv[2]开始 --- */
static void cmdParseSingleKeySubkeys(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid);
    int subkeys_count = argc - 2;
    on_key(ctx, dbid, cmd, argv, argc, 1, subkeys_count, 2, 1, NULL, NULL);
}

/* --- hsetnx / hincrby / hincrbyfloat：单个 key + 1 个 subkey --- */
static void cmdParseOneSubkey(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid);
    serverAssert(argc >= 3);
    on_key(ctx, dbid, cmd, argv, argc, 1, 1, 2, 1, NULL, NULL);
}


/* --- smove：2 个 key，各带 1 个 subkey（member=argv[3]）--- */

static void cmdParseSmove(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid); UNUSED(argv);
    serverAssert(argc == 4);
    on_key(ctx, dbid, cmd, argv, argc, 1, 1, 3, 1, NULL, NULL);
    on_key(ctx, dbid, cmd, argv, argc, 2, 1, 3, 1, NULL, NULL);
    
}

/* --- zadd：单个 key + member subkeys, 步长为2 --- */
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


/* --- zincrby：单个 key + 1 个 subkey --- */
static void cmdParseZincrby(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid);
    serverAssert(argc >= 4);
    on_key(ctx, dbid, cmd, argv, argc, 1, 1, 3, 1, NULL, NULL);
}

/* --- 2 个 key（argv[1]+argv[2]，共用：rename/renamenx/copy/lmove/rpoplpush/geosearchstore/zrangestore）--- */
static void cmdParseTwoKeys(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid);
    serverAssert(argc >= 3);
    on_key(ctx, dbid, cmd, argv, argc, 1, 0, 0, 0, NULL, NULL);
    on_key(ctx, dbid, cmd, argv, argc, 2, 0, 0, 0, NULL, NULL);
}

/* ================================================================
 * 范围/偏移类命令的 parse 函数（需要传递 extra 信息）
 * ================================================================ */

/* --- list 范围命令 --- */

/* lpop：单个 key + 可选 count，设置 range start=0, end=count */
static void cmdParseLpop(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid); UNUSED(cmd);
    long long count = 1;
    if (argc >= 3) getLongLongFromObject(argv[2], &count);
    cmdParseKeyExtra extra = {.extra_type = CMDPARSE_EXTRA_RANGE,
                              .range = {.start = 0, .end = count, .reverse = 0}};
    on_key(ctx, dbid, cmd, argv, argc, 1, 0, 0, 0, NULL, &extra);
}

/* rpop：单个 key + 可选 count，设置 range start=-count, end=-1 */
static void cmdParseRpop(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid); UNUSED(cmd);
    long long count = 1;
    if (argc >= 3) getLongLongFromObject(argv[2], &count);
    cmdParseKeyExtra extra = {.extra_type = CMDPARSE_EXTRA_RANGE,
                              .range = {.start = -count, .end = -1, .reverse = 0}};
    on_key(ctx, dbid, cmd, argv, argc, 1, 0, 0, 0, NULL, &extra);
}


static void cmdParseBlpop(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid); UNUSED(cmd);
    cmdParseKeyExtra extra = {.extra_type = CMDPARSE_EXTRA_RANGE,
                              .range = {.start = 0, .end = 0, .reverse = 0}};
    for (int i = 1; i < argc - 1; i++) {
        on_key(ctx, dbid, cmd, argv, argc, i, 0, 0, 0, NULL, &extra);
    }
}

/* brpop：多个 key + timeout，每个 key 设置 range -1,-1 */
static void cmdParseBrpop(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid); UNUSED(cmd);
    cmdParseKeyExtra extra = {.extra_type = CMDPARSE_EXTRA_RANGE,
                              .range = {.start = -1, .end = -1, .reverse = 0}};
    for (int i = 1; i < argc - 1; i++) {
        on_key(ctx, dbid, cmd, argv, argc, i, 0, 0, 0, NULL, &extra);
    }
}

/* lrange：单个 key + start + end, arg_rewrite0=2(argv[2]=start), arg_rewrite1=3(argv[3]=end) */
static void cmdParseLrange(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid); UNUSED(cmd);
    if (argc < 4) return;
    long long start, end;
    if (getLongLongFromObject(argv[2], &start) != C_OK) return;
    if (getLongLongFromObject(argv[3], &end) != C_OK) return;
    cmdParseKeyExtra extra = {.extra_type = CMDPARSE_EXTRA_RANGE,
                              .range = {.start = start, .end = end, .reverse = 0,
                                        .arg_rewrite0 = 2, .arg_rewrite1 = 3}};
    on_key(ctx, dbid, cmd, argv, argc, 1, 0, 0, 0, NULL, &extra);
}

/* ltrim：单个 key + start + end, reverse=1（保留范围外的元素）, arg_rewrite0=2(argv[2]=start), arg_rewrite1=3(argv[3]=stop) */
static void cmdParseLtrim(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid); UNUSED(cmd);
    if (argc < 4) return;
    long long start, end;
    if (getLongLongFromObject(argv[2], &start) != C_OK) return;
    if (getLongLongFromObject(argv[3], &end) != C_OK) return;
    cmdParseKeyExtra extra = {.extra_type = CMDPARSE_EXTRA_RANGE,
                              .range = {.start = start, .end = end, .reverse = 1,
                                        .arg_rewrite0 = 2, .arg_rewrite1 = 3}};
    on_key(ctx, dbid, cmd, argv, argc, 1, 0, 0, 0, NULL, &extra);
}

/* lindex：单个 key + index, range start=index, end=index, arg_rewrite0=2(argv[2]=index) */
static void cmdParseLindex(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid); UNUSED(cmd);
    if (argc < 3) return;
    long long index;
    if (getLongLongFromObject(argv[2], &index) != C_OK) return;
    cmdParseKeyExtra extra = {.extra_type = CMDPARSE_EXTRA_RANGE,
                              .range = {.start = index, .end = index, .reverse = 0,
                                        .arg_rewrite0 = 2, .arg_rewrite1 = -1}};
    on_key(ctx, dbid, cmd, argv, argc, 1, 0, 0, 0, NULL, &extra);
}

/* --- zset 范围命令（分数） --- */

// /* zrangebyscore：key + min + max, min/max 可能是 (+inf 或 -inf 或 (exclusive */
static void cmdParseZrangebyscore(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid); UNUSED(cmd);
    if (argc < 4) return;
    cmdParseKeyExtra extra = {.extra_type = CMDPARSE_EXTRA_ZSCORE, .zscore = {.reverse = 0}};
    char *min_str = (char *)argv[2]->ptr;
    char *max_str = (char *)argv[3]->ptr;
    extra.zscore.minex = (min_str[0] == '(');
    extra.zscore.maxex = (max_str[0] == '(');
    extra.zscore.min = strtod(min_str + (extra.zscore.minex ? 1 : 0), NULL);
    extra.zscore.max = strtod(max_str + (extra.zscore.maxex ? 1 : 0), NULL);
    on_key(ctx, dbid, cmd, argv, argc, 1, 0, 0, 0, NULL, &extra);
}

/* zrevrangebyscore：同 zrangebyscore 但 reverse=1 */
static void cmdParseZrevrangebyscore(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid); UNUSED(cmd);
    if (argc < 4) return;
    cmdParseKeyExtra extra = {.extra_type = CMDPARSE_EXTRA_ZSCORE, .zscore = {.reverse = 1}};
    char *min_str = (char *)argv[2]->ptr;
    char *max_str = (char *)argv[3]->ptr;
    extra.zscore.minex = (min_str[0] == '(');
    extra.zscore.maxex = (max_str[0] == '(');
    extra.zscore.min = strtod(min_str + (extra.zscore.minex ? 1 : 0), NULL);
    extra.zscore.max = strtod(max_str + (extra.zscore.maxex ? 1 : 0), NULL);
    on_key(ctx, dbid, cmd, argv, argc, 1, 0, 0, 0, NULL, &extra);
}

// /* zremrangebyscore：同 zrangebyscore */
// #define cmdParseZremrangebyscore cmdParseZrangebyscore

// /* --- zset 范围命令（字典序）--- */

/* zrangebylex：key + min + max, extra 携带 arg 索引让 bridge 解析 */
static void cmdParseZrangebylex(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid); UNUSED(cmd);
    if (argc < 4) return;
    cmdParseKeyExtra extra = {.extra_type = CMDPARSE_EXTRA_ZLEX};
    char *min_str = (char *)argv[2]->ptr;
    char *max_str = (char *)argv[3]->ptr;
    extra.zlex.min_arg_idx = 2;
    extra.zlex.max_arg_idx = 3;
    extra.zlex.minex = (min_str[0] == '(');
    extra.zlex.maxex = (max_str[0] == '(');
    on_key(ctx, dbid, cmd, argv, argc, 1, 0, 0, 0, NULL, &extra);
}

/* zrevrangebylex：同 zrangebylex */
#define cmdParseZrevrangebylex cmdParseZrangebylex

// /* zlexcount：同 zrangebylex */
#define cmdParseZlexcount cmdParseZrangebylex

// /* zremrangebylex：同 zrangebylex */
#define cmdParseZremrangebylex cmdParseZrangebylex

// /* --- zset 排名范围 --- */

// /* zremrangebyrank：key + start + end */
// static void cmdParseZremrangebyrank(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
//     UNUSED(dbid); UNUSED(cmd);
//     if (argc < 4) return;
//     long long start, end;
//     if (getLongLongFromObject(argv[2], &start) != C_OK) return;
//     if (getLongLongFromObject(argv[3], &end) != C_OK) return;
//     cmdParseKeyExtra extra = {.extra_type = CMDPARSE_EXTRA_ZRANK,
//                               .zrank = {.start = start, .end = end}};
//     on_key(ctx, dbid, cmd, argv, argc, 1, 0, 0, 0, NULL, &extra);
// }

/* --- bitmap 命令 --- */

/* setbit：key + offset */
// static void cmdParseSetbit(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
//     UNUSED(dbid); UNUSED(cmd);
//     if (argc < 4) return;
//     long long offset;
//     getLongLongFromObject(argv[2], &offset);
//     cmdParseKeyExtra extra = {.extra_type = CMDPARSE_EXTRA_BITOFF,
//                               .bitoff = {.offset = offset}};
//     on_key(ctx, dbid, cmd, argv, argc, 1, 0, 0, 0, NULL, &extra);
// }

// /* getbit：key + offset */
// static void cmdParseGetbit(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
//     UNUSED(dbid); UNUSED(cmd);
//     serverAssert(argc == 3);
//     long long offset;
//     getLongLongFromObject(argv[2], &offset);
//     cmdParseKeyExtra extra = {.extra_type = CMDPARSE_EXTRA_BITOFF,
//                               .bitoff = {.offset = offset}};
//     on_key(ctx, dbid, cmd, argv, argc, 1, 0, 0, 0, NULL, &extra);
// }


/* bitpos：key bit [start [end]] */
// static void cmdParseBitpos(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
//     UNUSED(dbid); UNUSED(cmd);
//     if (argc <= 3) {
//         on_key(ctx, dbid, cmd, argv, argc, 1, 0, 0, 0, NULL, NULL);
//         return;
//     }
//     long long start, end = LLONG_MAX;
//     if (getLongLongFromObject(argv[3], &start) != C_OK) {
//         on_key(ctx, dbid, cmd, argv, argc, 1, 0, 0, 0, NULL, NULL);
//         return;
//     }
//     if (argc >= 5) getLongLongFromObject(argv[4], &end);
//     cmdParseKeyExtra extra = {.extra_type = CMDPARSE_EXTRA_BITRNG,
//                               .bitrng = {.start = start, .end = end}};
//     on_key(ctx, dbid, cmd, argv, argc, 1, 0, 0, 0, NULL, &extra);
// }

/* --- 只读子键 --- */

/* zscore：key + member（1 个 subkey） */
static void cmdParseZscore(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid); UNUSED(cmd);
    if (argc >= 3) {
        on_key(ctx, dbid, cmd, argv, argc, 1, 1, 2, 1, NULL, NULL);
    }
}


/* --- geo 命令 --- */

/* geoadd：类似 zadd，但 member 步长为 3（lon/lat/member），带可选 NX/XX/CH */
static void cmdParseGeoAdd(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid); UNUSED(cmd);
    int i = 2;
    while (i < argc) {
        sds arg = (sds)argv[i]->ptr;
        if (!strcasecmp(arg, "nx") || !strcasecmp(arg, "xx") || !strcasecmp(arg, "ch")) {
            i++;
        } else {
            break;
        }
    }
    int subkeys_count = (argc - i) / 3;  /* lon/lat/member 三元组 */
    on_key(ctx, dbid, cmd, argv, argc, 1, subkeys_count, i + 2, 3, NULL, NULL); /* member 从 i+2 开始 */
}

/* geodist：key + member1 + member2（2 个 subkey） */
static void cmdParseGeoDist(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid); UNUSED(cmd);
    if (argc >= 4) {
        on_key(ctx, dbid, cmd, argv, argc, 1, 2, 2, 1, NULL, NULL);
    }
}

/* bitcount：key + [start end] */
// static void cmdParseBitcount(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
//     UNUSED(dbid); UNUSED(cmd);
//     if (argc < 4) {
//         on_key(ctx, dbid, cmd, argv, argc, 1, 0, 0, 0, NULL, NULL);
//         return;
//     }
//     long long start, end;
//     if (getLongLongFromObject(argv[2], &start) != C_OK) return;
//     if (getLongLongFromObject(argv[3], &end) != C_OK) return;
//     cmdParseKeyExtra extra = {.extra_type = CMDPARSE_EXTRA_BITRNG,
//                               .bitrng = {.start = start, .end = end}};
//     on_key(ctx, dbid, cmd, argv, argc, 1, 0, 0, 0, NULL, &extra);
// }

// static void cmdParseBitOp(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
//     UNUSED(dbid); UNUSED(cmd); UNUSED(argv);
//     for (int i = 2; i < argc; i++) {
//         on_key(ctx, dbid, cmd, argv, argc, i, 0, 0, 0, NULL, NULL);
//     }
// }

/* --- blmove：2 个 list key --- */
// static void cmdParseBlmove(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
//     UNUSED(dbid); UNUSED(cmd); UNUSED(argv);
//     serverAssert(argc >= 3);
//     on_key(ctx, dbid, cmd, argv, argc, 1, 0, 0, 0, NULL, NULL);
//     on_key(ctx, dbid, cmd, argv, argc, 2, 0, 0, 0, NULL, NULL);
// }

// /* --- brpoplpush：2 个 list key --- */
// static void cmdParseBrpoplpush(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
//     UNUSED(dbid); UNUSED(cmd); UNUSED(argv);
//     serverAssert(argc >= 3);
//     on_key(ctx, dbid, cmd, argv, argc, 1, 0, 0, 0, NULL, NULL);
//     on_key(ctx, dbid, cmd, argv, argc, 2, 0, 0, 0, NULL, NULL);
// }


/* zrevrange 与 zrange 行为相同 */
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
