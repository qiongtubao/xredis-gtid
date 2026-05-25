#ifndef XREDIS_CMDPARSE_H
#define XREDIS_CMDPARSE_H

/* xredis_cmdparse.h - 命令 key 解析公共接口
 * 从 xredis-gtid 中分离出的独立模块，供 gtid 和 swap 共用
 *
 * 注意：包含本头文件前，调用方须先包含 sds.h 以确保 sds 类型已定义。
 * 在 Redis 代码中通常通过 server.h -> rio.h -> sds.h 链完成。
 */

typedef struct redisObject robj;

/* ================================================================
 * 回调模式：通知调用方一个 key 的位置描述
 * ================================================================ */

/* 回调类型：通知调用方一个 key 的位置描述
 * @param ctx           调用方传入的上下文指针
 * @param key_type      key 的类型（OBJ_STRING/OBJ_HASH/OBJ_SET/OBJ_ZSET/OBJ_LIST/OBJ_UNKNOWN）
 * @param key_arg_idx   key 在 argv 中的索引位置
 * @param subkeys_count subkey 的数量
 * @param subkeys_start 第一个 subkey 在 argv 中的索引位置（当 subkey_arg_idxs == NULL 时有效）
 * @param subkeys_step  subkey 之间的步长（当 subkey_arg_idxs == NULL 时有效）
 * @param subkey_arg_idxs subkey 在 argv 中的索引位置数组；若不为 NULL 则直接按数组取值，
 *                        若为 NULL 则通过 subkeys_start + i * subkeys_step 计算
 */
typedef void (*cmdParseOnKeyFn)(void *ctx, int key_type, int key_arg_idx,
                                int subkeys_count, int subkeys_start,
                                int subkeys_step, const int *subkey_arg_idxs);

struct redisCommand;

/* 命令定义条目（供 xredis_commands.def 使用） */
typedef struct cmdParseCommandDef {
    const char *name;
    int (*count)(robj **argv, int argc);
    void (*parse)(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key);
} cmdParseCommandDef;

/* 计算命令中包含的 key 条目数（用于预分配） */
int cmdParseCountKeys(robj **argv, int argc);

/* 解析命令，通过回调通知每个 key 的位置描述
 * @param dbid   数据库 id
 * @param argv   命令参数数组
 * @param argc   参数个数
 * @param ctx    调用方上下文，会透传给 on_key 回调
 * @param on_key 回调函数，每解析出一个 key 调用一次
 */
void cmdParseKeys(int dbid, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key);

/* 绑定 cmdparse 函数到 server.commands 中的所有命令（应在 server 启动时调用一次） */
void cmdParseBindToCommands(void);

/* 通过命令名查找 count 函数（从 server.commands 取） */
int (*cmdParseGetCountFunc(const char *cmd_name))(robj **argv, int argc);

/* 通过命令名查找 parse 函数（从 server.commands 取） */
void (*cmdParseGetParseFunc(const char *cmd_name))(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key);

#endif
