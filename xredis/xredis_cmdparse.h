#ifndef XREDIS_CMDPARSE_H
#define XREDIS_CMDPARSE_H

/* xredis_cmdparse.h - Common command key parse interface
 * Independent module from xredis-gtid, shared by gtid and swap
 *
 * Note: include sds.h before this header to ensure sds type is defined.
 * In Redis code, typically via server.h -> rio.h -> sds.h chain.
 */

typedef struct redisObject robj;

/* ================================================================
 * Callback mode: notify caller of key position description
 * ================================================================ */

/* Callback type: notify caller of key position description
 * @param ctx           Caller-provided context pointer
 * @param key_type      Key type (OBJ_STRING/OBJ_HASH/OBJ_SET/OBJ_ZSET/OBJ_LIST/OBJ_UNKNOWN)
 * @param key_arg_idx   Key index in argv
 * @param subkeys_count Subkey count
 * @param subkeys_start First subKey index in argv (valid when subkey_arg_idxs == NULL)
 * @param subkeys_step  Subkey stride (valid when subkey_arg_idxs == NULL)
 * @param subkey_arg_idxs SubKey index array in argv; if not NULL, use array directly,
 *                        if NULL, calculate via subkeys_start + i * subkeys_step
 */
typedef void (*cmdParseOnKeyFn)(void *ctx, int key_type, int key_arg_idx,
                                int subkeys_count, int subkeys_start,
                                int subkeys_step, const int *subkey_arg_idxs);

struct redisCommand;
typedef void (*cmdParseOnKeyFn)(void *ctx, int dbid, struct redisCommand* cmd, robj** argv, int argc, int key_arg_idx,
                                int subkeys_count, int subkeys_start,
                                int subkeys_step, const int *subkey_arg_idxs,
                                const cmdParseKeyExtra *extra);

/* Command definition entry (for xredis_commands.def) */
typedef struct cmdParseCommandDef {
    const char *name;
    void (*parse)(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key);
} cmdParseCommandDef;

/* Count keys in command (for preallocation) */
int cmdParseCountKeys(robj **argv, int argc);

/* Parse command, notify each key position via callback
 * @param dbid   Database id
 * @param argv   Command argument array
 * @param argc   Argument count
 * @param ctx    Caller context, passed through to on_key callback
 * @param on_key Callback function, called once per parsed key
 */
void cmdParseKeys(int dbid, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key);

/* Bind cmdparse functions to all commands in server.commands (call once at server startup) */
void cmdParseBindToCommands(void);

/* Lookup count function by command name (from server.commands) */
int (*cmdParseGetCountFunc(const char *cmd_name))(robj **argv, int argc);

/* Lookup parse function by command name (from server.commands) */
void (*cmdParseGetParseFunc(const char *cmd_name))(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key);

#endif
