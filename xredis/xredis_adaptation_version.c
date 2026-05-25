#include "server.h"
#include "xredis_gtid.h"
#include "xredis_cmdparse.h"

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

/* ================================================================
 * gtid 侧回调模式适配
 * ================================================================ */

/* gtidOnKey 回调上下文：需要传递 dbid 和 argv 给回调 */
typedef struct {
    int dbid;
    robj **argv;
    gtidGapLogKeysInfos *kis;
} gtidOnKeyCtx;

static void addKeyInfo(gtidGapLogKeysInfos *kis, int dbid, int type, sds key,
                       sds *subkeys, int subkeys_count)
{
    gtidGapLogKeyInfo *ki = createGtidGapLogKeyInfo(dbid, type, key, subkeys, subkeys_count);
    kis->keys[kis->size++] = ki;
}

/* gtid 回调：在回调中 sdsdup 创建 gtidGapLogKeyInfo */
static void gtidOnKey(void *ctx, int key_type, int key_arg_idx,
                      int subkeys_count, int subkeys_start,
                      int subkeys_step, const int *subkey_arg_idxs)
{
    gtidOnKeyCtx *gctx = ctx;
    sds key = sdsdup((sds)gctx->argv[key_arg_idx]->ptr);
    sds *subkeys = subkeys_count > 0 ? zmalloc(sizeof(sds) * subkeys_count) : NULL;
    for (int i = 0; i < subkeys_count; i++) {
        int subkey_idx = subkey_arg_idxs ? subkey_arg_idxs[i] : (subkeys_start + i * subkeys_step);
        subkeys[i] = sdsdup((sds)gctx->argv[subkey_idx]->ptr);
    }
    addKeyInfo(gctx->kis, gctx->dbid, key_type, key, subkeys, subkeys_count);
}

void addKeyInfoToKeysInfos(gtidGapLogKeysInfos *kis, int dbid, robj **args, int argc) {
    if (argc < 2 || kis == NULL) return;

    gtidOnKeyCtx ctx = { .dbid = dbid, .argv = args, .kis = kis };
    cmdParseKeys(dbid, args, argc, &ctx, gtidOnKey);
}
