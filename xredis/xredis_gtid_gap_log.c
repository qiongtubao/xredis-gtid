
#include "server.h"

static void uuidSetFreeWrapper(void *ptr) {
    uuidSet *us = (uuidSet*)ptr;
    if (us) {
        uuidSetFree(us);
    }
}

void gtidGapLogSkiplistDestructor(void *privdata, void *val) {
    UNUSED(privdata);
    skiplist *sl = (skiplist*)val;
    if (sl) {
        freeSkipList(sl);
    }
}

static dictType gtidGapLogDictType = {
    .hashFunction = dictSdsHash,
    .keyCompare = dictSdsKeyCompare,
    .keyDestructor = dictSdsDestructor,
    .valDestructor = gtidGapLogSkiplistDestructor
};
gtidGapLog* createGtidGapLog() {
    gtidGapLog* gaplog =  zmalloc(sizeof(gtidGapLog));
    gaplog->data = dictCreate(&gtidGapLogDictType, NULL);
    gaplog->size = 0;
    gaplog->history = listCreate();
    listSetFreeMethod(gaplog->history, uuidSetFreeWrapper);
    return gaplog;
}

void resetGtidGapLog(gtidGapLog* gaplog) {
    dictEmpty(gaplog->data, NULL);
    gaplog->size = 0;
    listEmpty(gaplog->history);
}
void freeGtidGapLog(gtidGapLog* gaplog) {
    dictRelease(gaplog->data);
    listRelease(gaplog->history);
    gaplog->size = 0;
}

/*gap log keys info*/
gtidGapLogKeysInfos* createGtidGapLogKeysInfos(int max_size) {
    gtidGapLogKeysInfos* infos = zmalloc(sizeof(gtidGapLogKeysInfos));
    infos->size = 0;
    infos->keys = zcalloc(sizeof(gtidGapLogKeyInfo*) * max_size);
    return infos;
}

void freeGtidGapLogKeysInfos(void *data) {
    if (data == NULL) return;
    gtidGapLogKeysInfos* kis = (gtidGapLogKeysInfos*)data;  
    for (int i = 0; i < kis->size; i++) {
        freeGtidGapLogKeyInfo(kis->keys[i]);
    }
    zfree(kis->keys);
    zfree(kis);
}

/*gap log key info*/
gtidGapLogKeyInfo* createGtidGapLogKeyInfo(int dbid, int type, sds key, sds* subkeys, int subkeys_count) {
    gtidGapLogKeyInfo *ki = zcalloc(sizeof(gtidGapLogKeyInfo));
    ki->dbid = dbid;
    ki->key_type = type;
    ki->key = key;           /* move */
    ki->subkeys = subkeys;   /* move */
    ki->subkeys_count = subkeys_count;
    return ki;
}

void freeGtidGapLogKeyInfo(gtidGapLogKeyInfo* ki) {
    if (ki == NULL) return;
    sdsfree(ki->key);
    for (int i = 0; i < ki->subkeys_count; i++) {
        sdsfree(ki->subkeys[i]);
    }
    zfree(ki->subkeys);
    zfree(ki);
}