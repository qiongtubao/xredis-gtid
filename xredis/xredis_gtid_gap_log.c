
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
    infos->keys = zmalloc(sizeof(gtidGapLogKeysInfos) * max_size);
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

gtidGapLogKeyInfo** gtidGapLogKeysPrepareBuildfer(gtidGapLogKeysBuilder* builder, int add_numkeys) {
    if (!builder->keys_infos) {
        builder->keys_infos = builder->cache;
    }

    if (add_numkeys  + builder->numkeys > builder->size) {
        if (builder->keys_infos != builder->cache) {
            builder->keys_infos = zrealloc(builder->keys_infos, sizeof(gtidGapLogKeyInfo*) * ( 2 * builder->size));
        } else {
            builder->keys_infos = zmalloc(sizeof(gtidGapLogKeyInfo*) * (2 * builder->size));
            if (builder->numkeys)
                memcpy(builder->keys_infos, builder->cache, sizeof(gtidGapLogKeyInfo*) * builder->numkeys);
        }
        builder->size = 2 * builder->size;
    }
    return builder->keys_infos + builder->numkeys;

}

void freeGtidGaplogKeysBuilder(gtidGapLogKeysBuilder* builer) {
    for (int i  = 0; i < builer->numkeys; i++) {
        freeGtidGapLogKeyInfo(builer->keys_infos[i]);  
        builer->keys_infos[i] = NULL;
    }
    if (builer && builer->keys_infos != builer->cache) {
        zfree(builer->keys_infos);
    }   
}

gtidGapLogKeysInfos* buildGtidGapLogKeys(gtidGapLogKeysBuilder* builder) {
    gtidGapLogKeysInfos* keys = zmalloc(sizeof(gtidGapLogKeysInfos));
    keys->size = builder->numkeys;
    if (builder->cache == builder->keys_infos) {
        keys->keys =zmalloc(sizeof(gtidGapLogKeyInfo*) * keys->size);
        for(int i = 0; i < builder->numkeys; i++) {
            keys->keys[i] = builder->keys_infos[i];
            builder->keys_infos[i] = NULL;
        }
    } else {
        keys->keys = builder->keys_infos;
        builder->keys_infos = NULL;
        
    }
    builder->numkeys = 0;
    return keys;
}