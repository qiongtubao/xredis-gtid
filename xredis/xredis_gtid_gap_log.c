
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
gtidGapLog* gtidGapLogNew() {
    gtidGapLog* gaplog =  zmalloc(sizeof(gtidGapLog));
    gaplog->data = dictCreate(&gtidGapLogDictType, NULL);
    gaplog->size = 0;
    gaplog->history = listCreate();
    listSetFreeMethod(gaplog->history, uuidSetFreeWrapper);
    return gaplog;
}

void gtidGapLogReset(gtidGapLog* gaplog) {
    dictEmpty(gaplog->data, NULL);
    gaplog->size = 0;
    listEmpty(gaplog->history);
}
void gtidGapLogRelease(gtidGapLog* gaplog) {
    dictRelease(gaplog->data);
    listRelease(gaplog->history);
    gaplog->size = 0;
}



void gtidGapLogKeysRelease(void *data) {
    if (data == NULL) return;
    gtidGapLogKeys* keys = (gtidGapLogKeys*)data;  
    for (int i = 0; i < keys->size; i++) {
        gtidGapLogKeyRelease(keys->keys[i]);
    }
    zfree(keys->keys);
    zfree(keys);
}

/*gap log key info*/
gtidGapLogKey* gtidGapLogKeyNew(int dbid, int type, sds key, sds* subkeys, int subkeys_count) {
    gtidGapLogKey *ki = zcalloc(sizeof(gtidGapLogKey));
    ki->dbid = dbid;
    ki->key_type = type;
    ki->key = key;           /* move */
    ki->subkeys = subkeys;   /* move */
    ki->subkeys_count = subkeys_count;
    return ki;
}

void gtidGapLogKeyRelease(gtidGapLogKey* ki) {
    if (ki == NULL) return;
    sdsfree(ki->key);
    for (int i = 0; i < ki->subkeys_count; i++) {
        sdsfree(ki->subkeys[i]);
    }
    zfree(ki->subkeys);
    zfree(ki);
}

gtidGapLogKey** gtidGapLogKeysPrepareBuilder(gtidGapLogKeysBuilder* builder, int add_numkeys) {
    if (!builder->keys_infos) {
        builder->keys_infos = builder->cache;
    }

    if (add_numkeys  + builder->numkeys > builder->size) {
        if (builder->keys_infos != builder->cache) {
            builder->keys_infos = zrealloc(builder->keys_infos, sizeof(gtidGapLogKey*) * ( 2 * builder->size));
        } else {
            builder->keys_infos = zmalloc(sizeof(gtidGapLogKey*) * (2 * builder->size));
            if (builder->numkeys)
                memcpy(builder->keys_infos, builder->cache, sizeof(gtidGapLogKey*) * builder->numkeys);
        }
        builder->size = 2 * builder->size;
    }
    return builder->keys_infos + builder->numkeys;

}

void gtidGapLogDeinitKeysBuilder(gtidGapLogKeysBuilder* builer) {
    for (int i  = 0; i < builer->numkeys; i++) {
        gtidGapLogKeyRelease(builer->keys_infos[i]);  
        builer->keys_infos[i] = NULL;
    }
    if (builer && builer->keys_infos != builer->cache) {
        zfree(builer->keys_infos);
    }   
}

gtidGapLogKeys* buildGtidGapLogKeys(gtidGapLogKeysBuilder* builder) {
    gtidGapLogKeys* keys = zmalloc(sizeof(gtidGapLogKeys));
    keys->size = builder->numkeys;
    /*move keys*/
    keys->keys =zmalloc(sizeof(gtidGapLogKey*) * keys->size);
    for(int i = 0; i < builder->numkeys; i++) {
        keys->keys[i] = builder->keys_infos[i];
        builder->keys_infos[i] = NULL;
    }
    builder->numkeys = 0;
    return keys;
}

/* ========== gtidGapLog Data iterator ========== */
void gtidGapLogInitDataIterator(gtidGapLogDataIterator *iter, skiplist *sl, gno_t start_gno) {
    iter->node = findFirstGteSkipList(sl, start_gno);
}

void gtidGapLogDeinitDataIterator(gtidGapLogDataIterator *iter) {
    UNUSED(iter);
}

gno_t gtidGapLogDataGetGno(gtidGapLogDataIterator* iter) {
    if (iter->node == NULL) return -1;
    return (gno_t)iter->node->score;
}

gtidGapLogKeys* gtidGapLogDataNext(gtidGapLogDataIterator* iter) {
    if (iter->node == NULL) return NULL;
    gtidGapLogKeys* keys = (gtidGapLogKeys*)iter->node->value;
    iter->node = iter->node->level[0].forward;
    return keys;
}

/* ========== gtidGapLog History iterator ========== */

void gtidGapLogInitHistoryIterator(gtidGapLogHistoryIterator* iter,
                                    gtidGapLog* gaplog, long long index) {
    iter->list_node = listFirst(gaplog->history);
    iter->interval_node = NULL;
    iter->next_gno = 0;

    if (iter->list_node == NULL) return;

    uuidSet *us = listNodeValue(iter->list_node);
    iter->interval_node = us->intervals->header->forwards[0];

    long long remaining = index;

    while (iter->list_node && remaining > 0) {
        us = listNodeValue(iter->list_node);
        gno_t us_count = uuidSetCount(us);
        if (remaining >= us_count) {
            remaining -= us_count;
            iter->list_node = listNextNode(iter->list_node);
            if (iter->list_node) {
                us = listNodeValue(iter->list_node);
                iter->interval_node = us->intervals->header->forwards[0];
            }
            continue;
        }
        break;
    }

    while (iter->interval_node && remaining > 0) {
        gno_t interval_len = iter->interval_node->end - iter->interval_node->start + 1;
        if (remaining >= interval_len) {
            remaining -= interval_len;
            iter->interval_node = iter->interval_node->forwards[0];
            continue;
        }
        break;
    }

    if (iter->list_node && iter->interval_node) {
        iter->next_gno = iter->interval_node->start + remaining;
    } else {
        iter->list_node = NULL;
    }
}


gno_t gtidGapLogHistoryNext(gtidGapLogHistoryIterator* iter,
                             const char** uuid, size_t* uuid_len) {
    if (iter->list_node == NULL) {
        *uuid = NULL;
        *uuid_len = 0;
        return 0;
    }

    uuidSet *us = listNodeValue(iter->list_node);
    *uuid = us->uuid;
    *uuid_len = us->uuid_len;

    gno_t result = iter->next_gno;

    iter->next_gno++;

    if (iter->next_gno > iter->interval_node->end) {
        iter->interval_node = iter->interval_node->forwards[0];
        if (iter->interval_node) {
            iter->next_gno = iter->interval_node->start;
        } else {
            iter->list_node = listNextNode(iter->list_node);
            if (iter->list_node) {
                us = listNodeValue(iter->list_node);
                iter->interval_node = us->intervals->header->forwards[0];
                iter->next_gno = iter->interval_node->start;
            }
        }
    }

    return result;
}

void gtidGapLogDeinitHistoryIterator(gtidGapLogHistoryIterator* iter) {
    UNUSED(iter);
}


void addReplyGtidGapLogKeys(client* c, gtidGapLogKeys* keys) {
    addReplyArrayLen(c, keys->size);
    for (int i = 0; i < keys->size; i++) {
        gtidGapLogKey *k = keys->keys[i];
        addReplyArrayLen(c, 4);
        addReplyBulkLongLong(c, k->dbid);
        robj o = { .type = k->key_type };
        addReplyBulkCString(c, getObjectTypeName(&o));
        addReplyBulkCBuffer(c, k->key, sdslen(k->key));
        addReplyArrayLen(c, k->subkeys_count);
        for (int j = 0; j < k->subkeys_count; j++) {
            addReplyBulkCBuffer(c, k->subkeys[j], sdslen(k->subkeys[j]));
        }
    }
}


int gtidGapLogTrim(gtidGapLog* gap_log ,size_t size) {
    size_t count = 0;
    while (count < size) {
        listNode *first_ln = listFirst(gap_log->history);
        if (first_ln == NULL) return count;

        uuidSet *first_uuid_set = (uuidSet*)listNodeValue(first_ln);

        gno_t min_gno = uuidSetNext(first_uuid_set, 0);
        if (min_gno == 0) {
            listDelNode(gap_log->history, first_ln);
            continue;
        }

        sds evict_uuid_sds = sdsnewlen(first_uuid_set->uuid, first_uuid_set->uuid_len);
        dictEntry *de = dictFind(gap_log->data, evict_uuid_sds);
        if (de != NULL) {
            skiplist *sl = dictGetVal(de);
            deleteSkipList(sl, min_gno);
            if (sl->length == 0) {
                dictDelete(gap_log->data, evict_uuid_sds);
            }
        } else {
            serverPanic("not find keysinfo in gtid_gap_log");
        }
        sdsfree(evict_uuid_sds);
        
        uuidSetRemove(first_uuid_set, min_gno, min_gno);
        if (uuidSetCount(first_uuid_set) == 0) {
            listDelNode(gap_log->history, first_ln);
        }

        gap_log->size--;
        count++;
    }
    return count;
}