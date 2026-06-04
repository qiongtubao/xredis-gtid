
#include "server.h"

static void uuidSetFreeWrapper(void *ptr) {
    uuidSet *us = (uuidSet*)ptr;
    if (us) {
        uuidSetFree(us);
    }
}

void gtidGaplogSkiplistDestructor(void *privdata, void *val) {
    UNUSED(privdata);
    skiplist *sl = (skiplist*)val;
    if (sl) {
        skiplistFree(sl);
    }
}

static dictType gtidGaplogDictType = {
    .hashFunction = dictSdsHash,
    .keyCompare = dictSdsKeyCompare,
    .keyDestructor = dictSdsDestructor,
    .valDestructor = gtidGaplogSkiplistDestructor
};
gtidGaplog* gtidGaplogNew() {
    gtidGaplog* gaplog =  zmalloc(sizeof(gtidGaplog));
    gaplog->data = dictCreate(&gtidGaplogDictType, NULL);
    gaplog->size = 0;
    gaplog->history = listCreate();
    listSetFreeMethod(gaplog->history, uuidSetFreeWrapper);
    return gaplog;
}

void gtidGaplogReset(gtidGaplog* gaplog) {
    dictEmpty(gaplog->data, NULL);
    gaplog->size = 0;
    listEmpty(gaplog->history);
}
void gtidGaplogRelease(gtidGaplog* gaplog) {
    dictRelease(gaplog->data);
    listRelease(gaplog->history);
    gaplog->size = 0;
}



void gtidGaplogKeysRelease(void *data) {
    if (data == NULL) return;
    gtidGaplogKeys* keys = (gtidGaplogKeys*)data;  
    for (int i = 0; i < keys->size; i++) {
        gtidGaplogKeyRelease(keys->keys[i]);
    }
    zfree(keys->keys);
    zfree(keys);
}

/*gap log key info*/
gtidGaplogKey* gtidGaplogKeyNew(int dbid, int type, sds key, sds* subkeys, int subkeys_count) {
    gtidGaplogKey *ki = zcalloc(sizeof(gtidGaplogKey));
    ki->dbid = dbid;
    ki->key_type = type;
    ki->key = key;           /* move */
    ki->subkeys = subkeys;   /* move */
    ki->subkeys_count = subkeys_count;
    return ki;
}

void gtidGaplogKeyRelease(gtidGaplogKey* ki) {
    if (ki == NULL) return;
    sdsfree(ki->key);
    for (int i = 0; i < ki->subkeys_count; i++) {
        sdsfree(ki->subkeys[i]);
    }
    zfree(ki->subkeys);
    zfree(ki);
}

gtidGaplogKey** gtidGaplogKeysPrepareBuilder(gtidGaplogKeysBuilder* builder, int add_numkeys) {
    if (!builder->keys_infos) {
        builder->keys_infos = builder->cache;
    }

    if (add_numkeys  + builder->numkeys > builder->size) {
        if (builder->keys_infos != builder->cache) {
            builder->keys_infos = zrealloc(builder->keys_infos, sizeof(gtidGaplogKey*) * ( 2 * builder->size));
        } else {
            builder->keys_infos = zmalloc(sizeof(gtidGaplogKey*) * (2 * builder->size));
            if (builder->numkeys)
                memcpy(builder->keys_infos, builder->cache, sizeof(gtidGaplogKey*) * builder->numkeys);
        }
        builder->size = 2 * builder->size;
    }
    return builder->keys_infos + builder->numkeys;

}

void gtidGaplogDeinitKeysBuilder(gtidGaplogKeysBuilder* builer) {
    for (int i  = 0; i < builer->numkeys; i++) {
        gtidGaplogKeyRelease(builer->keys_infos[i]);  
        builer->keys_infos[i] = NULL;
    }
    if (builer && builer->keys_infos != builer->cache) {
        zfree(builer->keys_infos);
    }   
}

gtidGaplogKeys* gtidGaplogKeysBuild(gtidGaplogKeysBuilder* builder) {
    gtidGaplogKeys* keys = zmalloc(sizeof(gtidGaplogKeys));
    keys->size = builder->numkeys;
    /*move keys*/
    keys->keys =zmalloc(sizeof(gtidGaplogKey*) * keys->size);
    for(int i = 0; i < builder->numkeys; i++) {
        keys->keys[i] = builder->keys_infos[i];
        builder->keys_infos[i] = NULL;
    }
    builder->numkeys = 0;
    return keys;
}

/* ========== gtidGaplog Data iterator ========== */
void gtidGaplogDataInitIterator(gtidGaplogDataIterator *iter, skiplist *sl, gno_t start_gno) {
    skiplistInitIterator(&iter->sl_iter, sl);
    skiplistIteratorSeek(&iter->sl_iter, start_gno);
}

void gtidGaplogDeinitDataIterator(gtidGaplogDataIterator *iter) {
    skiplistDeinitIterator(&iter->sl_iter);
}

void gtidGaplogDataIteratorSeek(gtidGaplogDataIterator *iter, gno_t gno) {
    skiplistIteratorSeek(&iter->sl_iter, gno);
}

gno_t gtidGaplogDataGetGno(gtidGaplogDataIterator* iter) {
    skiplistNode *node = iter->sl_iter.next;
    if (node == NULL) return -1;
    return (gno_t)node->score;
}

gtidGaplogKeys* gtidGaplogDataNext(gtidGaplogDataIterator* iter) {
    skiplistNode *node = skiplistIteratorNext(&iter->sl_iter);
    if (node == NULL) return NULL;
    return (gtidGaplogKeys*)node->value;
}

/* ========== gtidGaplog History iterator ========== */

void gtidGaplogInitHistoryIterator(gtidGaplogHistoryIterator* iter,
                                    gtidGaplog* gaplog, long long index) {
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


gno_t gtidGaplogHistoryNext(gtidGaplogHistoryIterator* iter,
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

void gtidGaplogDeinitHistoryIterator(gtidGaplogHistoryIterator* iter) {
    UNUSED(iter);
}

/* Reposition the history iterator so the next call to
 * gtidGaplogHistoryNext returns a gno >= `gno`. If `gno` falls inside the
 * current uuidSet's interval range, seek within it. Otherwise advance to
 * the first entry of the next uuidSet (if any). If the iterator is past
 * the end, leave it in the exhausted state. */
void gtidGaplogHistoryIteratorSeek(gtidGaplogHistoryIterator* iter, gno_t gno) {
    if (iter->list_node == NULL) return;

    uuidSet *us = listNodeValue(iter->list_node);
    uuidSetIterator us_iter;
    uuidSetInitIterator(&us_iter, us);

    if (uuidSetIteratorSeek(&us_iter, gno) == 0) {
        /* gno is past this uuidSet's range: jump to next uuidSet */
        iter->list_node = listNextNode(iter->list_node);
        iter->interval_node = NULL;
        iter->next_gno = 0;
        if (iter->list_node) {
            us = listNodeValue(iter->list_node);
            iter->interval_node = us->intervals->header->forwards[0];
            if (iter->interval_node)
                iter->next_gno = iter->interval_node->start;
        }
        uuidSetDeinitIterator(&us_iter);
        return;
    }

    iter->interval_node = us_iter.next;
    iter->next_gno = gno;
    uuidSetDeinitIterator(&us_iter);
}


void addReplyGtidGaplogKeys(client* c, gtidGaplogKeys* keys) {
    addReplyArrayLen(c, keys->size);
    for (int i = 0; i < keys->size; i++) {
        gtidGaplogKey *k = keys->keys[i];
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


int gtidGaplogTrim(gtidGaplog* gap_log ,size_t size) {
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
            skiplistDelete(sl, min_gno);
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