#ifndef  __skiplist_H__
#define  __skiplist_H__


#define SKIPLIST_MAXLEVEL 16

typedef struct skiplistNode {
    long long score;                     
    void *value;    
    struct skiplistNode *backward;
    struct {
        struct skiplistNode *forward; 
    } level[]; 
} skiplistNode;

typedef struct skipType {
    void (*freeValue)(void* value);
} skipType;

typedef struct skiplist {
    skiplistNode *header; 
    skiplistNode *tail;  
    unsigned long length;      
    int level;                 
    skipType* type;
} skiplist;
struct 
skiplist* createSkipList(skipType* type);
void freeSkipList(skiplist *sl);
int tryInsertSkipList(skiplist *sl, long long score, void *value, int score_unique);

int deleteSkipList(skiplist *sl, long long score);
void* firstSkipList(skiplist *sl);



#endif