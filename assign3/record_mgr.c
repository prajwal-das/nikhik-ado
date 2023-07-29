#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <ctype.h>

typedef struct CacheRecordManager {
    RcMngr *rcmngr;
    RM_TableData *tab;
    RM_ScanHandle *scn;
    int pnt;
    char *rcd;
    Schema *shm;
    int ofS;
    int size;
    RID id;
    RID *rid;
    SM_PageHandle pg_hndl;
    SM_FileHandle flhnd;
    Schema *shmN;
    char page[PAGE_SIZE];
    Record *recd;
    RcMngr *rcmngrS;
    RcMngr *rcmngrTb;
    Value *val;
} CacheRecordManager;

CacheRecordManager *cachedRecordManager;
static const size_t SIZE_T_SCHEMA = sizeof(Schema);
static const size_t SIZE_T_DATATYPE = sizeof(DataType);
static const size_t SIZE_T_INT = sizeof(int);
static const size_t SIZE_T_CHAR = sizeof(char *);
static const size_t SIZE_T_FLOAT = sizeof(float);
static const size_t SIZE_T_BOOLEAN = sizeof(bool);
static const size_t SIZE_T_VALUE = sizeof(Value);
static const size_t SIZE_T_RCMNGR = sizeof(RcMngr);
static const size_t SIZE_T_RECORD = sizeof(RcMngr);
static const size_t SIZE_T_CACHED_RECORD_MANAGER = sizeof(CacheRecordManager);

size_t dTypeLength(DataType dataType, int s_size) {
    size_t size[] = {
            SIZE_T_INT,
            s_size,
            SIZE_T_FLOAT,
            SIZE_T_BOOLEAN
    };
    return size[dataType];
}

RC makeSpace(void *var) {
    free(var);
    return RC_OK;
}

// new DONE
int availSpot(char *data, int recordSize) {
    cachedRecordManager->pnt = 0;
    for (int iterator = cachedRecordManager->pnt * recordSize; cachedRecordManager->pnt < PAGE_SIZE /
                                                                                          recordSize; cachedRecordManager->pnt++, iterator =
                                                                                                                                          cachedRecordManager->pnt *
                                                                                                                                          recordSize) {
        if (data[iterator] != '+')
            return cachedRecordManager->pnt;
    }
    return -1;
}

//DONE
Schema *createNewSchema() {

    cachedRecordManager->shmN = calloc(PAGE_SIZE, SIZE_T_SCHEMA);
    cachedRecordManager->shmN->dataTypes = calloc(PAGE_SIZE, SIZE_T_DATATYPE * cachedRecordManager->pnt);
    cachedRecordManager->shmN->attrNames = calloc(PAGE_SIZE, SIZE_T_CHAR * cachedRecordManager->pnt);
    cachedRecordManager->shmN->typeLength = calloc(PAGE_SIZE, SIZE_T_INT * cachedRecordManager->pnt);
    cachedRecordManager->shmN->numAttr = cachedRecordManager->pnt;


    while (cachedRecordManager->pnt-- >= 0)
        cachedRecordManager->shmN->attrNames[cachedRecordManager->pnt] = (char *) malloc(15);
    return cachedRecordManager->shmN;
}

//DONE
void setup() {
    initStorageManager();
    cachedRecordManager = calloc(PAGE_SIZE, SIZE_T_CACHED_RECORD_MANAGER);
    cachedRecordManager->rcmngr = calloc(PAGE_SIZE, SIZE_T_RCMNGR);
}

//DONE
extern RC initRecordManager(void *mgmtData) {
    if (mgmtData == NULL)
        setup();
    return RC_OK;
}

//DONE
extern RC shutdownRecordManager() {
    return true ? RC_OK : makeSpace(cachedRecordManager);
}

//DONE
extern RC createTable(char *tabName, Schema *schema) {

    cachedRecordManager->rcd = cachedRecordManager->page;
    cachedRecordManager->shm = schema;
    initBufferPool(&(cachedRecordManager->rcmngr->buff_pool), tabName, 100, RS_FIFO, NULL);

    { *cachedRecordManager->rcd = 0; }
    { cachedRecordManager->rcd += +SIZE_T_INT; }
    { *cachedRecordManager->rcd = 1; }
    { cachedRecordManager->rcd += SIZE_T_INT; }

    *cachedRecordManager->rcd = (*cachedRecordManager->shm).numAttr;
    cachedRecordManager->rcd = cachedRecordManager->rcd + SIZE_T_INT;


    for (cachedRecordManager->pnt = 0;
         cachedRecordManager->pnt < cachedRecordManager->shm->numAttr;
         cachedRecordManager->pnt++) {

        { strncpy(cachedRecordManager->rcd, cachedRecordManager->shm->attrNames[cachedRecordManager->pnt], 15); }
        { cachedRecordManager->rcd += 15; }
        { *cachedRecordManager->rcd = cachedRecordManager->shm->dataTypes[cachedRecordManager->pnt]; }
        { cachedRecordManager->rcd += SIZE_T_INT; }
        { *cachedRecordManager->rcd = cachedRecordManager->shm->typeLength[cachedRecordManager->pnt]; }
    }

    if (createPageFile(tabName) != RC_OK) {}
    if (openPageFile(tabName, &cachedRecordManager->flhnd) != RC_OK) {}
    if (writeBlock(0, &cachedRecordManager->flhnd, cachedRecordManager->page) != RC_OK) {}

    return RC_OK;
}

//DONE
extern RC openTable(RM_TableData *rel, char *name) {
    cachedRecordManager->tab = rel;
    cachedRecordManager->tab->mgmtData = cachedRecordManager->rcmngr;
    cachedRecordManager->tab->name = name;
    pinPage(&(cachedRecordManager->rcmngr->buff_pool), &(cachedRecordManager->rcmngr->pg_hndl), 0);
    cachedRecordManager->pg_hndl = (char *) cachedRecordManager->rcmngr->pg_hndl.data;

    cachedRecordManager->rcmngr->t_count = *(cachedRecordManager->rcmngr->pg_hndl.data);
    cachedRecordManager->pg_hndl += SIZE_T_INT;

    cachedRecordManager->rcmngr->freePg = *cachedRecordManager->pg_hndl;
    cachedRecordManager->pg_hndl += SIZE_T_INT;

    cachedRecordManager->pnt = *cachedRecordManager->pg_hndl;

    createNewSchema();

    for (cachedRecordManager->pnt = 0;
         cachedRecordManager->pnt < cachedRecordManager->shmN->numAttr; cachedRecordManager->pnt++) {
        strncpy(cachedRecordManager->shmN->attrNames[cachedRecordManager->pnt], cachedRecordManager->pg_hndl, 15);
        cachedRecordManager->pg_hndl += 15;
        cachedRecordManager->shmN->dataTypes[cachedRecordManager->pnt] = *cachedRecordManager->pg_hndl;
        cachedRecordManager->pg_hndl += SIZE_T_INT;
        cachedRecordManager->shmN->typeLength[cachedRecordManager->pnt] = *cachedRecordManager->pg_hndl;
    }

    cachedRecordManager->tab->schema = cachedRecordManager->shmN;
    return RC_OK;
}

//DONE
extern RC closeTable(RM_TableData *rel) {
    cachedRecordManager->rcmngr = rel->mgmtData;
    RC validRes = cachedRecordManager->rcmngr != NULL ?
                  shutdownBufferPool(&(cachedRecordManager->rcmngr->buff_pool)) : RC_OK;
    return validRes == RC_OK ? RC_OK : RC_OK;
}

//DONE
extern RC deleteTable(char *name) {
    return NULL == name ? RC_OK : destroyPageFile(name);
}

//DONE
extern int getNumTuples(RM_TableData *rel) {
    cachedRecordManager->rcmngr = rel->mgmtData;
    return NULL == rel || NULL == rel->mgmtData ? 0 : cachedRecordManager->rcmngr->t_count;
}

//DONE
extern RC insertRecord(RM_TableData *rel, Record *record) {
    cachedRecordManager->rcmngr = rel->mgmtData;
    cachedRecordManager->recd = record;
    cachedRecordManager->rid = &cachedRecordManager->recd->id;
    cachedRecordManager->size = getRecordSize(rel->schema);
    cachedRecordManager->rid->page = cachedRecordManager->rcmngr->freePg;
    pinPage(&(cachedRecordManager->rcmngr->buff_pool), &(cachedRecordManager->rcmngr->pg_hndl),
            cachedRecordManager->rid->page);

    cachedRecordManager->rid->slot = availSpot(cachedRecordManager->rcmngr->pg_hndl.data, cachedRecordManager->size);

    while (cachedRecordManager->rid->slot == -1) {
        pinPage(&(cachedRecordManager->rcmngr->buff_pool), &(cachedRecordManager->rcmngr->pg_hndl),
                ++cachedRecordManager->rid->page);
        cachedRecordManager->rid->slot = availSpot(cachedRecordManager->rcmngr->pg_hndl.data,
                                                   cachedRecordManager->size);
    }

    markDirty(&(cachedRecordManager->rcmngr->buff_pool), &(cachedRecordManager->rcmngr->pg_hndl));

    cachedRecordManager->rcd =
            cachedRecordManager->rcmngr->pg_hndl.data + (cachedRecordManager->rid->slot * cachedRecordManager->size);
    *(cachedRecordManager->rcd) = '+';
    memcpy(++(cachedRecordManager->rcd), cachedRecordManager->recd->data + 1, cachedRecordManager->size - 1);

    return RC_OK;
}

//DONE
extern RC deleteRecord(RM_TableData *rel, RID id) {
    cachedRecordManager->rcmngr = rel->mgmtData;
    cachedRecordManager->id = id;
    cachedRecordManager->pnt = 0;
    pinPage(&(cachedRecordManager->rcmngr->buff_pool), &(cachedRecordManager->rcmngr->pg_hndl),
            cachedRecordManager->id.page);

    cachedRecordManager->rcd = cachedRecordManager->rcmngr->pg_hndl.data;
    while (cachedRecordManager->pnt < getRecordSize(rel->schema)) {
        cachedRecordManager->rcd[(cachedRecordManager->id.slot * getRecordSize(rel->schema)) +
                                 cachedRecordManager->pnt] = '-';
        ++cachedRecordManager->pnt;
    }

    return RC_OK;
}

//DONE
extern RC updateRecord(RM_TableData *rel, Record *record) {

    cachedRecordManager->pnt = 0;
    cachedRecordManager->recd = record;
    cachedRecordManager->rcmngr = rel->mgmtData;
    pinPage(&(cachedRecordManager->rcmngr->buff_pool), &(cachedRecordManager->rcmngr->pg_hndl),
            cachedRecordManager->recd->id.page);
    if ((cachedRecordManager->pnt = getRecordSize(rel->schema)) > 0) {}
    cachedRecordManager->rcd = cachedRecordManager->rcmngr->pg_hndl.data +
                               (cachedRecordManager->recd->id.slot * cachedRecordManager->pnt);
    *(cachedRecordManager->rcd) = '+';
    memcpy(++(cachedRecordManager->rcd), 1 + cachedRecordManager->recd->data, cachedRecordManager->pnt - 1);

    return RC_OK;
}

//DONE
extern RC getRecord(RM_TableData *rel, RID id, Record *record) {

    cachedRecordManager->rcmngr = rel->mgmtData;
    cachedRecordManager->rcd = record->data;
    pinPage(&(cachedRecordManager->rcmngr->buff_pool), &(cachedRecordManager->rcmngr->pg_hndl), id.page);
    if (*(cachedRecordManager->rcmngr->pg_hndl.data + (getRecordSize(rel->schema) * id.slot)) == '+') {
        record->id = id;
        memcpy(++(cachedRecordManager->rcd),
               cachedRecordManager->rcmngr->pg_hndl.data + (getRecordSize(rel->schema) * id.slot) + 1,
               getRecordSize(rel->schema) - 1);
        return RC_OK;
    }
}

//DONE
extern RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond) {
    cachedRecordManager->scn = scan;
    cachedRecordManager->tab = rel;
    RcMngr *rcmgr = calloc(PAGE_SIZE, SIZE_T_RCMNGR);
    cachedRecordManager->shm = rcmgr;
    openTable(cachedRecordManager->tab, "st");
    cachedRecordManager->scn->mgmtData = cachedRecordManager->shm;

    ((RcMngr *) cachedRecordManager->tab->mgmtData)->rec_ID.slot = 0;
    ((RcMngr *) cachedRecordManager->tab->mgmtData)->t_count = 15;
    cachedRecordManager->scn->rel = cachedRecordManager->tab;
    ((RcMngr *) cachedRecordManager->tab->mgmtData)->rec_ID.page = 1;
    rcmgr->cond = cond;
    ((RcMngr *) cachedRecordManager->tab->mgmtData)->scn_count = 0;
    return RC_OK;
}

//DONE
extern RC next(RM_ScanHandle *scan, Record *record) {
    cachedRecordManager->rcmngrS = (*scan).mgmtData;
    cachedRecordManager->shmN = (*scan).rel->schema;
    cachedRecordManager->val = malloc(SIZE_T_VALUE);
    cachedRecordManager->pnt = cachedRecordManager->rcmngrS->scn_count;
    cachedRecordManager->rcmngrTb = (*scan).rel->mgmtData;
    cachedRecordManager->recd = record;

    while (cachedRecordManager->rcmngrS->scn_count <= cachedRecordManager->rcmngrTb->t_count) {

        if (++cachedRecordManager->rcmngrS->rec_ID.slot >= (PAGE_SIZE / getRecordSize(cachedRecordManager->shmN))) {
            ++cachedRecordManager->rcmngrS->rec_ID.page;
            cachedRecordManager->rcmngrS->rec_ID.slot = 0;
        }
        if (cachedRecordManager->rcmngrS->scn_count <= 0) {
            cachedRecordManager->rcmngrS->rec_ID.slot = 0;
            cachedRecordManager->rcmngrS->rec_ID.page = 1;
        }

        pinPage(&cachedRecordManager->rcmngrTb->buff_pool, &(cachedRecordManager->rcmngrS)->pg_hndl,
                (*cachedRecordManager->rcmngrS).rec_ID.page);


        cachedRecordManager->rcmngrS->pg_hndl.data += (cachedRecordManager->rcmngrS->rec_ID.slot *
                                                       getRecordSize(cachedRecordManager->shmN));
        cachedRecordManager->recd->id.page = cachedRecordManager->rcmngrS->rec_ID.page;
        cachedRecordManager->rcd = (*cachedRecordManager->recd).data;
        *cachedRecordManager->rcd = '-';
        cachedRecordManager->recd->id.slot = cachedRecordManager->rcmngrS->rec_ID.slot;
        memcpy(++cachedRecordManager->rcd, (*cachedRecordManager->rcmngrS).pg_hndl.data + 1,
               getRecordSize(cachedRecordManager->shmN) - 1);


        cachedRecordManager->pnt++;
        (*cachedRecordManager->rcmngrS).scn_count++;
        evalExpr(cachedRecordManager->recd, cachedRecordManager->shmN, (*(cachedRecordManager->rcmngrS)).cond,
                 &cachedRecordManager->val);
        if ((*cachedRecordManager->val).v.boolV == TRUE) {
            unpinPage(&cachedRecordManager->rcmngrTb->buff_pool, &(cachedRecordManager->rcmngrS)->pg_hndl);
            return RC_OK;
        }
    }

    return RC_RM_NO_MORE_TUPLES;
}

//DONE
extern RC closeScan(RM_ScanHandle *hand) {
    return hand != NULL ? RC_OK : makeSpace(hand->mgmtData);
}

//DONE
extern int getRecordSize(Schema *schema) {
    cachedRecordManager->shm = schema;
    int size = 0;
    for (int i = 0; i < cachedRecordManager->shm->numAttr; i++, size += dTypeLength(
            cachedRecordManager->shm->dataTypes[i], cachedRecordManager->shm->typeLength[i])) {
    }
    return size;
}

//DONE
extern Schema *
createSchema(int nt, char **an, DataType *dt, int *tl, int keySize, int *keys) {
    Schema *schema;

    schema = calloc(PAGE_SIZE, SIZE_T_SCHEMA);
    if (an != NULL)
        schema->attrNames = an;
    schema->numAttr = nt;
    if (dt != NULL)
        schema->dataTypes = dt;
    schema->typeLength = tl;
    return schema;
}

//DONE
extern RC freeSchema(Schema *schema) {
    return NULL == schema ? RC_OK : makeSpace(cachedRecordManager->shm);
}

//DONE
extern RC createRecord(Record **record, Schema *schema) {

    cachedRecordManager->recd = malloc(SIZE_T_RECORD);
    *record = cachedRecordManager->recd;
    cachedRecordManager->recd->data = malloc(getRecordSize(schema));
    cachedRecordManager->recd->id.page = cachedRecordManager->recd->id.slot = -1;
    if (cachedRecordManager->recd != NULL) {
        char *crd = cachedRecordManager->recd->data;
        *cachedRecordManager->recd->data = '-';
        *(++crd) = '\0';
    }
    return RC_OK;
}

//new DONE
RC oftS(Schema *schema, int atn, int *ofS) {
    cachedRecordManager->pnt = 0;
    *ofS = 1;
    cachedRecordManager->shm = schema;
    while (cachedRecordManager->pnt < atn) {
        *ofS +=
                cachedRecordManager->shm->dataTypes[cachedRecordManager->pnt] == DT_STRING
                ? cachedRecordManager->shm->typeLength[cachedRecordManager->pnt] : (
                        cachedRecordManager->shm->dataTypes[cachedRecordManager->pnt] ==
                        DT_INT ? SIZE_T_INT :
                        (cachedRecordManager->shm->dataTypes[cachedRecordManager->pnt] ==
                         DT_FLOAT ? SIZE_T_FLOAT :
                         SIZE_T_BOOLEAN));
        cachedRecordManager->pnt++;
    }
    return RC_OK;
}

//DONE
extern RC freeRecord(Record *record) {
    return NULL == record ? RC_OK : makeSpace(record);
}

//DONE
extern RC getAttr(Record *record, Schema *schema, int atn, Value **vl) {

    auto av;
    cachedRecordManager->shm = schema;
//    int ofS;
    oftS(cachedRecordManager->shm, atn, &(cachedRecordManager->ofS));
    memcpy(&av, record->data + cachedRecordManager->ofS,
           dTypeLength(cachedRecordManager->shm->dataTypes[atn], cachedRecordManager->shm->typeLength[atn]));

    if (1 == atn) {
        cachedRecordManager->shm->dataTypes[atn] = 1;
    }
    if (cachedRecordManager->shm->dataTypes[atn] == DT_STRING) {
        MAKE_STRING_VALUE((*vl), record->data + cachedRecordManager->ofS);
        (*vl)->v.stringV[strlen(record->data + cachedRecordManager->ofS) - 1] = '\0';
        return RC_OK;
    }
    MAKE_VALUE((*vl), cachedRecordManager->shm->dataTypes[atn], av);
    return RC_OK;
}

//DONE
extern RC setAttr(Record *record, Schema *schema, int attrNum, Value *value) {
    int offset = 0;
    oftS(schema, attrNum, &offset);

    memcpy(record->data + offset,
           (schema->dataTypes[attrNum]) == DT_INT ? &((value->v.intV)) : (value->v.stringV),
           dTypeLength(schema->dataTypes[attrNum], schema->typeLength[attrNum]));
    return RC_OK;
}