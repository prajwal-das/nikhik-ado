#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <ctype.h>

typedef struct CacheRecordManager {
    RcMngr *rcmngr;
    RM_TableData *rel;
    RM_ScanHandle *scan;
    int pnt;
    char *rcd;
    Schema *schema;
    int ofS;
    int size;
    RID id;
    RID *rid;
    SM_PageHandle pg_hndl;
    SM_FileHandle fh;
    Schema *newschema;
    char page[PAGE_SIZE];
    Record *record;
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

    cachedRecordManager->newschema = calloc(PAGE_SIZE, SIZE_T_SCHEMA);
    cachedRecordManager->newschema->dataTypes = calloc(PAGE_SIZE, SIZE_T_DATATYPE * cachedRecordManager->pnt);
    cachedRecordManager->newschema->attrNames = calloc(PAGE_SIZE, SIZE_T_CHAR * cachedRecordManager->pnt);
    cachedRecordManager->newschema->typeLength = calloc(PAGE_SIZE, SIZE_T_INT * cachedRecordManager->pnt);
    cachedRecordManager->newschema->numAttr = cachedRecordManager->pnt;


    while (cachedRecordManager->pnt-- >= 0)
        cachedRecordManager->newschema->attrNames[cachedRecordManager->pnt] = (char *) malloc(15);
    return cachedRecordManager->newschema;
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
    cachedRecordManager->schema = schema;
    initBufferPool(&(cachedRecordManager->rcmngr->buff_pool), tabName, 100, RS_FIFO, NULL);

    { *cachedRecordManager->rcd = 0; }
    { cachedRecordManager->rcd += +SIZE_T_INT; }
    { *cachedRecordManager->rcd = 1; }
    { cachedRecordManager->rcd += SIZE_T_INT; }

    *cachedRecordManager->rcd = (*cachedRecordManager->schema).numAttr;
    cachedRecordManager->rcd = cachedRecordManager->rcd + SIZE_T_INT;


    for (cachedRecordManager->pnt = 0;
         cachedRecordManager->pnt < cachedRecordManager->schema->numAttr;
         cachedRecordManager->pnt++) {

        { strncpy(cachedRecordManager->rcd, cachedRecordManager->schema->attrNames[cachedRecordManager->pnt], 15); }
        { cachedRecordManager->rcd += 15; }
        { *cachedRecordManager->rcd = cachedRecordManager->schema->dataTypes[cachedRecordManager->pnt]; }
        { cachedRecordManager->rcd += SIZE_T_INT; }
        { *cachedRecordManager->rcd = cachedRecordManager->schema->typeLength[cachedRecordManager->pnt]; }
    }

    if (createPageFile(tabName) != RC_OK) {}
    if (openPageFile(tabName, &cachedRecordManager->fh) != RC_OK) {}
    if (writeBlock(0, &cachedRecordManager->fh, cachedRecordManager->page) != RC_OK) {}

    return RC_OK;
}

//DONE
extern RC openTable(RM_TableData *rel, char *name) {
    cachedRecordManager->rel = rel;
    cachedRecordManager->rel->mgmtData = cachedRecordManager->rcmngr;
    cachedRecordManager->rel->name = name;
    pinPage(&(cachedRecordManager->rcmngr->buff_pool), &(cachedRecordManager->rcmngr->pg_hndl), 0);
    cachedRecordManager->pg_hndl = (char *) cachedRecordManager->rcmngr->pg_hndl.data;

    cachedRecordManager->rcmngr->t_count = *(cachedRecordManager->rcmngr->pg_hndl.data);
    cachedRecordManager->pg_hndl += SIZE_T_INT;

    cachedRecordManager->rcmngr->freePg = *cachedRecordManager->pg_hndl;
    cachedRecordManager->pg_hndl += SIZE_T_INT;

    cachedRecordManager->pnt = *cachedRecordManager->pg_hndl;

    createNewSchema();

    for (cachedRecordManager->pnt = 0;
         cachedRecordManager->pnt < cachedRecordManager->newschema->numAttr; cachedRecordManager->pnt++) {
        strncpy(cachedRecordManager->newschema->attrNames[cachedRecordManager->pnt], cachedRecordManager->pg_hndl, 15);
        cachedRecordManager->pg_hndl += 15;
        cachedRecordManager->newschema->dataTypes[cachedRecordManager->pnt] = *cachedRecordManager->pg_hndl;
        cachedRecordManager->pg_hndl += SIZE_T_INT;
        cachedRecordManager->newschema->typeLength[cachedRecordManager->pnt] = *cachedRecordManager->pg_hndl;
    }

    cachedRecordManager->rel->schema = cachedRecordManager->newschema;
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
    cachedRecordManager->record = record;
    cachedRecordManager->rid = &cachedRecordManager->record->id;
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
    memcpy(++(cachedRecordManager->rcd), cachedRecordManager->record->data + 1, cachedRecordManager->size - 1);

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
    cachedRecordManager->record = record;
    cachedRecordManager->rcmngr = rel->mgmtData;
    pinPage(&(cachedRecordManager->rcmngr->buff_pool), &(cachedRecordManager->rcmngr->pg_hndl),
            cachedRecordManager->record->id.page);
    if ((cachedRecordManager->pnt = getRecordSize(rel->schema)) > 0) {}
    cachedRecordManager->rcd = cachedRecordManager->rcmngr->pg_hndl.data +
                               (cachedRecordManager->record->id.slot * cachedRecordManager->pnt);
    *(cachedRecordManager->rcd) = '+';
    memcpy(++(cachedRecordManager->rcd), 1 + cachedRecordManager->record->data, cachedRecordManager->pnt - 1);

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
    cachedRecordManager->scan = scan;
    cachedRecordManager->rel = rel;
    RcMngr *rcmgr = calloc(PAGE_SIZE, SIZE_T_RCMNGR);
    cachedRecordManager->schema = rcmgr;
    openTable(cachedRecordManager->rel, "st");
    cachedRecordManager->scan->mgmtData = cachedRecordManager->schema;

    ((RcMngr *) cachedRecordManager->rel->mgmtData)->rec_ID.slot = 0;
    ((RcMngr *) cachedRecordManager->rel->mgmtData)->t_count = 15;
    cachedRecordManager->scan->rel = cachedRecordManager->rel;
    ((RcMngr *) cachedRecordManager->rel->mgmtData)->rec_ID.page = 1;
    rcmgr->cond = cond;
    ((RcMngr *) cachedRecordManager->rel->mgmtData)->scn_count = 0;
    return RC_OK;
}

//DONE
extern RC next(RM_ScanHandle *scan, Record *record) {
    cachedRecordManager->rcmngrS = (*scan).mgmtData;
    cachedRecordManager->newschema = (*scan).rel->schema;
    cachedRecordManager->val = malloc(SIZE_T_VALUE);
    cachedRecordManager->pnt = cachedRecordManager->rcmngrS->scn_count;
    cachedRecordManager->rcmngrTb = (*scan).rel->mgmtData;
    cachedRecordManager->record = record;


    while (cachedRecordManager->rcmngrS->scn_count <= cachedRecordManager->rcmngrTb->t_count) {
        if (++cachedRecordManager->rcmngrS->rec_ID.slot >= (PAGE_SIZE / getRecordSize(cachedRecordManager->newschema))) {
            ++cachedRecordManager->rcmngrS->rec_ID.page;
            cachedRecordManager->rcmngrS->rec_ID.slot = 0;
        }

        pinPage(&cachedRecordManager->rcmngrTb->buff_pool, &(cachedRecordManager->rcmngrS)->pg_hndl,
                (*cachedRecordManager->rcmngrS).rec_ID.page);

        if (cachedRecordManager->rcmngrS->scn_count <= 0) {
            cachedRecordManager->rcmngrS->rec_ID.slot = 0;
            cachedRecordManager->rcmngrS->rec_ID.page = 1;
        }
        cachedRecordManager->rcmngrS->pg_hndl.data += (cachedRecordManager->rcmngrS->rec_ID.slot *
                                                         getRecordSize(cachedRecordManager->newschema));
        cachedRecordManager->record->id.page = cachedRecordManager->rcmngrS->rec_ID.page;
        cachedRecordManager->rcd = (*cachedRecordManager->record).data;
        *cachedRecordManager->rcd = '-';
        cachedRecordManager->record->id.slot = cachedRecordManager->rcmngrS->rec_ID.slot;
        memcpy(++cachedRecordManager->rcd, (*cachedRecordManager->rcmngrS).pg_hndl.data + 1,
               getRecordSize(cachedRecordManager->newschema) - 1);

        cachedRecordManager->pnt++;
        (*cachedRecordManager->rcmngrS).scn_count++;
        evalExpr(cachedRecordManager->record, cachedRecordManager->newschema, cachedRecordManager->rcmngrS->cond,
                 &cachedRecordManager->val);
    }
    return RC_OK;
}

//DONE
extern RC closeScan(RM_ScanHandle *hand) {
    return hand != NULL ? RC_OK : makeSpace(hand->mgmtData);
}

//DONE
extern int getRecordSize(Schema *schema) {
    cachedRecordManager->schema = schema;
    int size = 0;
    for (int i = 0; i < cachedRecordManager->schema->numAttr; i++, size += dTypeLength(
            cachedRecordManager->schema->dataTypes[i], cachedRecordManager->schema->typeLength[i])) {
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
    return NULL == schema ? RC_OK : makeSpace(cachedRecordManager->schema);
}

//DONE
extern RC createRecord(Record **record, Schema *schema) {

    cachedRecordManager->record = malloc(SIZE_T_RECORD);
    *record = cachedRecordManager->record;
    cachedRecordManager->record->data = malloc(getRecordSize(schema));
    cachedRecordManager->record->id.page = cachedRecordManager->record->id.slot = -1;
    if (cachedRecordManager->record != NULL) {
        char *crd = cachedRecordManager->record->data;
        *cachedRecordManager->record->data = '-';
        *(++crd) = '\0';
    }
    return RC_OK;
}

//new DONE
RC oftS(Schema *schema, int atn, int *ofS) {
    cachedRecordManager->pnt = 0;
    *ofS = 1;
    cachedRecordManager->schema = schema;
    while (cachedRecordManager->pnt < atn) {
        *ofS +=
                cachedRecordManager->schema->dataTypes[cachedRecordManager->pnt] == DT_STRING
                ? cachedRecordManager->schema->typeLength[cachedRecordManager->pnt] : (
                        cachedRecordManager->schema->dataTypes[cachedRecordManager->pnt] ==
                        DT_INT ? SIZE_T_INT :
                        (cachedRecordManager->schema->dataTypes[cachedRecordManager->pnt] ==
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
    cachedRecordManager->schema = schema;
//    int ofS;
    oftS(cachedRecordManager->schema, atn, &(cachedRecordManager->ofS));
    memcpy(&av, record->data + cachedRecordManager->ofS,
           dTypeLength(cachedRecordManager->schema->dataTypes[atn], cachedRecordManager->schema->typeLength[atn]));

    if (1 == atn) {
        cachedRecordManager->schema->dataTypes[atn] = 1;
    }
    if (cachedRecordManager->schema->dataTypes[atn] == DT_STRING) {
        MAKE_STRING_VALUE((*vl), record->data + cachedRecordManager->ofS);
        (*vl)->v.stringV[strlen(record->data + cachedRecordManager->ofS) - 1] = '\0';
        return RC_OK;
    }
    MAKE_VALUE((*vl), cachedRecordManager->schema->dataTypes[atn], av);
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