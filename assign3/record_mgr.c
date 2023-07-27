#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <ctype.h>

typedef struct CacheRecordManager {
    RcMngr *rcmngr;
    int pnt;
    Schema *schema;
    int ofS;
    Record *record
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

RC Return_code;

RC makeSpace(void *var) {
    free(var);
    return RC_OK;
}

// new
int findFreeSlot(char *data, int recordSize) {
    int cntr = 0;
    int slot = -1;
    bool flg = FALSE;
    do {

        for (; cntr < PAGE_SIZE / recordSize; cntr++) {
            int indx = cntr * recordSize;
            flg = data[indx] == '+' ? flg : TRUE;
            slot = data[indx] == '+' ? -1 : cntr;
            if (slot > -1 && flg)
                return slot;
        }
        break;
    } while (recordSize > 0);

    return slot;
}

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

extern RC createTable(char *name, Schema *schema) {

    int ATR_SIZE = 15;
    char d[PAGE_SIZE];
    char *hpg = d;
    SM_FileHandle fh;
    initBufferPool(&(cachedRecordManager->rcmngr->buff_pool), name, 100, RS_LRU, NULL);

    *(int *) hpg = 0;
    hpg = hpg + SIZE_T_INT;
    *(int *) hpg = 1;
    hpg = hpg + SIZE_T_INT;
    if (PAGE_SIZE > 0) {
        *(int *) hpg = (*schema).numAttr;
        hpg = hpg + SIZE_T_INT;
        *(int *) hpg = (*schema).keySize;
    }
    hpg = hpg + SIZE_T_INT;

    int cntr = 0;
    do {
        strncpy(hpg, (*schema).attrNames[cntr], ATR_SIZE);
        hpg = hpg + ATR_SIZE;
        *(int *) hpg = (int) (*schema).dataTypes[cntr];
        if (TRUE) {
            hpg = hpg + SIZE_T_INT;
            *(int *) hpg = (int) (*schema).typeLength[cntr];
        }
        cntr++;
        hpg = hpg + SIZE_T_INT;

    } while (cntr < schema->numAttr);

    if (createPageFile(name) == RC_OK)
        openPageFile(name, &fh);

    if (writeBlock(0, &fh, d) == RC_OK)
        closePageFile(&fh);

    return RC_OK;
}

extern RC openTable(RM_TableData *rel, char *name) {
    int ATR_SIZE = 15;
    if (TRUE) {

        SM_PageHandle pg_hndl;
        int a_count;
        rel->name = name;
        int cnt = 0;
        rel->mgmtData = cachedRecordManager->rcmngr;
        while (SIZE_T_INT > 0) {
            pinPage(&(cachedRecordManager->rcmngr->buff_pool), &(cachedRecordManager->rcmngr->pg_hndl), 0);
            pg_hndl = (char *) cachedRecordManager->rcmngr->pg_hndl.data;
            cachedRecordManager->rcmngr->t_count = *(int *) pg_hndl;
            break;
        }
        pg_hndl = pg_hndl + SIZE_T_INT;

        if (SIZE_T_INT != 0) {
            cachedRecordManager->rcmngr->freePg = *(int *) pg_hndl;
            pg_hndl = pg_hndl + SIZE_T_INT;
        }
        a_count = *(int *) pg_hndl;
        pg_hndl = pg_hndl + SIZE_T_INT;
        Schema *sch = (Schema *) malloc(SIZE_T_SCHEMA);
        while (SIZE_T_SCHEMA != 0) {
            sch->dataTypes = (DataType *) malloc(SIZE_T_DATATYPE * a_count);
            sch->attrNames = (char **) malloc(SIZE_T_CHAR * a_count);
            sch->numAttr = a_count;
            break;
        }
        sch->typeLength = (int *) malloc(SIZE_T_INT * a_count);
        for (; cnt < a_count; cnt++)
            sch->attrNames[cnt] = (char *) malloc(ATR_SIZE);

        cnt = 0;
        do {
            strncpy(sch->attrNames[cnt], pg_hndl, ATR_SIZE);
            pg_hndl = pg_hndl + ATR_SIZE;
            if (cnt < sch->numAttr) {
                sch->dataTypes[cnt] = *(int *) pg_hndl;
                pg_hndl = SIZE_T_INT + pg_hndl;
                sch->typeLength[cnt] = *(int *) pg_hndl;
            } else
                break;
            pg_hndl = SIZE_T_INT + pg_hndl;
            cnt++;

        } while (cnt < sch->numAttr);

        rel->schema = sch;
        if (unpinPage(&(cachedRecordManager->rcmngr->buff_pool), &(cachedRecordManager->rcmngr->pg_hndl)) == RC_OK)
            forcePage(&(cachedRecordManager->rcmngr->buff_pool), &(cachedRecordManager->rcmngr->pg_hndl));
        return RC_OK;
    }
    return RC_PINNED_PAGES_IN_BUFFER;
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


extern RC insertRecord(RM_TableData *rel, Record *record) {
    cachedRecordManager->rcmngr = rel->mgmtData;
    RID *r_ID = &record->id;

    char *d, *SLOC;
    int rec_size = 0;
    while (rec_size == 0) {

        rec_size = getRecordSize(rel->schema);
        r_ID->page = cachedRecordManager->rcmngr->freePg;
        pinPage(&(cachedRecordManager->rcmngr->buff_pool), &(cachedRecordManager->rcmngr->pg_hndl), r_ID->page);
    }
    d = cachedRecordManager->rcmngr->pg_hndl.data;
    r_ID->slot = findFreeSlot(d, rec_size);
    while ((r_ID->slot < 0) && (r_ID->slot == -1)) {
        unpinPage(&(cachedRecordManager->rcmngr->buff_pool), &(cachedRecordManager->rcmngr->pg_hndl));
        int pageNum = r_ID->page + 1;
        r_ID->page = pageNum;
        pinPage(&(cachedRecordManager->rcmngr->buff_pool), &(cachedRecordManager->rcmngr->pg_hndl), pageNum);
        d = cachedRecordManager->rcmngr->pg_hndl.data;
        r_ID->slot = findFreeSlot(d, rec_size);
    }
    if (d != NULL) {
        SLOC = d;
        markDirty(&(cachedRecordManager->rcmngr->buff_pool), &(cachedRecordManager->rcmngr->pg_hndl));
    }
    do {

        SLOC = SLOC + (r_ID->slot * rec_size);
        *SLOC = '+';
        memcpy(++SLOC, (*record).data + 1, rec_size - 1);

    } while (1 != 1);

    if (SLOC != d) {
        unpinPage(&(cachedRecordManager->rcmngr->buff_pool), &(cachedRecordManager->rcmngr->pg_hndl));
        cachedRecordManager->rcmngr->t_count++;
    }
    pinPage(&(cachedRecordManager->rcmngr->buff_pool), &(cachedRecordManager->rcmngr->pg_hndl), 0);
    return RC_OK;
}


extern RC deleteRecord(RM_TableData *rel, RID id) {
    cachedRecordManager->rcmngr = rel->mgmtData;
    Return_code = RC_OK;
    char *data;
    int pg_id = id.page, cntr = 0;
    while (rel->mgmtData != NULL) {

        pinPage(&(cachedRecordManager->rcmngr->buff_pool), &(cachedRecordManager->rcmngr->pg_hndl), pg_id);
        cachedRecordManager->rcmngr->freePg = pg_id;
        if (cachedRecordManager->rcmngr->pg_hndl.data != NULL) {
            data = cachedRecordManager->rcmngr->pg_hndl.data;
        }
        int rec_size = getRecordSize(rel->schema);
        char minus = '-';
        TOP :
        data[(id.slot * rec_size) + cntr] = minus;
        if (cntr < rec_size) {
            cntr++;
            goto TOP;
        }

        if (data != NULL) {
            if (markDirty(&(cachedRecordManager->rcmngr->buff_pool), &(cachedRecordManager->rcmngr->pg_hndl)) ==
                RC_OK) {
                forcePage(&(cachedRecordManager->rcmngr->buff_pool), &(cachedRecordManager->rcmngr->pg_hndl));
            }
        }
        break;
    }
    return RC_OK;
}


extern RC updateRecord(RM_TableData *rel, Record *record) {

    char *d;
    int rel_size = 0;
    char plus = '+';
    if (rel->mgmtData == NULL)
        return RC_ERROR;
    else {
        cachedRecordManager->rcmngr = rel->mgmtData;
        pinPage(&(cachedRecordManager->rcmngr->buff_pool), &(cachedRecordManager->rcmngr->pg_hndl), record->id.page);
    }
    if (getRecordSize(rel->schema) > 0) {
        rel_size = getRecordSize(rel->schema);
    }
    RID id = (*record).id;
    d = cachedRecordManager->rcmngr->pg_hndl.data;

    while (d != NULL) {

        d = d + (id.slot * rel_size);
        *d = plus;
        break;
    }
    memcpy(++d, (*record).data + 1, rel_size - 1);
    int res = markDirty(&(cachedRecordManager->rcmngr->buff_pool), &(cachedRecordManager->rcmngr->pg_hndl));
    if (res == RC_OK)
        unpinPage(&(cachedRecordManager->rcmngr->buff_pool), &(cachedRecordManager->rcmngr->pg_hndl));

    return res;
}


extern RC getRecord(RM_TableData *rel, RID id, Record *record) {

    bool flg = TRUE;
    int rec_size = rec_size = getRecordSize(rel->schema);
    while (flg) {

        cachedRecordManager->rcmngr = rel->mgmtData;
        pinPage(&(cachedRecordManager->rcmngr->buff_pool), &(cachedRecordManager->rcmngr->pg_hndl), id.page);
        char *d = cachedRecordManager->rcmngr->pg_hndl.data;
        d = d + (rec_size * id.slot);
        while (!(*d != '+')) {

            record->id = id;
            char *data = record->data;
            memcpy(++data, d + 1, rec_size - 1);
            unpinPage(&(cachedRecordManager->rcmngr->buff_pool), &(cachedRecordManager->rcmngr->pg_hndl));
            return RC_OK;
            break;
        }
        return RC_RM_NO_TUPLE_WITH_GIVEN_RID;
        flg = FALSE;
    }

    return RC_ERROR;
}


extern RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond) {
    int ATR_SIZE = 15;
    while (cond != NULL) {

        RcMngr *scn_mgr;
        RcMngr *tbl_mgr;
        int zero = 0;
        if (openTable(rel, "ScanTable") == RC_OK) {
            if (SIZE_T_RCMNGR > 0) {
                scn_mgr = (RcMngr *) malloc(SIZE_T_RCMNGR);
                scan->mgmtData = scn_mgr;

                scn_mgr->cond = cond;
                scn_mgr->rec_ID.slot = zero;
            }
            scn_mgr->rec_ID.page = 1;
            scn_mgr->scn_count = zero;
            if (scn_mgr != NULL) {
                tbl_mgr = (*rel).mgmtData;
                tbl_mgr->t_count = ATR_SIZE;
            }
            scan->rel = rel;
        }
        return RC_OK;
        break;
    }
    return RC_SCAN_CONDITION_NOT_FOUND;
}

extern RC next(RM_ScanHandle *scan, Record *record) {
    Return_code = RC_OK;
    if ((*scan).rel->mgmtData != NULL) {
        RcMngr *scnMgr = (*scan).mgmtData;
        if (scnMgr->cond != NULL) {

            RcMngr *tableManager = (*scan).rel->mgmtData;
            char *dt;

            if ((*scan).rel->mgmtData != NULL) {

                Schema *schema = (*scan).rel->schema;

                if ((*scnMgr).cond == NULL)
                    return RC_SCAN_CONDITION_NOT_FOUND;

                Value *result = (Value *) malloc(SIZE_T_VALUE);

                int tot_slots = PAGE_SIZE / getRecordSize(schema);
                int sc_count = 0;
                sc_count = (*scnMgr).scn_count;
                int count_tp = (*tableManager).t_count;

                if (count_tp != 0) {
                    while (sc_count <= count_tp) {
                        if (!(sc_count > 0)) {
                            (*scnMgr).rec_ID.slot = 0;
                            (*scnMgr).rec_ID.page = 1;
                        } else {
                            (*scnMgr).rec_ID.slot++;
                            if ((*scnMgr).rec_ID.slot >= tot_slots) {
                                if (sc_count > 0)
                                    (*scnMgr).rec_ID.page++;
                                (*scnMgr).rec_ID.slot = 0;
                            }
                        }
                        int count = 1, count1 = 0;
                        if (count == 1) {
                            pinPage(&tableManager->buff_pool, &scnMgr->pg_hndl, (*scnMgr).rec_ID.page);
                            count1 = 1;
                        }

                        if (count1 == 1) {
                            dt = (*scnMgr).pg_hndl.data;
                            if (true)
                                dt = dt + ((*scnMgr).rec_ID.slot * getRecordSize(schema));
                            (*record).id.slot = (*scnMgr).rec_ID.slot;
                            if (true)
                                (*record).id.page = (*scnMgr).rec_ID.page;
                            char *dataPointer = (*record).data;
                            *dataPointer = '-';
                            memcpy(++dataPointer, dt + 1, getRecordSize(schema) - 1);
                        }

                        sc_count++;
                        if (true)
                            (*scnMgr).scn_count++;
                        evalExpr(record, schema, (*scnMgr).cond, &result);
                        if ((*result).v.boolV == TRUE) {
                            if (true)
                                unpinPage(&tableManager->buff_pool, &scnMgr->pg_hndl);
                            return RC_OK;
                        }
                    }

                    int count = 1, count1 = 0;
                    unpinPage(&tableManager->buff_pool, &scnMgr->pg_hndl);
                    if (count == 1) {
                        scnMgr->rec_ID.page = 1;
                        count1 = 1;
                    }

                    if (count1 == 1) {
                        (*scnMgr).rec_ID.slot = 0;
                    }

                    if (count1 == 1) {
                        (*scnMgr).scn_count = 0;
                    }

                    return RC_RM_NO_MORE_TUPLES;
                } else {
                    return RC_RM_NO_MORE_TUPLES;
                }
            }
        } else {
            return RC_SCAN_CONDITION_NOT_FOUND;
        }
    } else {
        return RC_SCAN_CONDITION_NOT_FOUND;
    }

    return RC_RM_NO_MORE_TUPLES;
}

extern RC closeScan(RM_ScanHandle *scan) {
    RcMngr *scnMgr = scan->mgmtData != NULL ? scan->mgmtData : NULL;
    cachedRecordManager->rcmngr = (*scan).rel->mgmtData;
    Return_code = RC_OK;
    int zero = 0;

    while ((scnMgr->scn_count != 0) && (!(scnMgr->scn_count < 0))) {
        if (unpinPage(&(cachedRecordManager->rcmngr->buff_pool), &scnMgr->pg_hndl) == RC_OK) {
            scnMgr->scn_count = zero;
            scnMgr->rec_ID.page = 1;
            scnMgr->rec_ID.slot = zero;
        }
        scan->mgmtData = NULL;
        free(scan->mgmtData);
        break;
    }
    return Return_code;
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