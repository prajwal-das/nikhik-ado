#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <ctype.h>

RcMngr *rcmngr;
RC Return_code;


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
}

//DONE
extern RC initRecordManager(void *mgmtData) {
    if (mgmtData == NULL)
        setup();
    return RC_OK;
}

extern RC shutdownRecordManager() {
    Return_code = RC_OK;
    while (Return_code == RC_OK) {
        int shut = shutdownBufferPool(&rcmngr->buff_pool);
        if (shut != Return_code)
            NULL;
        rcmngr = NULL;
        free(rcmngr);
        break;
    }
    return Return_code;
}

extern RC createTable(char *name, Schema *schema) {

    int r_size = sizeof(RcMngr);
    int ATR_SIZE = 15;
    rcmngr = (RcMngr *) malloc(r_size);
    int cntr = 0;
    char d[PAGE_SIZE];
    char *hpg = d;
    SM_FileHandle fh;
    while (initBufferPool(&rcmngr->buff_pool, name, 100, RS_LRU, NULL) == RC_OK) {

        *(int *) hpg = 0;
        hpg = hpg + sizeof(int);
        *(int *) hpg = 1;
        hpg = hpg + sizeof(int);
        if (PAGE_SIZE > 0) {
            *(int *) hpg = (*schema).numAttr;
            hpg = hpg + sizeof(int);
            *(int *) hpg = (*schema).keySize;
        }
        hpg = hpg + sizeof(int);
        break;
    }

    do {
        strncpy(hpg, (*schema).attrNames[cntr], ATR_SIZE);
        hpg = hpg + ATR_SIZE;
        *(int *) hpg = (int) (*schema).dataTypes[cntr];
        if (TRUE) {
            hpg = hpg + sizeof(int);
            *(int *) hpg = (int) (*schema).typeLength[cntr];
        }
        cntr++;
        hpg = hpg + sizeof(int);

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
        rel->mgmtData = rcmngr;
        while (sizeof(int) > 0) {
            pinPage(&rcmngr->buff_pool, &rcmngr->pg_hndl, 0);
            pg_hndl = (char *) (*rcmngr).pg_hndl.data;
            rcmngr->t_count = *(int *) pg_hndl;
            break;
        }
        pg_hndl = pg_hndl + sizeof(int);

        if (sizeof(int) != 0) {
            rcmngr->freePg = *(int *) pg_hndl;
            pg_hndl = pg_hndl + sizeof(int);
        }
        a_count = *(int *) pg_hndl;
        pg_hndl = pg_hndl + sizeof(int);
        Schema *sch = (Schema *) malloc(sizeof(Schema));
        while (sizeof(Schema) != 0) {
            sch->dataTypes = (DataType *) malloc(sizeof(DataType) * a_count);
            sch->attrNames = (char **) malloc(sizeof(char *) * a_count);
            sch->numAttr = a_count;
            break;
        }
        sch->typeLength = (int *) malloc(sizeof(int) * a_count);
        for (; cnt < a_count; cnt++)
            sch->attrNames[cnt] = (char *) malloc(ATR_SIZE);

        cnt = 0;
        do {
            strncpy(sch->attrNames[cnt], pg_hndl, ATR_SIZE);
            pg_hndl = pg_hndl + ATR_SIZE;
            if (cnt < sch->numAttr) {
                sch->dataTypes[cnt] = *(int *) pg_hndl;
                pg_hndl = sizeof(int) + pg_hndl;
                sch->typeLength[cnt] = *(int *) pg_hndl;
            } else
                break;
            pg_hndl = sizeof(int) + pg_hndl;
            cnt++;

        } while (cnt < sch->numAttr);

        rel->schema = sch;
        if (unpinPage(&rcmngr->buff_pool, &rcmngr->pg_hndl) == RC_OK)
            forcePage(&rcmngr->buff_pool, &rcmngr->pg_hndl);
        return RC_OK;
    }
    return RC_PINNED_PAGES_IN_BUFFER;
}

extern RC closeTable(RM_TableData *rel) {
    RcMngr *rcmngr = rel->mgmtData;
    if (rel->mgmtData != NULL)
        shutdownBufferPool(&rcmngr->buff_pool);
    return RC_OK;
}

extern RC deleteTable(char *name) {
    int res = destroyPageFile(name);
    return res;
}

extern int getNumTuples(RM_TableData *rel) {
    RcMngr *rcmngr = rel->mgmtData;
    int cnt = (*rcmngr).t_count;
    return cnt > 0 ? cnt : 0;
}


extern RC insertRecord(RM_TableData *rel, Record *record) {
    RcMngr *rcmngr = rel->mgmtData;
    RID *r_ID = &record->id;

    char *d, *SLOC;
    int rec_size = 0;
    while (rec_size == 0) {

        rec_size = getRecordSize(rel->schema);
        r_ID->page = rcmngr->freePg;
        pinPage(&rcmngr->buff_pool, &rcmngr->pg_hndl, r_ID->page);
    }
    d = rcmngr->pg_hndl.data;
    r_ID->slot = findFreeSlot(d, rec_size);
    while ((r_ID->slot < 0) && (r_ID->slot == -1)) {
        unpinPage(&rcmngr->buff_pool, &rcmngr->pg_hndl);
        int pageNum = r_ID->page + 1;
        r_ID->page = pageNum;
        pinPage(&rcmngr->buff_pool, &rcmngr->pg_hndl, pageNum);
        d = rcmngr->pg_hndl.data;
        r_ID->slot = findFreeSlot(d, rec_size);
    }
    if (d != NULL) {
        SLOC = d;
        markDirty(&rcmngr->buff_pool, &rcmngr->pg_hndl);
    }
    do {

        SLOC = SLOC + (r_ID->slot * rec_size);
        *SLOC = '+';
        memcpy(++SLOC, (*record).data + 1, rec_size - 1);

    } while (1 != 1);

    if (SLOC != d) {
        unpinPage(&rcmngr->buff_pool, &rcmngr->pg_hndl);
        rcmngr->t_count++;
    }
    pinPage(&rcmngr->buff_pool, &rcmngr->pg_hndl, 0);
    return RC_OK;
}


extern RC deleteRecord(RM_TableData *rel, RID id) {
    RcMngr *rcmngr = rel->mgmtData;
    Return_code = RC_OK;
    char *data;
    int pg_id = id.page, cntr = 0;
    while (rel->mgmtData != NULL) {

        pinPage(&rcmngr->buff_pool, &rcmngr->pg_hndl, pg_id);
        rcmngr->freePg = pg_id;
        if ((*rcmngr).pg_hndl.data != NULL) {
            data = (*rcmngr).pg_hndl.data;
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
            if (markDirty(&rcmngr->buff_pool, &rcmngr->pg_hndl) == RC_OK) {
                forcePage(&rcmngr->buff_pool, &rcmngr->pg_hndl);
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
        RcMngr *rcmngr = rel->mgmtData;
        pinPage(&rcmngr->buff_pool, &rcmngr->pg_hndl, record->id.page);
    }
    if (getRecordSize(rel->schema) > 0) {
        rel_size = getRecordSize(rel->schema);
    }
    RID id = (*record).id;
    d = (*rcmngr).pg_hndl.data;

    while (d != NULL) {

        d = d + (id.slot * rel_size);
        *d = plus;
        break;
    }
    memcpy(++d, (*record).data + 1, rel_size - 1);
    int res = markDirty(&rcmngr->buff_pool, &rcmngr->pg_hndl);
    if (res == RC_OK)
        unpinPage(&rcmngr->buff_pool, &rcmngr->pg_hndl);

    return res;
}


extern RC getRecord(RM_TableData *rel, RID id, Record *record) {

    bool flg = TRUE;
    int rec_size = rec_size = getRecordSize(rel->schema);
    while (flg) {

        RcMngr *rcmngr = rel->mgmtData;
        pinPage(&rcmngr->buff_pool, &rcmngr->pg_hndl, id.page);
        char *d = (*rcmngr).pg_hndl.data;
        d = d + (rec_size * id.slot);
        while (!(*d != '+')) {

            record->id = id;
            char *data = record->data;
            memcpy(++data, d + 1, rec_size - 1);
            unpinPage(&rcmngr->buff_pool, &rcmngr->pg_hndl);
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
            if (sizeof(RcMngr) > 0) {
                scn_mgr = (RcMngr *) malloc(sizeof(RcMngr));
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

                Value *result = (Value *) malloc(sizeof(Value));

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
    RcMngr *rcmngr = (*scan).rel->mgmtData;
    Return_code = RC_OK;
    int zero = 0;

    while ((scnMgr->scn_count != 0) && (!(scnMgr->scn_count < 0))) {
        if (unpinPage(&rcmngr->buff_pool, &scnMgr->pg_hndl) == RC_OK) {
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


extern int getRecordSize(Schema *schema) {

    int cntr = 0, size = 0;

    do {
        switch (schema->dataTypes[cntr]) {
            case DT_STRING :
                size = size + schema->typeLength[cntr];
                break;
            case DT_INT :
                size = size + sizeof(int);
                break;
            case DT_FLOAT :
                size = size + sizeof(float);
                break;
            case DT_BOOL :
                size = size + sizeof(bool);
                break;
        }
        cntr++;
    } while (cntr < schema->numAttr);
    size = size + 1;
    return size;
}


extern Schema *
createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys) {
    Schema *schema;

    if (sizeof(Schema) > 0) {
        schema = (Schema *) malloc(sizeof(Schema));
        schema->numAttr = numAttr;
        schema->attrNames = attrNames != NULL ? attrNames : NULL;
        schema->dataTypes = dataTypes != NULL ? dataTypes : NULL;
        schema->typeLength = typeLength != NULL ? typeLength : 0;
        schema->keySize = keySize;
        schema->keyAttrs = keys;
    }
    return schema;
}

extern RC freeSchema(Schema *schema) {
    free(schema);
    return RC_OK;
}

extern RC createRecord(Record **record, Schema *schema) {
    char minus = '-', blank = '\0';
    while (sizeof(Record) > 0) {

        Record *newRecord = (Record *) malloc(sizeof(Record));
        newRecord->data = (char *) malloc(getRecordSize(schema));
        newRecord->id.page = newRecord->id.slot = -1;
        if (newRecord != NULL) {
            char *dp = newRecord->data;
            *dp = minus;
            *(++dp) = blank;
        }
        *record = newRecord;
        return RC_OK;
    }
    return RC_ERROR;
}

RC attrOffset(Schema *schema, int attrNum, int *result) {
    int cntr = 0;
    *result = 1;
    while (cntr < attrNum) {

        *result =
                schema->dataTypes[cntr] == DT_STRING ? *result + schema->typeLength[cntr] : (schema->dataTypes[cntr] ==
                                                                                             DT_INT ? *result +
                                                                                                      sizeof(int) :
                                                                                             (schema->dataTypes[cntr] ==
                                                                                              DT_FLOAT ? *result +
                                                                                                         sizeof(float) :
                                                                                              *result + sizeof(bool)));
        cntr++;
    }
    return RC_OK;
}

extern RC freeRecord(Record *record) {
    do {
        free(record);
    } while (FALSE);
    return RC_OK;

}

extern RC getAttr(Record *record, Schema *schema, int attrNum, Value **value) {
    Return_code = RC_OK;
    int offset = 0;
    attrOffset(schema, attrNum, &offset);
    char *dataPointer = (*record).data;
    Value *attribute = (Value *) malloc(sizeof(Value));
    if (true)
        dataPointer = dataPointer + offset;
    schema->dataTypes[attrNum] = (attrNum == 1) ? 1 : schema->dataTypes[attrNum];
    switch (schema->dataTypes[attrNum]) {
        case DT_STRING :
            if (true) {
                int length = schema->typeLength[attrNum];
                (*attribute).v.stringV = (char *) malloc(length + 1);
                if (true)
                    strncpy((*attribute).v.stringV, dataPointer, length);
                (*attribute).dt = DT_STRING;
                (*attribute).v.stringV[length] = '\0';
            }
            break;
        case DT_INT:
            if (true) {
                int value = 0;
                if (true)
                    memcpy(&value, dataPointer, sizeof(int));
                (*attribute).dt = DT_INT;
                (*attribute).v.intV = value;
            }
            break;
        case DT_FLOAT:
            if (true) {
                float value;
                if (true)
                    memcpy(&value, dataPointer, sizeof(float));
                (*attribute).dt = DT_FLOAT;
                (*attribute).v.floatV = value;
            }
            break;
        case DT_BOOL:
            if (true) {
                bool value;
                if (true)
                    memcpy(&value, dataPointer, sizeof(bool));
                (*attribute).dt = DT_BOOL;
                (*attribute).v.boolV = value;
            }
            break;
    }
    *value = attribute;
    return Return_code;
}

extern RC setAttr(Record *record, Schema *schema, int attrNum, Value *value) {
    int offset = 0;
    char *dp;
    Return_code = RC_OK;
    while (attrOffset(schema, attrNum, &offset) == Return_code) {
        dp = record->data;
        dp = dp + offset;
        if (dp != NULL && (schema->dataTypes[attrNum]) == DT_STRING) {
            strncpy(dp, (*value).v.stringV, schema->typeLength[attrNum]);
            dp = dp + schema->typeLength[attrNum];
        } else if (dp != NULL && (schema->dataTypes[attrNum]) == DT_INT) {
            *(int *) dp = value->v.intV;
            dp = dp + sizeof(int);
        } else if (dp != NULL && (schema->dataTypes[attrNum]) == DT_FLOAT) {
            *(float *) dp = value->v.floatV;
            dp = dp + sizeof(float);
        } else if (dp != NULL && (schema->dataTypes[attrNum]) == DT_BOOL) {
            *(bool *) dp = value->v.boolV;
            dp = dp + sizeof(bool);
        }
        break;
    }
    return Return_code;
}