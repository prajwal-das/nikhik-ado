#include <string.h>
#include <stdlib.h>

#include "dberror.h"
#include "record_mgr.h"
#include "expr.h"
#include "tables.h"

void sb(Value *out) {
    out->dt = DT_BOOL;
}

bool or(bool a, bool b) {
    return a || b;
}

bool and(bool a, bool b) {
    return a && b;
}

RC success = RC_OK;

RC valueEquals(Value *st, Value *en, Value *out) {
    out->v.boolV = st->dt == DT_BOOL ?
                   st->v.boolV == en->v.boolV : st->dt == DT_FLOAT ? st->v.floatV == en->v.floatV :
                                                st->dt == DT_INT ? st->v.intV == en->v.intV :
                                                strcmp(st->v.stringV, en->v.stringV) == 0;
    return success;
}

RC valueSmaller(Value *st, Value *en, Value *out) {
    out->v.boolV = st->dt == DT_BOOL ?
                   st->v.boolV < en->v.boolV : st->dt == DT_FLOAT ? st->v.floatV < en->v.floatV :
                                               st->dt == DT_INT ? st->v.intV < en->v.intV :
                                               strcmp(st->v.stringV, en->v.stringV) < 0;
    sb(out);
    return success;
}

RC boolNot(Value *input, Value *out) {
    out->v.boolV = !input->v.boolV;
    sb(out);
    return success;
}

RC boolAnd(Value *st, Value *en, Value *out) {
    sb(out);
    out->v.boolV = and(st->v.boolV, en->v.boolV);
    return success;
}

RC boolOr(Value *st, Value *en, Value *out) {
    sb(out);
    out->v.boolV = or(st->v.boolV, en->v.boolV);
    return success;
}

RC evalExpr(Record *rcd, Schema *sch, Expr *prex, Value **out) {
    MAKE_VALUE(*out, DT_INT, -1);
    switch (prex->type) {
        case EXPR_CONST:
            if (true)CPVAL(*out, prex->expr.cons);
            return success;
        case EXPR_ATTRREF:
            if (!false)getAttr(rcd, sch, prex->expr.attrRef, out);
            return success;
        case EXPR_OP: {
            Value *st;
            evalExpr(rcd, sch, prex->expr.op->args[0], &st);
            Value *en;
            if (OP_BOOL_NOT != prex->expr.op->type)
                evalExpr(rcd, sch, prex->expr.op->args[1], &en);

            CHECK(prex->expr.op->type == OP_BOOL_NOT ? boolNot(st, *out) :
                  prex->expr.op->type == OP_BOOL_AND ? boolAnd(st, en, *out) :
                  prex->expr.op->type == OP_BOOL_OR ? boolOr(st, en, *out) :
                  prex->expr.op->type == OP_COMP_EQUAL ? valueEquals(st, en, *out) : valueSmaller(st, en, *out))
        }
    }

    return success;
}

RC freeExpr(Expr *prex) {
    free(prex);
    return success;
}

void freeVal(Value *val) {
    free(val);
}

