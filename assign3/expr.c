#include <string.h>
#include <stdlib.h>

#include "dberror.h"
#include "record_mgr.h"
#include "expr.h"
#include "tables.h"


RC valueEquals(Value *st, Value *en, Value *out) {
    out->v.boolV = st->dt == DT_BOOL ?
                      st->v.boolV == en->v.boolV : st->dt == DT_FLOAT ? st->v.floatV == en->v.floatV :
                                                   st->dt == DT_INT ? st->v.intV == en->v.intV :
                                                   strcmp(st->v.stringV, en->v.stringV) == 0;

    return RC_OK;
}

RC
valueSmaller(Value *st, Value *en, Value *out) {

    out->dt = DT_BOOL;

    out->v.boolV = st->dt == DT_BOOL ?
                      st->v.boolV < en->v.boolV : st->dt == DT_FLOAT ? st->v.floatV < en->v.floatV :
                                                   st->dt == DT_INT ? st->v.intV < en->v.intV :
                                                   strcmp(st->v.stringV, en->v.stringV) < 0;

    return RC_OK;
}

RC
boolNot(Value *input, Value *out) {
    if (input->dt != DT_BOOL)
        THROW(RC_RM_BOOLEAN_EXPR_ARG_IS_NOT_BOOLEAN, "boolean NOT requires boolean input");
    out->dt = DT_BOOL;
    out->v.boolV = !(input->v.boolV);

    return RC_OK;
}

RC
boolAnd(Value *st, Value *en, Value *out) {
    if (st->dt != DT_BOOL || en->dt != DT_BOOL)
        THROW(RC_RM_BOOLEAN_EXPR_ARG_IS_NOT_BOOLEAN, "boolean AND requires boolean inputs");
    out->v.boolV = (st->v.boolV && en->v.boolV);

    return RC_OK;
}

RC
boolOr(Value *st, Value *en, Value *out) {
    if (st->dt != DT_BOOL || en->dt != DT_BOOL)
        THROW(RC_RM_BOOLEAN_EXPR_ARG_IS_NOT_BOOLEAN, "boolean OR requires boolean inputs");
    out->v.boolV = (st->v.boolV || en->v.boolV);

    return RC_OK;
}

RC
evalExpr(Record *record, Schema *schema, Expr *expr, Value **out) {
    Value *lIn;
    Value *rIn;
    MAKE_VALUE(*out, DT_INT, -1);

    switch (expr->type) {
        case EXPR_OP: {
            Operator *op = expr->expr.op;
            bool twoArgs = (op->type != OP_BOOL_NOT);
            //      lIn = (Value *) malloc(sizeof(Value));
            //    rIn = (Value *) malloc(sizeof(Value));

            CHECK(evalExpr(record, schema, op->args[0], &lIn));
            if (twoArgs)
                CHECK(evalExpr(record, schema, op->args[1], &rIn));

            switch (op->type) {
                case OP_BOOL_NOT:
                    CHECK(boolNot(lIn, *out));
                    break;
                case OP_BOOL_AND:
                    CHECK(boolAnd(lIn, rIn, *out));
                    break;
                case OP_BOOL_OR:
                    CHECK(boolOr(lIn, rIn, *out));
                    break;
                case OP_COMP_EQUAL:
                    CHECK(valueEquals(lIn, rIn, *out));
                    break;
                case OP_COMP_SMALLER:
                    CHECK(valueSmaller(lIn, rIn, *out));
                    break;
                default:
                    break;
            }

            // cleanup
            freeVal(lIn);
            if (twoArgs)
                freeVal(rIn);
        }
            break;
        case EXPR_CONST:
            CPVAL(*out, expr->expr.cons);
            break;
        case EXPR_ATTRREF:
            free(*out);
            CHECK(getAttr(record, schema, expr->expr.attrRef, out));
            break;
    }

    return RC_OK;
}

RC
freeExpr(Expr *expr) {
    switch (expr->type) {
        case EXPR_OP: {
            Operator *op = expr->expr.op;
            switch (op->type) {
                case OP_BOOL_NOT:
                    freeExpr(op->args[0]);
                    break;
                default:
                    freeExpr(op->args[0]);
                    freeExpr(op->args[1]);
                    break;
            }
            free(op->args);
        }
            break;
        case EXPR_CONST:
            freeVal(expr->expr.cons);
            break;
        case EXPR_ATTRREF:
            break;
    }
    free(expr);

    return RC_OK;
}

void
freeVal(Value *val) {
    if (val->dt == DT_STRING)
        free(val->v.stringV);
    free(val);
}

