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

RC evalExpr(Record *record, Schema *schema, Expr *expr, Value **out) {
    Value *lIn;
    Value *rIn;
    MAKE_VALUE(*out, DT_INT, -1);

    switch (expr->type) {
        case EXPR_OP: {
            Operator *op = expr->expr.op;
            bool twoArgs = (op->type != OP_BOOL_NOT);

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

    return success;
}

RC freeExpr(Expr *expr) {
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

    return success;
}

void freeVal(Value *val) {
    free(val);
}

