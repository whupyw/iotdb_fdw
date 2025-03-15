
#include "include/iotdb_fdw.h"
#include "postgres.h"
//#include "fmgr.h"
#include "catalog/pg_operator.h"
#include "datatype/timestamp.h"
#include "foreign/foreign.h"

#include "miscadmin.h"
#include "nodes/bitmapset.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pathnodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "parser/parser.h"
#include "parser/parsetree.h"
#include "postgres.h"
#include "postgres_ext.h"
#include <string.h>
#include <sys/types.h>

#include "lib/stringinfo.h"

#include "storage/lockdefs.h"

#include "utils/elog.h"
#include "utils/fmgrprotos.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

#include "access/htup.h"
#include "access/table.h"
#include "access/tupdesc.h"

#include "catalog/pg_attribute.h"
#include "catalog/pg_type_d.h"

#include "commands/defrem.h"
#include "utils/timestamp.h"
#include "varatt.h"

PG_MODULE_MAGIC;
#define QUOTE '"'

typedef enum {
  FDW_COLLATE_NONE,   /* expression is of a noncollatable type */
  FDW_COLLATE_SAFE,   /* collation derives from a foreign Var */
  FDW_COLLATE_UNSAFE, /* collation derives from something else */
} FDWCollateState;

typedef struct foreign_glob_cxt {
  PlannerInfo *root;         // global planner state
  RelOptInfo *foreignrel;    // foreign relation
  Relids relids;             // relids of base relations in the underlying
  Oid relid;                 // relation oid
  uint mixing_aggref_Status; // mixing_aggref_status, but now we do not use
  bool for_tlist;            // whether evaluation for the expression of tlist
  bool is_inner_func;        // exist or not in inner exprs
} foreign_glob_cxt;

typedef struct foreign_loc_cxt {
  Oid collation;              /* OID of current collation, if any */
  FDWCollateState state;      /* state of current collation choice */
  bool can_skip_cast;         /* outer function can skip float8/numeric cast */
  bool can_pushdown_stable;   /* true if query contains stable
                               * function with star or regex */
  bool can_pushdown_volatile; /* true if query contains volatile
                               * function */
  bool influx_fill_enable;    /* true if deparse subexpression inside
                               * influx_time() */
  bool have_otherfunc_influx_time_tlist; /* true if having other functions than
                                            influx_time() in tlist. */
  bool has_time_key; /* mark true if comparison with time key column */
  bool has_sub_or_add_operator; /* mark true if expression has '-' or '+'
                                   operator */
  bool is_comparison;           /* mark true if has comparison */
} foreign_loc_cxt;

typedef enum {
  UNKNOWN_OPERATOR = 0,
  LIKE_OPERATOR,      /* LIKE case senstive */
  NOT_LIKE_OPERATOR,  /* NOT LIKE case sensitive */
  ILIKE_OPERATOR,     /* LIKE case insensitive */
  NOT_ILIKE_OPERATOR, /* NOT LIKE case insensitive */
  REGEX_MATCH_CASE_SENSITIVE_OPERATOR,
  REGEX_NOT_MATCH_CASE_SENSITIVE_OPERATOR,
  REGEX_MATCH_CASE_INSENSITIVE_OPERATOR,
  REGEX_NOT_MATCH_CASE_INSENSITIVE_OPERATOR
} PatternMatchingOperator;

typedef struct deparse_expr_cxt {
  PlannerInfo *root;      /* global planner state */
  RelOptInfo *foreignrel; /* the foreign relation we are planning for */
  RelOptInfo *scanrel;    /* the underlying scan relation. Same as
                           * foreignrel, when that represents a join or
                           * a base relation. */
  StringInfo buf;         /* output buffer to append to */
  List **params_list;     /* exprs that will become remote Params */
  PatternMatchingOperator op_type; /* Type of operator for pattern matching */
  bool is_tlist;                   /* deparse during target list exprs */
  bool can_skip_cast;         /* outer function can skip float8/numeric cast */
  bool can_delete_directly;   /* DELETE statement can pushdown
                               * directly */
  bool has_bool_cmp;          /* outer has bool comparison target */
  FuncExpr *influx_fill_expr; /* Store the fill() function */

  /*
   * For comparison with time key column, if its data type is timestamp with
   * time zone, need to convert to timestamp without time zone.
   */
  bool convert_to_timestamp;
} deparse_expr_cxt;

// static bool iotdb_foreign_expr_walker(Node *ndoe, foreign_glob_cxt *glob_cxt,
//                                       foreign_loc_cxt *outer_cxt)
// {

// }
static void iotdb_deparse_target_list(StringInfo buf, PlannerInfo *root,
                                      Index rtindex, Relation rel,
                                      Bitmapset *attrs_used,
                                      List **retrieved_attrs);

static void iotdb_deparse_select(List *tlist, List **retrieved_attrs,
                                 deparse_expr_cxt *context);

static void iotdb_deparse_column_ref(StringInfo buf, int varno, int varattno,
                                     Oid vartype, PlannerInfo *root,
                                     bool convert, bool *can_delete_directly);

static char *iotdb_quote_identifier(const char *s, char q);

static void iotdb_append_field_key(TupleDesc tupdesc, StringInfo buf,
                                   Index rtindex, PlannerInfo *root,
                                   bool first);

static void iotdb_deparse_select(List *tlist, List **retrieved_attrs,
                                 deparse_expr_cxt *context);

static void iotdb_deparse_from_expr(List *quals, deparse_expr_cxt *context);

static void iotdb_append_conditions(List *exprs, deparse_expr_cxt *context);

static void iotdb_deparse_from_expr_for_rel(StringInfo buf, PlannerInfo *root,
                                            RelOptInfo *foreignrel,
                                            bool use_alias, List **params_list);

static void iotdb_deparse_relation(StringInfo buf, Relation rel);

static void iotdb_deparse_expr(Expr *node, deparse_expr_cxt *context);

static bool iotdb_foreign_expr_walker(Node *node, foreign_glob_cxt *glob_cxt,
                                      foreign_loc_cxt *outer_cxt);

static void iotdb_deparse_var(Var *node, deparse_expr_cxt *context);

static void iotdb_print_remote_param(int paramindex, Oid paramtype,
                                     int32 paramtypmod,
                                     deparse_expr_cxt *context);

static void iotdb_print_remote_placeholder(Oid paramtype, int32 paramtypmod,
                                           deparse_expr_cxt *context);

static void iotdb_deparse_column_ref(StringInfo buf, int varno, int varattno,
                                     Oid vartype, PlannerInfo *root,
                                     bool convert, bool *can_delete_directly);

/// @todo
// static void iotdb_deparse_const(Const *node, deparse_expr_cxt *context,
//                                 int showtype);

PG_FUNCTION_INFO_V1(iotdb_fdw_handler);
PG_FUNCTION_INFO_V1(iotdb_fdw_version);

/*
 * Deparse SELECT statement for given relation into buf.
 *
 * tlist contains the list of desired columns to be fetched from foreign server.
 * For a base relation fpinfo->attrs_used is used to construct SELECT clause,
 * hence the tlist is ignored for a base relation.
 *
 * remote_conds is the list of conditions to be deparsed into the WHERE clause
 * (or, in the case of upper relations, into the HAVING clause).
 *
 * If params_list is not NULL, it receives a list of Params and other-relation
 * Vars used in the clauses; these values must be transmitted to the remote
 * server as parameter values.
 *
 * If params_list is NULL, we're generating the query for EXPLAIN purposes,
 * so Params and other-relation Vars should be replaced by dummy values.
 *
 * pathkeys is the list of pathkeys to order the result by.
 *
 * List of columns selected is returned in retrieved_attrs.
 */
void iotdb_deparse_select_stmt_for_rel(StringInfo buf, PlannerInfo *root,
                                       RelOptInfo *rel, List *tlist,
                                       List *remote_conds, List *pathkeys,
                                       bool is_subquery, List **retrieved_attrs,
                                       List **params_list, bool has_limit) {
  deparse_expr_cxt context;
  IotDBFdwRelationInfo *fpinfo = (IotDBFdwRelationInfo *)rel->fdw_private;
  List *quals;

  /*
   * We handle relations for foreign tables, joins between those and upper
   * relations.
   */
  Assert(rel->reloptkind == RELOPT_JOINREL ||
         rel->reloptkind == RELOPT_BASEREL ||
         rel->reloptkind == RELOPT_OTHER_MEMBER_REL ||
         rel->reloptkind == RELOPT_UPPER_REL);
  /* Fill portions of context common to upper, join and base relation */
  context.buf = buf;
  context.root = root;
  context.foreignrel = rel;
  context.scanrel =
      (rel->reloptkind == RELOPT_UPPER_REL) ? fpinfo->outerrel : rel;
  context.params_list = params_list;
  context.op_type = UNKNOWN_OPERATOR;
  context.is_tlist = false;
  context.can_skip_cast = false;
  context.convert_to_timestamp = false;
  context.has_bool_cmp = false;
  /* Construct SELECT clause */
  iotdb_deparse_select(tlist, retrieved_attrs, &context);

  /*
   * For upper relations, the WHERE clause is built from the remote
   * conditions of the underlying scan relation; otherwise, we can use the
   * supplied list of remote conditions directly.
   */
  if (rel->reloptkind == RELOPT_UPPER_REL) {
    IotDBFdwRelationInfo *ofpinfo;

    ofpinfo = (IotDBFdwRelationInfo *)fpinfo->outerrel->fdw_private;
    quals = ofpinfo->remote_conds;
  } else
    quals = remote_conds;

  /* Construct FROM and WHERE clauses */
  iotdb_deparse_from_expr(quals, &context);
  ///@todo not support GROUP BY and HAVING clause for now.
  // if (rel->reloptkind == RELOPT_UPPER_REL) {
  //   /* Append GROUP BY clause */
  //   iotdb_append_group_by_clause(tlist, &context);

  //   /* Append HAVING clause */
  //   if (remote_conds) {
  //     appendStringInfo(buf, " HAVING ");
  //     iotdb_append_conditions(remote_conds, &context);
  //   }
  // }

  /* Add ORDER BY clause if we found any useful pathkeys */
  // if (pathkeys)
  //   iotdb_append_order_by_clause(pathkeys, &context);

  /* Add LIMIT clause if necessary */
  // if (has_limit)
  //   iotdb_append_limit_clause(&context);
}

void iotdb_deparse_select(List *tlist, List **retrieved_attrs,
                          deparse_expr_cxt *context) {
  StringInfo buf = context->buf;
  PlannerInfo *root = context->root;
  RelOptInfo *foreignrel = context->foreignrel;
  IotDBFdwRelationInfo *fpinfo =
      (IotDBFdwRelationInfo *)foreignrel->fdw_private;
  RangeTblEntry *rte = NULL;
  // Construct SELECT list
  appendStringInfo(buf, "SELECT");

  if (foreignrel->reloptkind == RELOPT_JOINREL ||
      fpinfo->is_tlist_func_push_down == true ||
      foreignrel->reloptkind == RELOPT_UPPER_REL) {
    ereport(
        ERROR,
        (errmsg("join relation or upper relation is not supported now...")));
  } else {
    /*
     * For a base relation fpinfo->attrs_used gives the list of columns
     * required to be fetched from the foreign server.
     */
    rte = planner_rt_fetch(foreignrel->relid, root);

    /*
     * Core code already has some lock on each rel being planned, so we
     * can use NoLock here.
     */
    Relation rel = table_open(rte->relid, NoLock);

    iotdb_deparse_target_list(buf, root, foreignrel->relid, rel,
                              fpinfo->attrs_used, retrieved_attrs);
    table_close(rel, NoLock);
  }
}

/*
 * Emit a target list that retrieves the columns specified in attrs_used.
 * This is used for both SELECT and RETURNING targetlists.
 */
static void iotdb_deparse_target_list(StringInfo buf, PlannerInfo *root,
                                      Index rtindex, Relation rel,
                                      Bitmapset *attrs_used,
                                      List **retrieved_attrs) {
  TupleDesc tupdesc = RelationGetDescr(rel);
  bool first;
  bool have_wholerow;
  int i;
  bool need_field_key;
  RangeTblEntry *rte = NULL;
  have_wholerow =
      bms_is_member(0 - FirstLowInvalidHeapAttributeNumber, attrs_used);

  first = true;
  need_field_key = true;
  *retrieved_attrs = NIL;

  for (i = 1; i <= tupdesc->natts; i++) {
    Form_pg_attribute attr = TupleDescAttr(tupdesc, i - 1);

    // ignore dropped attributes
    if (attr->attisdropped)
      continue;

    if (have_wholerow ||
        bms_is_member(i - FirstLowInvalidHeapAttributeNumber, attrs_used)) {
      rte = planner_rt_fetch(rtindex, root);
      char *name = iotdb_get_column_name(rte->relid, i);

      if (!IOTDB_IS_TIME_COLUMN(name)) {
        if (!iotdb_is_tag_key(name, rte->relid)) {
          need_field_key = false;
        }
        if (!first)
          appendStringInfoString(buf, ", ");
        first = false;
        iotdb_deparse_column_ref(buf, rtindex, i, -1, root, false, false);
      }

      *retrieved_attrs = lappend_int(*retrieved_attrs, i);
    }
  }

  if (first) {
    appendStringInfoString(buf, " * ");
    return;
  }

  if (need_field_key) {
    iotdb_append_field_key(tupdesc, buf, rtindex, root, first);
  }
}

/*
 * influxdb_get_column_name
 * This function return name of column.
 */
char *iotdb_get_column_name(Oid relid, int attnum) {
  List *options = NULL;
  ListCell *lc_opt;
  char *column_name = NULL;

  options = GetForeignColumnOptions(relid, attnum);

  foreach (lc_opt, options) {
    DefElem *def = (DefElem *)lfirst(lc_opt);
    if (strcmp(def->defname, "column_name") == 0) {
      column_name = defGetString(def);
      break;
    }
  }

  if (column_name == NULL) {
    column_name = get_attname(relid, attnum, false);
  }
  return column_name;
}

// chech if column is tag key
bool iotdb_is_tag_key(const char *colname, Oid reloid) {
  iotdb_opt *options;
  ListCell *lc;

  // get fdw options
  options = iotdb_get_options(reloid, GetUserId());

  // if there is no tag in tags, then column is field
  if (!options->tag_list)
    return false;

  foreach (lc, options->tag_list) {
    char *tag_name = (char *)lfirst(lc);
    if (strcmp(colname, tag_name) == 0)
      return true;
  }
  return false;
}

static void iotdb_deparse_column_ref(StringInfo buf, int varno, int varattno,
                                     Oid vartype, PlannerInfo *root,
                                     bool convert, bool *can_delete_directly) {
  RangeTblEntry *rte = NULL;
  char *colname = NULL;

  Assert(!IS_SPECIAL_VARNO(varno));

  rte = planner_rt_fetch(varno, root);

  colname = iotdb_get_column_name(rte->relid, varattno);

  if (can_delete_directly)
    if (!IOTDB_IS_TIME_COLUMN(colname) &&
        !iotdb_is_tag_key(colname, rte->relid))
      *can_delete_directly = false;

  if (convert && vartype == BOOLOID)
    appendStringInfo(buf, "(%s=true)", colname);
  else {
    if (IOTDB_IS_TIME_COLUMN(colname))
      appendStringInfo(buf, "time");
    else
      appendStringInfoString(buf, iotdb_quote_identifier(colname, QUOTE));
  }
}

// ensure some special identifier is quoted
static char *iotdb_quote_identifier(const char *s, char q) {
  char *result = palloc(strlen(s) * 2 + 3);
  char *r = result;

  *r++ = q;
  while (*s) {
    if (*s == q)
      *r++ = *s;
    *r++ = *s;
    s++;
  }
  *r++ = q;
  *r++ = '\0';
  return result;
}

void iotdb_append_field_key(TupleDesc tupdesc, StringInfo buf, Index rtindex,
                            PlannerInfo *root, bool first) {
  int i;
  RangeTblEntry *rte = NULL;
  for (i = 1; i <= tupdesc->natts; i++) {
    Form_pg_attribute attr = TupleDescAttr(tupdesc, i - 1);
    rte = planner_rt_fetch(rtindex, root);
    char *name = iotdb_get_column_name(rte->relid, i);

    if (attr->attisdropped)
      continue;

    if (!IOTDB_IS_TIME_COLUMN(name) && !iotdb_is_tag_key(name, rte->relid)) {
      if (!first)
        appendStringInfoString(buf, ", ");
      iotdb_deparse_column_ref(buf, rtindex, i, -1, root, false, false);
      return;
    }
  }
}

void iotdb_deparse_from_expr(List *quals, deparse_expr_cxt *context) {
  StringInfo buf = context->buf;
  RelOptInfo *scanrel = context->scanrel;

  /* For upper relations, scanrel must be either a joinrel or a baserel */
  Assert(context->foreignrel->reloptkind != RELOPT_UPPER_REL ||
         scanrel->reloptkind == RELOPT_JOINREL ||
         scanrel->reloptkind == RELOPT_BASEREL);

  /* Construct FROM clause */
  appendStringInfoString(buf, " FROM ");
  iotdb_deparse_from_expr_for_rel(buf, context->root, scanrel,
                                  (bms_num_members(scanrel->relids) > 1),
                                  context->params_list);

  /* Construct WHERE clause */
  if (quals != NIL) {
    appendStringInfo(buf, " WHERE ");
    iotdb_append_conditions(quals, context);
  }
}

// WHERE
void iotdb_append_conditions(List *exprs, deparse_expr_cxt *context) {
  int nestlevel;
  ListCell *lc;
  bool is_first = true;
  StringInfo buf = context->buf;

  /* Make sure any constants in the exprs are printed portably */
  nestlevel = iotdb_set_transmission_modes();

  foreach (lc, exprs) {
    Expr *expr = (Expr *)lfirst(lc);

    /* Extract clause from RestrictInfo, if required */
    if (IsA(expr, RestrictInfo))
      expr = ((RestrictInfo *)expr)->clause;

    /* Connect expressions with "AND" and parenthesize each condition. */
    if (!is_first)
      appendStringInfoString(buf, " AND ");

    context->has_bool_cmp = true;

    appendStringInfoChar(buf, '(');
    ///@todo
    iotdb_deparse_expr(expr, context);
    appendStringInfoChar(buf, ')');

    context->has_bool_cmp = false;

    is_first = false;
  }

  iotdb_reset_transmisson_modes(nestlevel);
}

// Construct FROM clause for given relation
void iotdb_deparse_from_expr_for_rel(StringInfo buf, PlannerInfo *root,
                                     RelOptInfo *foreignrel, bool use_alias,
                                     List **params_list) {
  Assert(!use_alias);
  RangeTblEntry *rte = NULL;
  if (foreignrel->reloptkind == RELOPT_JOINREL) {
    Assert(false);
  } else {
    rte = planner_rt_fetch(foreignrel->relid, root);

    Relation rel = table_open(rte->relid, NoLock);
    iotdb_deparse_relation(buf, rel);

    table_close(rel, NoLock);
  }
}

// Append remote name of specified foreign table to buf.
void iotdb_deparse_relation(StringInfo buf, Relation rel) {
  char *relname = iotdb_get_table_name(rel);
  appendStringInfo(buf, "%s", relname);
}

void iotdb_deparse_expr(Expr *node, deparse_expr_cxt *context) {
  [[maybe_unused]] bool outer_can_skip_cast = context->can_skip_cast;
  [[maybe_unused]] bool outer_convert_to_timestamp =
      context->convert_to_timestamp;

  if (node == NULL)
    return;

  context->can_skip_cast = false;
  context->convert_to_timestamp = false;

  // for SELECT, we only support T_Var, T_Const, T_Param, T_OpExpr, T_BoolExpr
  switch (nodeTag(node)) {
  case T_Var:
    context->convert_to_timestamp = outer_convert_to_timestamp;
    iotdb_deparse_var((Var *)node, context);
    break;
  case T_Const:
    context->convert_to_timestamp = outer_convert_to_timestamp;
    ///@todo
    /// iotdb_deparse_const((Const *)node, context,0);
    break;
  case T_Param:
    ///@todo
    // iotdb_deparse_param((Param *)node, context);
    break;
  case T_OpExpr:
    context->convert_to_timestamp = outer_convert_to_timestamp;
    ///@todo
    // iotdb_deparse_op_expr((OpExpr *)node, context);
    break;
  case T_BoolExpr:
    ///@todo
    // iotdb_deparse_bool_expr((BoolExpr *)node, context);
    break;
  default:
    ereport(ERROR, (errmsg("unsupported expression type for SELECT: %d",
                           (int)nodeTag(node))));
    break;
  }
}

/*
 * Check if expression is safe to execute remotely, and return true if so.
 *
 * In addition, *outer_cxt is updated with collation information.
 *
 * We must check that the expression contains only node types we can deparse,
 * that all types/functions/operators are safe to send (which we approximate
 * as being built-in), and that all collations used in the expression derive
 * from Vars of the foreign table.  Because of the latter, the logic is
 * pretty close to assign_collations_walker() in parse_collate.c, though we
 * can assume here that the given expression is valid.
 */

bool iotdb_foreign_expr_walker(Node *node, foreign_glob_cxt *glob_cxt,
                               foreign_loc_cxt *loc_cxt) {
  [[maybe_unused]] bool check_type = true;
  [[maybe_unused]] foreign_loc_cxt *inner_loc_cxt = NULL;
  [[maybe_unused]] Oid collation;
  [[maybe_unused]] FDWCollateState collate_state;
  [[maybe_unused]] HeapTuple tuple;
  [[maybe_unused]] Form_pg_operator form;
  [[maybe_unused]] char *opname;
  [[maybe_unused]] static bool is_time_column = false;

  [[maybe_unused]] IotDBFdwRelationInfo *fpinfo =
      (IotDBFdwRelationInfo *)glob_cxt->foreignrel->fdw_private;

  if (node == NULL)
    return true;
  // set up inner_Cxt for possible recursion to child nodes
  inner_loc_cxt->collation = InvalidOid;
  inner_loc_cxt->state = FDW_COLLATE_NONE;
  inner_loc_cxt->can_skip_cast = false;
  inner_loc_cxt->can_pushdown_stable = false;
  inner_loc_cxt->can_pushdown_volatile = false;
  inner_loc_cxt->has_time_key = false;
  inner_loc_cxt->has_sub_or_add_operator = false;
  inner_loc_cxt->is_comparison = false;

  // not support pushdown
  switch (nodeTag(node)) {
  default:
    return false;
  }
}

// deparse given var node to context->buf
void iotdb_deparse_var(Var *node, deparse_expr_cxt *context) {
  StringInfo buf = context->buf;
  Relids relids = context->scanrel->relids;

  if (bms_is_member(node->varno, relids) && node->varlevelsup == 0) {
    bool convert = context->has_bool_cmp;

    ///@todo valuable belongs to foreign table
    iotdb_deparse_column_ref(buf, node->varno, node->varattno, node->vartype,
                             context->root, convert,
                             &context->can_delete_directly);
  } else {
    if (context->params_list) {
      int pindex = 0;
      ListCell *lc;

      foreach (lc, *context->params_list) {
        pindex++;
        if (equal(node, (Node *)lfirst(lc))) {
          break;
        }
      }
      if (lc == NULL) {
        pindex++;
        *context->params_list = lappend(*context->params_list, node);
      }
      ///@todo
      iotdb_print_remote_param(pindex, node->vartype, node->vartypmod, context);

    }

    else {
      ///@todo
      iotdb_print_remote_placeholder(node->vartype, node->vartypmod, context);
    }
  }
}

void iotdb_print_remote_param(int paramindex, Oid paramtype, int32 paramtypmod,
                              deparse_expr_cxt *context) {
  StringInfo buf = context->buf;
  appendStringInfo(buf, "$%d", paramindex);
}

void iotdb_print_remote_placeholder(Oid paramtype, int32 paramtypmod,
                                    deparse_expr_cxt *context) {
  StringInfo buf = context->buf;
  appendStringInfo(buf, "(SELECT null)");
}

// void
// iotdb_deparse_const(Const *node, deparse_expr_cxt *context, int showtype)
// {
//   StringInfo	buf = context->buf;
// 	Oid			typoutput;
// 	bool		typIsVarlena;
// 	char	   *extval;
// 	char	   *type_name;

//   if(node->constisnull)
//   {
//     appendStringInfoString(buf, "NULL");
//     return;
//   }

//   getTypeOutputInfo(node->ConstrType, &typOutput, &typIsVarlena);

//   switch(node->consttype)
//   {
//     case INT2OID:
//     case INT4OID:
//     case INT8OID:
//     case FLOAT4OID:
//     case FLOAT8OID:
//     case OIDOID:
//     case NUMERICOID:
//     {
//       extval = OidOutputFunctionCall(typoutput, node->constvalue);

// 				/*
// 				 * No need to quote unless it's a special value
// such as 'NaN'.
// 				 * See comments in get_const_expr().
// 				 */
// 				if (strspn(extval, "0123456789+-eE.") ==
// strlen(extval))
// 				{
// 					if (extval[0] == '+' || extval[0] ==
// '-') 						appendStringInfo(buf,
// "(%s)", extval); 					else
// appendStringInfoString(buf, extval);
// 				}
// 				else
// 					appendStringInfo(buf, "'%s'", extval);

//     }
//     break;
//   case BITOID:
//   case VARBITOID:
//     extval = OidOutputFunctionCall(typoutput, node->constvalue);
//     appendStringInfo(buf, "B'%s'", extval);
//     break;
//   case BOOLOID:
//     extval = OidOutputtFunctionCall(typoutput, node->constvalue);
//     if (strcmp(extval, "t") == 0)
//       appendStringInfoString(buf, "true");
//     else
//       appendStringInfoString(buf, "false");
//     break;
//   }
// }

bool iotdb_is_foreign_expr(PlannerInfo *root, RelOptInfo *baserel, Expr *expr,
                           bool for_tlist) {
  foreign_glob_cxt glob_cxt;
  foreign_loc_cxt loc_cxt;
  IotDBFdwRelationInfo *fpinfo = (IotDBFdwRelationInfo *)baserel->fdw_private;

  // check that the expression consists of nodes that are safe to execute

  glob_cxt.root = root;
  glob_cxt.foreignrel = baserel;
  glob_cxt.relids = baserel->relids;
  glob_cxt.mixing_aggref_Status = IOTDB_TARGETS_MIXING_AGGREF_SAFE;
  glob_cxt.for_tlist = for_tlist;
  glob_cxt.is_inner_func = false;

  /*
   * For an upper relation, use relids from its underneath scan relation,
   * because the upperrel's own relids currently aren't set to anything
   * meaningful by the core code.  For other relation, use their own relids.
   */

  if (baserel->reloptkind == RELOPT_UPPER_REL)
    glob_cxt.relids = fpinfo->outerrel->relids;
  else
    glob_cxt.relids = baserel->relids;

  loc_cxt.collation = InvalidOid;
  loc_cxt.state = FDW_COLLATE_NONE;
  loc_cxt.can_skip_cast = false;
  loc_cxt.influx_fill_enable = false;
  loc_cxt.has_time_key = false;
  loc_cxt.has_sub_or_add_operator = false;
  loc_cxt.is_comparison = false;

  if (!iotdb_foreign_expr_walker((Node *)expr, &glob_cxt, &loc_cxt))
    return false;
  return true;
}

void iotdb_bind_sql_var(Oid type, int attnum, Datum value,
                        IotDBColumnInfo *param_col_info,
                        IotDBType *param_iotdb_types,
                        IotDBValue *param_iotdb_values) {
  Oid outputFunctionId = InvalidOid;
  bool typeVarlena = false;

  getTypeOutputInfo(type, &outputFunctionId, &typeVarlena);

  switch (type) {
  case INT2OID: {
    int16 dat = DatumGetInt16(value);

    param_iotdb_values[attnum].i = dat;
    param_iotdb_types[attnum] = IOTDB_INT64;
    break;
  }
  case INT4OID: {
    int32 dat = DatumGetInt32(value);

    param_iotdb_values[attnum].i = dat;
    param_iotdb_types[attnum] = IOTDB_INT64;
    break;
  }
  case INT8OID: {
    int64 dat = DatumGetInt64(value);

    param_iotdb_values[attnum].i = dat;
    param_iotdb_types[attnum] = IOTDB_INT64;
    break;
  }
  case FLOAT4OID: {
    float4 dat = DatumGetFloat4(value);

    param_iotdb_values[attnum].d = (double)dat;
    param_iotdb_types[attnum] = IOTDB_DOUBLE;
    break;
  }
  case FLOAT8OID: {
    float8 dat = DatumGetFloat8(value);

    param_iotdb_values[attnum].d = dat;
    param_iotdb_types[attnum] = IOTDB_DOUBLE;
    break;
  }
  case NUMERICOID: {
    Datum valueDatum;
    valueDatum = DirectFunctionCall1(numeric_float8, value);
    float8 dat = DatumGetFloat8(valueDatum);

    param_iotdb_values[attnum].d = dat;
    param_iotdb_types[attnum] = IOTDB_DOUBLE;
    break;
  }
  case BOOLOID: {
    bool dat = DatumGetBool(value);

    param_iotdb_values[attnum].b = dat;
    param_iotdb_types[attnum] = IOTDB_BOOLEAN;
    break;
  }

  case TEXTOID:
  case BPCHAROID:
  case VARCHAROID: {
    char *outputstring = NULL;
    outputFunctionId = InvalidOid;
    typeVarlena = false;

    getTypeOutputInfo(type, &outputFunctionId, &typeVarlena);
    outputstring = OidOutputFunctionCall(outputFunctionId, value);
    param_iotdb_types[attnum] = IOTDB_STRING;
    param_iotdb_values[attnum].s = outputstring;
    break;
  }

  case TIMEOID:
  case TIMESTAMPOID:
  case TIMESTAMPTZOID: {
    if (param_col_info[attnum].col_type == IOTDB_TIME_KEY) {
      const int64 postgres_to_unix_epoch_usecs =
          (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * USECS_PER_DAY;
      Timestamp valueTimeStamp = DatumGetTimestamp(value);
      int64 valueNanoSecs =
          (valueTimeStamp + postgres_to_unix_epoch_usecs) * 1000;

      param_iotdb_values[attnum].i = valueNanoSecs;
      param_iotdb_types[attnum] = IOTDB_TIME;
    } else { // as string
      char *outputstring = NULL;
      outputFunctionId = InvalidOid;
      typeVarlena = false;

      getTypeOutputInfo(type, &outputFunctionId, &typeVarlena);
      outputstring = OidOutputFunctionCall(outputFunctionId, value);
      param_iotdb_types[attnum] = IOTDB_STRING;
      param_iotdb_values[attnum].s = outputstring;
    }
    break;
  }
  default: {
    ereport(ERROR,
            (errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
             errmsg("cannot convert constant value to InfluxDB value %u", type),
             errhint("Constant value data type: %u", type)));
    break;
  }
  }
}

char *iotdb_get_table_name(Relation rel)
{
    ForeignTable *table;
    char *relname = NULL;
    ListCell *lc;

    table = GetForeignTable(RelationGetRelid(rel));

    foreach(lc, table->options)
    {
        DefElem *def = (DefElem *)lfirst(lc);
        if (strcmp(def->defname, "table") == 0)
        {
            relname = defGetString(def);
            break;
        }
    }

    if(relname == NULL)
    {
        relname = RelationGetRelationName(rel);
    }

    return relname;
}