// #include "fmgr.h"

// #include "postgres.h"
#include "include/iotdb_fdw.h"
#include "access/tupdesc.h"
#include "executor/tuptable.h"
#include "optimizer/restrictinfo.h"
#include <stdbool.h>

#ifndef QUERY_REST
#include "include/query_rest.h"
#endif
#include "c.h"

#include "common/fe_memutils.h"

#include "executor/executor.h"
#include "fmgr.h"
#include "foreign/foreign.h"
#include "lib/stringinfo.h"

#include "nodes/bitmapset.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pathnodes.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "nodes/primnodes.h"
#include "nodes/value.h"

#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"

#include "foreign/fdwapi.h"

#include "commands/defrem.h"
#include "parser/parsetree.h"

#include "miscadmin.h"
#include "optimizer/planmain.h"
#include "postgres_ext.h"
#include "utils/elog.h"
#include <string.h>

#include "utils/float.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/palloc.h"

static void iotdbGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
                                   Oid foreigntableid);

static void iotdbGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel,
                                 Oid foreigntableid);

static ForeignScan *iotdbGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel,
                                        Oid foreigntableid,
                                        ForeignPath *best_path, List *tlist,
                                        List *scan_clauses, Plan *outer_plan);

static void iotdbBeginForeignScan(ForeignScanState *node, int eflags);

static TupleTableSlot *iotdbIterateForeignScan(ForeignScanState *node);

static void iotdbReScanForeignScan(ForeignScanState *node);

static void iotdbEndForeignScan(ForeignScanState *node);

/// get cost and size estimates for a foreign scan on given foreign relation
static void estimate_path_cost_size(PlannerInfo *root, RelOptInfo *foreignrel,
                                    List *param_join_conds, List *pathkeys,
                                    double *p_rows, int *p_width,
                                    Cost *p_startup_cos, Cost *p_total_cost);

/// extract actula remote iotdb columns that being fetched
static void iotdb_extract_slcols(IotDBFdwRelationInfo *fpinfo,
                                 PlannerInfo *root, RelOptInfo *baserel,
                                 List *tlist);

/*
 * Prepare for processing of parameters used in remote query.
 */
static void prepare_query_params(PlanState *node, List *fdw_exprs,
                                 List *remote_exprs, Oid foreigntableid,
                                 int numParams, FmgrInfo **param_flinfo,
                                 List **param_exprs, const char ***param_values,
                                 Oid **param_types,
                                 IotDBType **param_influxdb_types,
                                 IotDBValue **param_influxdb_values,
                                 IotDBColumnInfo **param_column_info);

static void create_cursor(ForeignScanState *node);
/*
 * Check if parameter is in the condition
 */
static bool iotdb_param_belong_to_qual(Node *qual, Node *param);

static void process_query_params(ExprContext *econtext, FmgrInfo *param_flinfo,
                                 List *param_exprs, const char **param_values,
                                 Oid *param_types,
                                 IotDBType *param_influxdb_types,
                                 IotDBValue *param_influxdb_values,
                                 IotDBColumnInfo *param_column_info);

static void make_tuple_from_result_row(IotDBRow *result_row,
                                       IotDBResult *result,
                                       TupleDesc tupleDescriptor, Datum *row,
                                       bool *is_null, Oid relid,
                                       IotDBFdwExecState *festate, bool is_agg);

// construct json struct from return values
static void iotdb_get_json_string_from_result(IotDBRow *result_row,
                                              IotDBResult *result, Oid relid,
                                              bool is_target_tags,
                                              bool is_target_fields,
                                              char **tags, char **fields);

Datum iotdb_fdw_version(PG_FUNCTION_ARGS);
/*
    // Funcs are used for scan foreign table

        GetForeignRelSize_function GetForeignRelSize;
        GetForeignPaths_function GetForeignPaths;
        GetForeignPlan_function GetForeignPlan;
        BeginForeignScan_function BeginForeignScan;
        IterateForeignScan_function IterateForeignScan;
        ReScanForeignScan_function ReScanForeignScan;
        EndForeignScan_function EndForeignScan;

*/

/*
 validate the generic options given to a FOREIGN DATA WRAPPER,
 SERVER, USER MAPPING or FOREIGN TABLE that uses iotdb_fdw
*/

Datum iotdb_fdw_handler(PG_FUNCTION_ARGS) {
  FdwRoutine *fdwroutine = makeNode(FdwRoutine);

  // only support scan foreign table
  fdwroutine->GetForeignRelSize = iotdbGetForeignRelSize;
  fdwroutine->GetForeignPaths = iotdbGetForeignPaths;
  fdwroutine->GetForeignPlan = iotdbGetForeignPlan;
  fdwroutine->BeginForeignScan = iotdbBeginForeignScan;
  fdwroutine->IterateForeignScan = iotdbIterateForeignScan;
  fdwroutine->ReScanForeignScan = iotdbReScanForeignScan;
  fdwroutine->EndForeignScan = iotdbEndForeignScan;

  PG_RETURN_POINTER(fdwroutine);
}

// create fdwplan for a scan on foreign table
static void iotdbGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
                                   Oid foreigntableid) {
  IotDBFdwRelationInfo *fpinfo;
  [[maybe_unused]] ListCell *lc;
  Oid userid;
  [[maybe_unused]] iotdb_opt *opt;

  fpinfo = (IotDBFdwRelationInfo *)palloc0(sizeof(IotDBFdwRelationInfo));
  baserel->fdw_private = (void *)fpinfo;

  userid = OidIsValid(baserel->userid) ? baserel->userid : GetUserId();
  opt = iotdb_get_options(foreigntableid, userid);
  // default is need to push down
  fpinfo->pushdown_safe = true;

  fpinfo->table = GetForeignTable(foreigntableid);
  fpinfo->server = GetForeignServer(fpinfo->table->serverid);

  pull_varattnos((Node *)baserel->reltarget->exprs, baserel->relid,
                 &fpinfo->attrs_used);

  foreach (lc, fpinfo->local_conds) {
    RestrictInfo *rinfo = (RestrictInfo *)lfirst(lc);

    pull_varattnos((Node *)rinfo->clause, baserel->relid, &fpinfo->attrs_used);
  }

  // Compute the selectivity and cost of the local_conds, to avoid repetitive
  // computation.
  fpinfo->local_conds_sel = clauselist_selectivity(
      root, fpinfo->local_conds, baserel->relid, JOIN_INNER, NULL);

  //
  fpinfo->rel_startup_cost = -1;
  fpinfo->rel_total_cost = -1;

  // remote estimate is not allowed
  if (fpinfo->use_remote_estimate) {
    ereport(ERROR, errmsg("remote estimate is not supported yet"));
  } else {
    // if the foreign table never be ANALYZED
    if (baserel->tuples < 0) {
      baserel->pages = 10;
      baserel->tuples = (10 * BLCKSZ) / (baserel->reltarget->width +
                                         MAXALIGN(SizeofHeapTupleHeader));
    }
    // Estimate baserel siz as best we can with local statistics
    set_baserel_size_estimates(root, baserel);

    /* Fill in basically-bogus cost estimates for use later. */
    ///@todo:  realize this function
    estimate_path_cost_size(root, baserel, NIL, NIL, &fpinfo->rows,
                            &fpinfo->width, &fpinfo->startup_cost,
                            &fpinfo->total_cost);
  }

  fpinfo->relation_name = psprintf("%u", baserel->relid);
}

Datum iotdb_fdw_version(PG_FUNCTION_ARGS) { PG_RETURN_INT32(CODE_VERSION); }

// estimate path cost and size for a scan on foreign table
static void estimate_path_cost_size(PlannerInfo *root, RelOptInfo *foreignrel,
                                    List *param_join_conds, List *pathkeys,
                                    double *p_rows, int *p_width,
                                    Cost *p_startup_cos, Cost *p_total_cost) {
  IotDBFdwRelationInfo *fpinfo =
      (IotDBFdwRelationInfo *)foreignrel->fdw_private;
  double rows;
  double retrieved_rows;
  int width;
  Cost startup_cost;
  Cost total_cost;
  Cost cpu_per_tuple;

  if (fpinfo->use_remote_estimate) {
    ereport(ERROR, errmsg("remote estimate is not supported yet"));
  } else {
    Cost run_cost = 0;

    // don't support join conditions
    Assert(param_join_conds == NIL);

    rows = foreignrel->rows;
    width = foreignrel->reltarget->width;

    // back into an estimate of the number of retrieved rows
    retrieved_rows = clamp_row_est(rows / fpinfo->local_conds_sel);

    if (fpinfo->rel_startup_cost > 0 && fpinfo->rel_total_cost > 0) {
      startup_cost = fpinfo->rel_startup_cost;
      run_cost = fpinfo->rel_total_cost;
    } else {
      Assert(foreignrel->reloptkind != RELOPT_JOINREL); // not RELOPT_JOINREL
      retrieved_rows = Min(retrieved_rows, foreignrel->tuples);

      startup_cost = 0;
      run_cost = 0;
      run_cost += seq_page_cost * foreignrel->pages;

      startup_cost += foreignrel->baserestrictcost.startup;
      cpu_per_tuple = cpu_tuple_cost + foreignrel->baserestrictcost.per_tuple;
      run_cost += cpu_per_tuple * foreignrel->tuples;
    }

    if (pathkeys != NIL) {
      startup_cost *= DEFAULT_FDW_SORT_MULTIPLIER;
      run_cost *= DEFAULT_FDW_SORT_MULTIPLIER;
    }
    total_cost = startup_cost + run_cost; // calculating total cost

    if (pathkeys == NIL && param_join_conds == NIL) {
      fpinfo->rel_startup_cost = startup_cost;
      fpinfo->rel_total_cost = total_cost;
    }

    /*
    take additional costs for transferring data from the foreign server,
    including the cost of CPU to process the data and the cost of network
    (fdw_startup_cost):transferring data across the network
    (fdw_tuple_cost per retireved row): local manipulation of data
    (cpu_tuple_cost per retrieved row)
    */
    startup_cost += fpinfo->fdw_startup_cost;
    total_cost += fpinfo->fdw_startup_cost;
    total_cost += fpinfo->fdw_tuple_cost * retrieved_rows;
    total_cost += cpu_tuple_cost * retrieved_rows;

    *p_rows = rows;
    *p_width = width;
    *p_startup_cos = startup_cost;
    *p_total_cost = total_cost;
  }
}
// create foreign path for a scan on foreign table
static void iotdbGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel,
                                 Oid foreigntableid) {
  Cost startup_cost = 10;
  Cost total_cost = baserel->rows + startup_cost;

  // estimate costs
  total_cost = baserel->rows;

  // create foreignpath node and add it as only possible path
  add_path(baserel, (Path *)create_foreignscan_path(
                        root, baserel,
                        NULL, // defalut pathtarget
                        baserel->rows, startup_cost, total_cost,
                        NIL, // no pathkeys
                        baserel->lateral_relids,
                        NULL,  // no extra plan
                        NIL,   // no fdw_restrictinfo list
                        NIL)); // no fdw_private data
}

// get a foreign scan plan node
static ForeignScan *iotdbGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel,
                                        Oid foreigntableid,
                                        ForeignPath *best_path, List *tlist,
                                        List *scan_clauses, Plan *outer_plan) {
  IotDBFdwRelationInfo *fpinfo = (IotDBFdwRelationInfo *)baserel->fdw_private;
  [[maybe_unused]] Index scan_relid = baserel->relid;
  List *fdw_private = NULL;
  List *local_exprs = NULL;
  List *fdw_scan_tlist = NIL;
  List *remote_conds = NIL;
  List *remote_exprs = NIL;
  List *params_list = NIL;

  StringInfoData sql;
  List *retrieved_attrs;
  ListCell *lc;
  int for_update;
  List *fdw_recheck_quals = NIL;

  bool has_limit = false;

  if (best_path->fdw_private)
    has_limit =
        boolVal(list_nth(best_path->fdw_private, FdwPathPrivateHasLimit));

  // build the query string to be sent for execution.
  // and identify expressions to be sent as pameters
  // build the query
  initStringInfo(&sql);

  /*
      target : seperate local_conds and remote_conds to accelerate speed of
     query
  */
  if ((baserel->reloptkind == RELOPT_BASEREL ||
       baserel->reloptkind == RELOPT_OTHER_MEMBER_REL) &&
      fpinfo->is_tlist_func_push_down == false)
  // for now, only support basic type, function
  // push_down is also unsupported
  {
    // because we don't support schemaless, so here is nothing happend
    // schmaless col s
    iotdb_extract_slcols(fpinfo, root, baserel, tlist);

    foreach (lc, scan_clauses) {
      RestrictInfo *rinfo = (RestrictInfo *)lfirst(lc);
      Assert(IsA(rinfo, RestrictInfo));

      // skip the pseudoconstant clauses, because it has been proceed in
      // otherplace
      if (rinfo->pseudoconstant)
        continue;
      // conditions can be placed to remote
      if (list_member_ptr(fpinfo->remote_conds, rinfo))
        remote_exprs = lappend(remote_exprs, rinfo->clause);
      // conditions can be placed to local
      else if (list_member_ptr(fpinfo->local_conds, rinfo))
        local_exprs = lappend(local_exprs, rinfo->clause);
      else if (iotdb_is_foreign_expr(root, baserel, rinfo->clause, false))
        remote_exprs = lappend(remote_exprs, rinfo->clause);
      // default is local
      else
        local_exprs = lappend(local_exprs, rinfo->clause);

      fdw_recheck_quals = remote_exprs;
    }
  } else {
    // Join relation or upper relation - set scan_relid to 0.
    // but i don't want to support such operation now
    scan_relid = 0;

    if (fpinfo->is_tlist_func_push_down == false) {
      // for now, only support basic type, function
      // push_down is also unsupported
      Assert(!scan_clauses);
    }
    remote_exprs = extract_actual_clauses(fpinfo->remote_conds, false);
    local_exprs = extract_actual_clauses(fpinfo->local_conds, false);

    if (fpinfo->is_tlist_func_push_down == true)
      ereport(ERROR,
              (errmsg("join relation or upper relation is not supported")));

    else
      fdw_scan_tlist = iotdb_build_tlist_to_deparse(baserel);

    if (outer_plan) {
      ///@todo here is grouping and aggregation
    }
  }

  initStringInfo(&sql);

  iotdb_deparse_select_stmt_for_rel(&sql, root, baserel, fdw_scan_tlist,
                                    remote_exprs, NULL, false, &retrieved_attrs,
                                    &params_list, has_limit);
  fpinfo->final_remote_exprs = remote_exprs;

  ///@todo for update
  for_update = 0;
  /* Get remote condition */
  if (baserel->reloptkind == RELOPT_UPPER_REL) {
    IotDBFdwRelationInfo *ofpinfo;

    ofpinfo = (IotDBFdwRelationInfo *)fpinfo->outerrel->fdw_private;
    remote_conds = ofpinfo->remote_conds;
  } else {
    remote_conds = remote_exprs;
  }

  fdw_private = list_make3(makeString(sql.data), retrieved_attrs,
                           makeInteger(for_update));
  fdw_private = lappend(fdw_private, fdw_scan_tlist);
  fdw_private =
      lappend(fdw_private, makeInteger(fpinfo->is_tlist_func_push_down));
  // fdw_private = lappend(fdw_private, makeInteger(fpinfo->slinfo.schemaless));
  fdw_private = lappend(fdw_private, remote_conds);

  return make_foreignscan(tlist, local_exprs, scan_relid, params_list,
                          fdw_private, fdw_scan_tlist, fdw_recheck_quals,
                          outer_plan);
}

/// Initialize access to database
static void iotdbBeginForeignScan(ForeignScanState *node, int eflags) {
  IotDBFdwExecState *festate = NULL;
  EState *estate = node->ss.ps.state;
  ForeignScan *fscan = (ForeignScan *)node->ss.ps.plan;
  RangeTblEntry *rte;
  int num_params;
  int rtindex;
  [[maybe_unused]] bool schemaless = false; // not support schemaless now
  Oid userid;
  [[maybe_unused]] ForeignTable *table;
  List *remote_exprs;
  ForeignTable *ftable;

  festate = (IotDBFdwExecState *)palloc0(sizeof(IotDBFdwExecState));
  node->fdw_state = (void *)festate;

  // save private state in node->fdw_state
  festate->rowidx = 0;
  node->fdw_state = (void *)festate;
  festate->rowidx = 0;

  // stash away the state info we have already
  festate->query = strVal(list_nth(fscan->fdw_private, 0));
  festate->retrieved_Attrs = list_nth(fscan->fdw_private, 1);
  // festate->for_update = intVal(list_nth(fscan->fdw_private, 2));
  festate->tlist = (List *)list_nth(fscan->fdw_private, 3);
  // festate->is_tlist_func_pushdown = intVal(list_nth(fscan->fdw_private, 4));
  remote_exprs = (List *)list_nth(fscan->fdw_private, 4);

  festate->cursor_exits = false;

  if (fscan->scan.scanrelid > 0)
    rtindex = fscan->scan.scanrelid;

  rtindex = bms_next_member(fscan->fs_base_relids, -1);
  rte = exec_rt_fetch(rtindex, estate);
  userid = OidIsValid(fscan->checkAsUser) ? fscan->checkAsUser : GetUserId();

  // get options
  festate->iotdbFdwOptions = iotdb_get_options(rte->relid, userid);

  ftable = GetForeignTable(rte->relid);
  festate->user = GetUserMapping(userid, ftable->serverid);

  num_params = list_length(fscan->fdw_exprs);
  festate->numParams = num_params;
  if (num_params > 0)
    prepare_query_params((PlanState *)node, fscan->fdw_exprs, remote_exprs,
                         rte->relid, num_params, &festate->param_finfo,
                         &festate->param_exprs, &festate->param_values,
                         &festate->param_types, &festate->param_iotdb_types,
                         &festate->param_iotdb_values,
                         &festate->param_column_info);
}

static TupleTableSlot *iotdbIterateForeignScan(ForeignScanState *node) {
  IotDBFdwExecState *festate = (IotDBFdwExecState *)node->fdw_state;
  TupleTableSlot *tupleSlot = node->ss.ss_ScanTupleSlot;
  EState *estate = node->ss.ps.state;
  TupleDesc tupleDescriptor = tupleSlot->tts_tupleDescriptor;
  iotdb_opt *opt = festate->iotdbFdwOptions;
  struct IotDBQuery_return volatile ret; // pack the query result
  struct IotDBResult volatile *result;
  ForeignScan *fscan = (ForeignScan *)node->ss.ps.plan;
  [[maybe_unused]] RangeTblEntry *rte;
  int32 rtindex;
  [[maybe_unused]] bool is_aggregate;

  /*
   * Identify which user to do the remote access as.  This should match what
   * ExecCheckRTEPerms() does.  In case of a join or aggregate, use the
   * lowest-numbered member RTE as a representative; we would get the same
   * result from any.
   */
  if (fscan->scan.scanrelid > 0) {
    rtindex = fscan->scan.scanrelid;
    is_aggregate = false;
  } else {
    rtindex = bms_next_member(fscan->fs_relids, -1);
    is_aggregate = true;
  }
  rte = rt_fetch(rtindex, estate->es_range_table);

  // get options
  opt = festate->iotdbFdwOptions;

  // if this is first call after Begin or Rescan, we need to create a new cursor
  // on remote side
  if (!festate->cursor_exits)
    create_cursor(node);

  memset(tupleSlot->tts_values, 0, sizeof(Datum) * tupleDescriptor->natts);
  memset(tupleSlot->tts_isnull, true, sizeof(bool) * tupleDescriptor->natts);
  ExecClearTuple(tupleSlot);

  if (festate->rowidx == 0) {
    MemoryContext oldcontext = NULL;

    PG_TRY();
    {
      oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);
      ret = IotDBQuery(festate->query, festate->user, opt,
                       festate->param_iotdb_types, festate->param_iotdb_values,
                       festate->numParams);

      if (ret.r1 != NULL) // ERROR occured
      {
        char *err = pstrdup(ret.r1);
        // char *err = ret.r1;
        free(ret.r1);
        ret.r1 = err;
        ereport(ERROR, (errmsg("IotDBQuery failed: %s", ret.r1)));
      }

      result = ret.r0;
      festate->temp_result = (void *)result;

      festate->row_nums = result->nrow;
      MemoryContextSwitchTo(oldcontext);
      // IotDBFreeResult(result);
    }
    PG_CATCH();
    {
      if (ret.r1 == NULL) {
        // IotDBFreeResult(result);
        ereport(ERROR, (errmsg("IotDBQuery failed: unknown error")));
      }

      if (oldcontext)
        MemoryContextSwitchTo(oldcontext);
      PG_RE_THROW();
    }
    PG_END_TRY();
  }

  if (festate->rowidx < festate->row_nums) {
    MemoryContext oldcontext = NULL;

    result = (IotDBResult *)festate->temp_result;

    make_tuple_from_result_row(&(result->rows[festate->rowidx]),
                               (IotDBResult *)result, tupleDescriptor,
                               tupleSlot->tts_values, tupleSlot->tts_isnull,
                               rte->relid, festate, is_aggregate);

    oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

    // freeIotDBResultRow(festate, festate->rowidx);

    if (festate->rowidx == (festate->row_nums - 1)) {
      // freeIotDBResult(festate);
    }
    MemoryContextSwitchTo(oldcontext);
    ExecStoreVirtualTuple(tupleSlot);
    festate->rowidx++;
  }
  return tupleSlot;
}

static void iotdbReScanForeignScan(ForeignScanState *node) {
  IotDBFdwExecState *festate = (IotDBFdwExecState *)node->fdw_state;

  festate->cursor_exits = false;
  festate->rowidx = 0;
}

static void iotdbEndForeignScan(ForeignScanState *node) { 
  IotDBFdwExecState *festate = (IotDBFdwExecState *)node->fdw_state;

  if( festate != NULL)
  {
    festate->cursor_exits = false;
    festate->rowidx = 0;
  }
}

static void iotdb_extract_slcols(IotDBFdwRelationInfo *fpinfo,
                                 PlannerInfo *root, RelOptInfo *baserel,
                                 List *tlist) {
  [[maybe_unused]] RangeTblEntry *rte;
  [[maybe_unused]] List *input_list =
      (tlist) ? tlist : baserel->reltarget->exprs;
  [[maybe_unused]] ListCell *lc;

  return;
}

int iotdb_set_transmission_modes(void) {
  int nestlevel = NewGUCNestLevel();

  /*
   * The values set here should match what pg_dump does.  See also
   * configure_remote_session in connection.c.
   */
  if (DateStyle != USE_ISO_DATES)
    (void)set_config_option("datestyle", "ISO", PGC_USERSET, PGC_S_SESSION,
                            GUC_ACTION_SAVE, true, 0, false);

  if (IntervalStyle != INTSTYLE_POSTGRES)
    (void)set_config_option("intervalstyle", "postgres", PGC_USERSET,
                            PGC_S_SESSION, GUC_ACTION_SAVE, true, 0, false);
  if (extra_float_digits < 3)
    (void)set_config_option("extra_float_digits", "3", PGC_USERSET,
                            PGC_S_SESSION, GUC_ACTION_SAVE, true, 0, false);

  /*
   * In addition force restrictive search_path, in case there are any
   * regproc or similar constants to be printed.
   */
  (void)set_config_option("search_path", "pg_catalog", PGC_USERSET,
                          PGC_S_SESSION, GUC_ACTION_SAVE, true, 0, false);

  return nestlevel;
}

void iotdb_reset_transmisson_modes(int nestlevel) {
  AtEOXact_GUC(true, nestlevel);
}

static void // params are used in remote query
prepare_query_params(PlanState *node, List *fdw_exprs, List *remote_exprs,
                     Oid foreigntableid, int numParams, FmgrInfo **param_flinfo,
                     List **param_exprs, const char ***param_values,
                     Oid **param_types, IotDBType **param_iotdb_types,
                     IotDBValue **param_iotdb_values,
                     IotDBColumnInfo **param_column_info) {
  int i;
  ListCell *lc;

  Assert(numParams > 0);

  *param_flinfo = (FmgrInfo *)palloc0(sizeof(FmgrInfo) * numParams);
  *param_types = (Oid *)palloc0(sizeof(Oid) * numParams);
  *param_iotdb_types = (IotDBType *)palloc0(sizeof(IotDBType) * numParams);
  *param_iotdb_values = (IotDBValue *)palloc0(sizeof(IotDBValue) * numParams);
  *param_column_info =
      (IotDBColumnInfo *)palloc0(sizeof(IotDBColumnInfo) * numParams);

  i = 0;
  foreach (lc, fdw_exprs) {
    Node *param_expr = (Node *)lfirst(lc);
    Oid typefnoid;
    bool isvarlena;

    (*param_types)[i] = exprType(param_expr);
    getTypeOutputInfo(exprType(param_expr), &typefnoid, &isvarlena);
    fmgr_info(typefnoid, &(*param_flinfo)[i]);

    // columns : TIME || TAGS || FIELD
    if (IOTDB_IS_TIME_TYPE((*param_types)[i])) {
      ListCell *expr_cell;

      foreach (expr_cell, remote_exprs) {
        Node *qual = (Node *)lfirst(expr_cell);

        if (iotdb_param_belong_to_qual(qual, param_expr)) {
          Var *col;
          char *colname;
          List *column_list = pull_var_clause(qual, PVC_RECURSE_PLACEHOLDERS);

          /*
           * Cases for time comparison with Parameter InfluxDB FDW supports
           * pushdown. (1) time type column (both time key and tags/fields) =
           * Param (2) time key column > Param (3) time key column < Param (4)
           * time key column >= Param (5) time key column <= Param
           *
           * In each case, there is only one time column, so column_list has one
           * item.
           */
          col = linitial(column_list);

          colname = iotdb_get_column_name(foreigntableid, col->varattno);

          if (IOTDB_IS_TIME_COLUMN(colname))
            (*param_column_info)[i].col_type = IOTDB_TIME_KEY;
          else if (iotdb_is_tag_key(colname, foreigntableid))
            (*param_column_info)[i].col_type = IOTDB_TAG_KEY;
          else
            (*param_column_info)[i].col_type = IOTDB_FIELD_KEY;
        }
      }
    }
    i++;
  }
}

/*
 * Check if parameter is in the condition
 */
static bool iotdb_param_belong_to_qual(Node *qual, Node *param) {
  if (qual == NULL)
    return false;

  if (equal(qual, param))
    return true;

  return expression_tree_walker(qual, iotdb_param_belong_to_qual, param);
}

// create a cursor for node's query with current parameter values
static void create_cursor(ForeignScanState *node) {
  IotDBFdwExecState *festate = (IotDBFdwExecState *)node->fdw_state;
  ExprContext *econtext = node->ss.ps.ps_ExprContext;
  int numParams = festate->numParams;
  const char **values = festate->param_values;

  if (numParams > 0) {
    MemoryContext oldcontxt;

    oldcontxt = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
    festate->params = palloc(numParams);
    process_query_params(
        econtext, festate->param_finfo, festate->param_exprs, values,
        festate->param_types, festate->param_iotdb_types,
        festate->param_iotdb_values, festate->param_column_info);
    MemoryContextSwitchTo(oldcontxt);
  }
}

static void process_query_params(ExprContext *econtext, FmgrInfo *param_flinfo,
                                 List *param_exprs, const char **param_values,
                                 Oid *param_types, IotDBType *param_iotdb_types,
                                 IotDBValue *param_iotdb_values,
                                 IotDBColumnInfo *param_column_info) {
  int nestlevel;
  int i;
  ListCell *lc;

  nestlevel = iotdb_set_transmission_modes();

  i = 0;
  foreach (lc, param_exprs) {
    ExprState *state = (ExprState *)lfirst(lc);
    Datum expr_value;
    bool isnull;

    // evaluate the parameters' value
    expr_value = ExecEvalExpr(state, econtext, &isnull);

    // IoTDB support null
    //  if(isnull)
    //  {
    //    ereport(ERROR, errmsg("iotdb_fdw:"));
    //  }

    // Bind parameters
    iotdb_bind_sql_var(param_types[i], i, expr_value, param_column_info,
                       param_iotdb_types, param_iotdb_values);
    i++;
  }
  iotdb_reset_transmisson_modes(nestlevel);
}

static void
make_tuple_from_result_row(IotDBRow *result_row, IotDBResult *result,
                           TupleDesc tupleDescriptor, Datum *row, bool *is_null,
                           Oid relid, IotDBFdwExecState *festate, bool is_agg) {
  ListCell *lc;
  int attid = 0;
  List *retrieved_attrs = festate->retrieved_Attrs;
  [[maybe_unused]] ListCell *targetc;
  char *opername = NULL;

  memset(row, 0, sizeof(Datum) * tupleDescriptor->natts);
  memset(is_null, true, sizeof(bool) * tupleDescriptor->natts);

  targetc = list_head(festate->tlist);

  foreach (lc, retrieved_attrs) {
    int attnum = lfirst_int(lc) - 1;
    Oid pgtype = TupleDescAttr(tupleDescriptor, attnum)->atttypid;
    int32 pgtypmod = TupleDescAttr(tupleDescriptor, attnum)->atttypmod;
    int result_idx = 0;
    char *colname = NULL;
    bool is_agg_star = false;
    bool is_regex = false;
    int ntags = 0;
    int nfields = 0;
    bool is_tags = false;
    bool is_fields = false;
    bool is_tagfields = false;
    char *value = NULL;
    char *tags_value = NULL;
    char *fields_value = NULL;

    if (is_agg) {
      ereport(ERROR, errmsg("Not support aggregate function"));
    } else {
      colname = iotdb_get_column_name(relid, attnum + 1);
      if (IOTDB_IS_TIME_COLUMN(colname)) {
        result_idx = 0;
      } else {
        {
          attid++;
          result_idx = attid;
        }
        // current we don't support schemaless
        //  if (festate->slinfo.schemaless) {
      }
    }

    if (is_tagfields) {
      if (!tags_value && !fields_value) {
        ///@todo
        iotdb_get_json_string_from_result(result_row, result, relid, is_tags,
                                          is_fields, &tags_value,
                                          &fields_value);
      }
      if (is_tags) {
        value = tags_value;
      } else if (is_fields) {
        value = fields_value;
      }
    } else {
      value = result_row->tuple[result_idx];
    }

    if (is_agg_star || is_regex) {
      is_null[attnum] = true;
      row[attnum] = iotdb_convert_record_to_datum(
          pgtype, pgtypmod, result_row->tuple, result_idx, ntags, nfields,
          result->colnums, opername, relid, result->ncol - result->ntag, false);

    } else if (value) {
      is_null[attnum] = false;
      row[attnum] = iotdb_convert_to_pg(pgtype, pgtypmod, value);
    }
  }
}

static void iotdb_get_json_string_from_result(IotDBRow *result_row,
                                              IotDBResult *result, Oid relid,
                                              bool is_target_tags,
                                              bool is_target_fields,
                                              char **tags, char **fields) {
  StringInfo buffer;
  int i = 0;
  bool is_first = true;
  bool has_jsstr = false;

  buffer = makeStringInfo();

  appendStringInfoChar(buffer, '{');

  for (i = 0; i < result->ncol; i++) {
    char *escaped_key = NULL;
    char *escaped_value = NULL;
    bool is_tag = false;

    if (IOTDB_IS_TIME_COLUMN(result->colnums[i])) {
      continue;
    }

    is_tag = iotdb_is_tag_key(result->colnums[i], relid);

    if (!(is_target_tags && is_tag) && !(is_target_fields && !is_tag)) {
      continue;
    }

    if (!is_first) {
      appendStringInfoChar(buffer, ',');
    }

    escaped_key = iotdb_escape_json_string(result->colnums[i]);
    if (escaped_key == NULL)
      elog(ERROR, "Cannot escape json column key");

    escaped_value = iotdb_escape_json_string(result_row->tuple[i]);

    appendStringInfo(buffer, "\"%s\" : ", escaped_key); /*key*/
    if (escaped_value)
      appendStringInfo(buffer, "\"%s\"", escaped_value); /*value*/
    else
      appendStringInfoString(buffer, "null"); /*null  */

    if (escaped_key != NULL)
      pfree(escaped_key);
    if (escaped_value != NULL)
      pfree(escaped_value);

    has_jsstr = true;
    is_first = false;
  }

  appendStringInfoChar(buffer, '}');

  if (has_jsstr) {
    if (is_target_tags)
      *tags = buffer->data;
    else
      *fields = buffer->data;
  }
  return;
}