// #include "fmgr.h"

// #include "postgres.h"
#include "include/iotdb_fdw.h"

#include "c.h"

#include "common/fe_memutils.h"

#include "lib/stringinfo.h"
#include "nodes/execnodes.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"

#include "nodes/pathnodes.h"
#include "nodes/pg_list.h"
#include "nodes/value.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"

#include "foreign/fdwapi.h"

#include "commands/defrem.h"

#include "miscadmin.h"
#include "optimizer/planmain.h"
#include "utils/elog.h"
#include <string.h>

#include "utils/float.h"
#include "utils/guc.h"

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
      Assert(foreignrel->reloptkind != RELOPT_JOINREL);   // not RELOPT_JOINREL
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
    }
  } else {
    // Join relation or upper relation - set scan_relid to 0.
    // but i don't want to support such operation now
    scan_relid = 0;

    ereport(ERROR,
            (errmsg("join relation or upper relation is not supported")));
  }

  initStringInfo(&sql);

  iotdb_deparse_select_stmt_for_rel(&sql, root, baserel, tlist, remote_exprs,
                                    NULL, false, &retrieved_attrs, &params_list,
                                    has_limit);
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
  // fdw_private = lappend(fdw_private,
  // makeInteger(fpinfo->is_tlist_func_pushdown)); fdw_private =
  // lappend(fdw_private, makeInteger(fpinfo->slinfo.schemaless));
  fdw_private = lappend(fdw_private, remote_conds);

  return make_foreignscan(tlist, local_exprs, scan_relid, params_list,
                          fdw_private, fdw_scan_tlist, fdw_recheck_quals,
                          outer_plan);
}

static void iotdbBeginForeignScan(ForeignScanState *node, int eflags) {
  
  return;
}

static TupleTableSlot *iotdbIterateForeignScan(ForeignScanState *node) {
  return NULL;
}

static void iotdbReScanForeignScan(ForeignScanState *node) { return; }

static void iotdbEndForeignScan(ForeignScanState *node) { return; }

static void iotdb_extract_slcols(IotDBFdwRelationInfo *fpinfo,
                                 PlannerInfo *root, RelOptInfo *baserel,
                                 List *tlist) {
  [[maybe_unused]] RangeTblEntry *rte;
  [[maybe_unused]] List *input_list =
      (tlist) ? tlist : baserel->reltarget->exprs;
  [[maybe_unused]] ListCell *lc;

  // because now is not support schemaless, so here is return directly
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