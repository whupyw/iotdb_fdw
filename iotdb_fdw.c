//#include "fmgr.h"

//#include "postgres.h"
#include "include/iotdb_fdw.h"

#include "c.h"

#include "common/fe_memutils.h"

#include "nodes/nodes.h"
#include "nodes/parsenodes.h"

#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"

#include "foreign/fdwapi.h"

#include "commands/defrem.h"

#include "utils/elog.h"
#include "miscadmin.h"

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


iotdb_opt *iotdb_get_options(Oid foreignoid, Oid userid)
{
    ForeignTable *f_table = NULL;
    ForeignServer *f_server = NULL;
    UserMapping *f_mapping;
    List *options;
    ListCell    *lc;
    iotdb_opt *opt;

    opt = (iotdb_opt *)palloc(sizeof(iotdb_opt));   
    memset(opt, 0, sizeof(iotdb_opt));

    PG_TRY();
    {
        f_table = GetForeignTable(foreignoid);
        f_server = GetForeignServer(f_table->serverid);
    }
    PG_CATCH();
    {
        f_table = NULL;
        f_server = GetForeignServer(foreignoid);
    }
    PG_END_TRY();

    f_mapping = GetUserMapping(userid, f_server->serverid);

    options = NIL;
    if (f_table)
        options = list_concat(options, f_table->options);
    options = list_concat(options, f_server->options);
    options = list_concat(options, f_mapping->options);

    foreach(lc, options)
    {
        DefElem *def = (DefElem *)lfirst(lc);

        if (strcmp(def->defname, "table") == 0)
            opt->svr_table = defGetString(def);
        else if (strcmp(def->defname, "host") == 0)
            opt->svr_address = defGetString(def);
        else if (strcmp(def->defname, "port") == 0)
            opt->svr_port = atoi(defGetString(def));
        else if (strcmp(def->defname, "user") == 0)
            opt->svr_username = defGetString(def);
        else if (strcmp(def->defname, "password") == 0)
            opt->svr_password = defGetString(def);
        else if (strcmp(def->defname, "dbname") == 0)
            opt->svr_database = defGetString(def);
    }

    return opt;
}

/*
 validate the generic options given to a FOREIGN DATA WRAPPER,
 SERVER, USER MAPPING or FOREIGN TABLE that uses iotdb_fdw
*/


Datum iotdb_fdw_handler(PG_FUNCTION_ARGS)
{
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
void iotdbGetForeignRelSize(PlannerInfo *root,
                                   RelOptInfo *baserel, 
                                   Oid foreigntableid)
{
    IotDBFdwRelationInfo *fpinfo;
    [[maybe_unused]]ListCell *lc;
    Oid userid ;
    [[maybe_unused]]iotdb_opt *opt;

    fpinfo = (IotDBFdwRelationInfo *)palloc0(sizeof(IotDBFdwRelationInfo));
    baserel->fdw_private = (void *)fpinfo;

    userid = OidIsValid(baserel->userid) ? baserel->userid : GetUserId();
    opt = iotdb_get_options(foreigntableid, userid);
    // default is need to push down
    fpinfo->pushdown_safe = true;


    fpinfo->table = GetForeignTable(foreigntableid);
    fpinfo->server = GetForeignServer(fpinfo->table->serverid);

    // Compute the selectivity and cost of the local_conds, to avoid repetitive computation.
    fpinfo->local_conds_sel = clauselist_selectivity(root,
                                                    fpinfo->local_conds,
                                                    baserel->relid,
                                                    JOIN_INNER,
                                                    NULL);
    
    // 
    fpinfo->rel_startup_cost = -1;
    fpinfo->rel_total_cost = -1;

    // remote estimate is not allowed
    if(fpinfo->use_remote_estimate)
    {
        ereport(ERROR,
                errmsg("remote estimate is not supported yet"));
    }
    else
    {
        // if the foreign table never be ANALYZED
        if(baserel->tuples < 0)
        {
            baserel->pages = 10;
            baserel->tuples = 
                ( 10 * BLCKSZ ) / (baserel->reltarget->width 
                                    + MAXALIGN(SizeofHeapTupleHeader) );   
        }
        // Estimate baserel siz as best we can with local statistics
        set_baserel_size_estimates(root, baserel);
        
		/* Fill in basically-bogus cost estimates for use later. */
        ///@todo:  realize this function
		estimate_path_cost_size(root, baserel, NIL, NIL,
            &fpinfo->rows, &fpinfo->width,
            &fpinfo->startup_cost, &fpinfo->total_cost);
    }
    
    fpinfo->relation_name = psprintf("%u", baserel->relid);
}

Datum iotdb_fdw_version(PG_FUNCTION_ARGS)
{
    PG_RETURN_INT32(CODE_VERSION);  
}

// estimate path cost and size for a scan on foreign table
void estimate_path_cost_size(PlannerInfo *root, 
                            RelOptInfo *foreignrel, 
                            List *param_join_conds, 
                            List *pathkeys, 
                            double *p_rows, int *p_width, 
                            Cost *p_startup_cos, Cost *p_total_cost)
{
    IotDBFdwRelationInfo *fpinfo = (IotDBFdwRelationInfo *)foreignrel->fdw_private;
    double rows;
    double retrieved_rows;
    int width;
    Cost startup_cost;
    Cost total_cost;
    Cost cpu_per_tuple;

    if(fpinfo->use_remote_estimate)
    {
        ereport(ERROR,
                errmsg("remote estimate is not supported yet"));
    }
    else 
    {
        Cost run_cost = 0;

        // don't support join conditions
        Assert(param_join_conds == NIL); 


        rows = foreignrel->rows;
        width = foreignrel->reltarget->width;

        // back into an estimate of the number of retrieved rows
        retrieved_rows = clamp_row_est(rows / fpinfo->local_conds_sel);

        if( fpinfo->rel_startup_cost > 0 && fpinfo->rel_total_cost > 0 )
        {
            startup_cost = fpinfo->rel_startup_cost;
            run_cost = fpinfo->rel_total_cost;
        }
        else
        {
            Assert(foreignrel->reloptkind != RELOPT_BASEREL);
            retrieved_rows = Min(retrieved_rows, foreignrel->tuples);

            startup_cost = 0;
            run_cost = 0;
            run_cost += seq_page_cost * foreignrel->pages;

            startup_cost += foreignrel->baserestrictcost.startup;
            cpu_per_tuple = cpu_tuple_cost + foreignrel->baserestrictcost.per_tuple;
            run_cost += cpu_per_tuple * foreignrel->tuples;
        }

        if(pathkeys != NIL)
        {
            startup_cost *= DEFAULT_FDW_SORT_MULTIPLIER;
            run_cost *= DEFAULT_FDW_SORT_MULTIPLIER;
        }
        total_cost = startup_cost + run_cost; // calculating total cost

        if( pathkeys == NIL && param_join_conds == NIL )
        {
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
void iotdbGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
    Cost startup_cost = 10;
    Cost total_cost = baserel->rows + startup_cost;

    //estimate costs
    total_cost = baserel->rows; 

    // create foreignpath node and add it as only possible path
    add_path(baserel, (Path *)create_foreignscan_path( root, 
                                                        baserel, 
                                                        NULL,  // defalut pathtarget
                                                        baserel->rows, 
                                                        startup_cost, 
                                                        total_cost,
                                                        NIL,  // no pathkeys
                                                        baserel->lateral_relids, 
                                                        NULL,    // no extra plan
                                                        NIL,  // no fdw_restrictinfo list
                                                        NIL));  // no fdw_private data
    
}

ForeignScan *iotdbGetForeignPlan(PlannerInfo *root,
                                       RelOptInfo *baserel,
                                       Oid foreigntableid,
                                       ForeignPath *best_path,
                                       List *tlist,
                                       List *scan_clauses,
                                       Plan *outer_plan)
{
    return NULL;
}

void iotdbBeginForeignScan(ForeignScanState *node, int eflags)
{
    return ;
}

TupleTableSlot *iotdbIterateForeignScan(ForeignScanState *node)
{
    return NULL;
}

void iotdbReScanForeignScan(ForeignScanState *node)
{
    return ;
}


void iotdbEndForeignScan(ForeignScanState *node)
{
    return ;
}
