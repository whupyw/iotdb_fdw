//#include "fmgr.h"
#include "postgres.h"
#include "include/iotdb_fdw.h"

#include "c.h"
#include "common/fe_memutils.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"


#include "nodes/pg_list.h"


#include "foreign/fdwapi.h"
#include "foreign/foreign.h"


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
		/*estimate_path_cost_size(root, baserel, NIL, NIL,
            &fpinfo->rows, &fpinfo->width,
            &fpinfo->startup_cost, &fpinfo->total_cost);*/    
    }
    
    fpinfo->relation_name = psprintf("%u", baserel->relid);
}

Datum iotdb_fdw_version(PG_FUNCTION_ARGS)
{
    PG_RETURN_INT32(CODE_VERSION);  
}
/// @todo functions below are unimplemented
void iotdbGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
    return ;
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
