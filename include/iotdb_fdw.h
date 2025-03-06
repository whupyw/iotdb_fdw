#include "postgres.h"
#include "fmgr.h"

#include "nodes/pathnodes.h"
#include "nodes/bitmapset.h" 
#include "nodes/nodes.h"
#include "nodes/plannodes.h"
#include "nodes/execnodes.h"

#include "executor/tuptable.h"

#include "foreign/foreign.h"

#include "stdbool.h"


#define CODE_VERSION 10000
typedef struct IotDBFdwRelationInfo
{
	/*
	 * True means that the relation can be pushed down. Always true for simple
	 * foreign scan.
	 */
	bool		pushdown_safe;

	/* baserestrictinfo clauses, broken down into safe and unsafe subsets. */
	List	   *remote_conds;
	List	   *local_conds;

	/* Actual remote restriction clauses for scan (sans RestrictInfos) */
	List	   *final_remote_exprs;

	/* Bitmap of attr numbers we need to fetch from the remote server. */
	Bitmapset  *attrs_used;

	/* True means that the query_pathkeys is safe to push down */
	bool		qp_is_pushdown_safe;

	/* Cost and selectivity of local_conds. */
	QualCost	local_conds_cost;
	Selectivity local_conds_sel;

	/* Selectivity of join conditions */
	Selectivity joinclause_sel;

	/* Estimated size and cost for a scan or join. */
	double		rows;
	int			width;
	Cost		startup_cost;
	Cost		total_cost;

	/* Costs excluding costs for transferring data from the foreign server */
	double		retrieved_rows;
	Cost		rel_startup_cost;
	Cost		rel_total_cost;

	/* Options extracted from catalogs. */
	bool		use_remote_estimate;
	Cost		fdw_startup_cost;
	Cost		fdw_tuple_cost;
	List	   *shippable_extensions;	/* OIDs of whitelisted extensions */

	/* Cached catalog information. */
	ForeignTable *table;
	ForeignServer *server;
	UserMapping *user;			/* only set in use_remote_estimate mode */

	int			fetch_size;		/* fetch size for this remote table */

	/*
	 * Name of the relation, for use while EXjPLAINing ForeignScan.  It is used
	 * for join and upper relations but is set for all relations.  For a base
	 * relation, this is really just the RT index as a string; we convert that
	 * while producing EXPLAIN output.  For join and upper relations, the name
	 * indicates which base foreign tables are included and the join type or
	 * aggregation type used.
	 */
	char	   *relation_name;

	/* Join information */
	RelOptInfo *outerrel;
	RelOptInfo *innerrel;
	JoinType	jointype;
	/* joinclauses contains only JOIN/ON conditions for an outer join */
	List	   *joinclauses;	/* List of RestrictInfo */

	/* Upper relation information */
	UpperRelationKind stage;

	/* Grouping information */
	List	   *grouped_tlist;

	/* Subquery information */
	bool		make_outerrel_subquery; /* do we deparse outerrel as a
										 * subquery? */
	bool		make_innerrel_subquery; /* do we deparse innerrel as a
										 * subquery? */
	Relids		lower_subquery_rels;	/* all relids appearing in lower
										 * subqueries */

	/*
	 * Index of the relation.  It is used to create an alias to a subquery
	 * representing the relation.
	 */
	int			relation_index;
	/* JsonB column list */
	List	   *slcols;
}			IotDBFdwRelationInfo;


typedef struct iotdb_opt
{
	char    *svr_database;  // IotDB  database name
    char    *svr_table;     // IotDB  table name(timeseries)
    char    *svr_address;   // IotDB  server ip addr
    int      svr_port;      // IotDB  port
    char    *svr_username;  // IotDB  user name
    char    *svr_password;  // IotDB  password

} iotdb_opt;

PG_MODULE_MAGIC;

#define DEFAULT_FDW_SORT_MULTIPLIER 1.2


extern Datum iotdb_fdw_handler(PG_FUNCTION_ARGS);
extern Datum iotdb_fdw_validator(PG_FUNCTION_ARGS);


PG_FUNCTION_INFO_V1(iotdb_fdw_handler);
PG_FUNCTION_INFO_V1(iotdb_fdw_version);

 void iotdbGetForeignRelSize(PlannerInfo *root,
                                   RelOptInfo *baserel, 
                                   Oid foreigntableid);

 void iotdbGetForeignPaths(PlannerInfo *root,
                                 RelOptInfo *baserel,
                                 Oid foreigntableid);

 ForeignScan *iotdbGetForeignPlan(PlannerInfo *root,
                                        RelOptInfo *baserel,
                                        Oid foreigntableid,
                                        ForeignPath *best_path,
                                        List *tlist,
                                        List *scan_clauses,
                                        Plan *outer_plan);

 void iotdbBeginForeignScan(ForeignScanState *node, int eflags);

 TupleTableSlot *iotdbIterateForeignScan(ForeignScanState *node);

 void iotdbReScanForeignScan(ForeignScanState *node);

 void iotdbEndForeignScan(ForeignScanState *node);

 iotdb_opt *iotdb_get_options(Oid foreignoid, Oid userid);

 Datum iotdb_fdw_version(PG_FUNCTION_ARGS);

/// get cost and size estimates for a foreign scan on given foreign relation 
void estimate_path_cost_size(PlannerInfo *root,
							RelOptInfo *foreignrel,
							List	*param_join_conds,
							List	*pathkeys,
							double *p_rows, int *p_width,
							Cost *p_startup_cos, Cost *p_total_cost);