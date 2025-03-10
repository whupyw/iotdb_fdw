#include "fmgr.h"
#include "postgres.h"

#include "nodes/pathnodes.h"
#include "nodes/primnodes.h"
#include "postgres_ext.h"
#include <sys/types.h>

typedef enum
{
	FDW_COLLATE_NONE,			/* expression is of a noncollatable type */
	FDW_COLLATE_SAFE,			/* collation derives from a foreign Var */
	FDW_COLLATE_UNSAFE,			/* collation derives from something else */
} FDWCollateState;

typedef struct foreign_glob_cxt
{
    PlannerInfo *root;          // global planner state
    RelOptInfo  *foreignrel;    // foreign relation 
    Relids      relids;         // relids of base relations in the underlying
    Oid         relid;          // relation oid
    uint        mixing_aggref_Status;   //mixing_aggref_status, but now we do not use
    bool        for_tlist;      //whether evaluation for the expression of tlist
    bool        is_inner_func;  // exist or not in inner exprs
}foreign_glob_cxt;

typedef struct foreign_loc_cxt
{
	Oid			collation;		/* OID of current collation, if any */
	FDWCollateState state;		/* state of current collation choice */
	bool		can_skip_cast;	/* outer function can skip float8/numeric cast */
	bool		can_pushdown_stable;	/* true if query contains stable
										 * function with star or regex */
	bool		can_pushdown_volatile;	/* true if query contains volatile
										 * function */
	bool		influx_fill_enable; /* true if deparse subexpression inside
									 * influx_time() */
	bool		have_otherfunc_influx_time_tlist; /* true if having other functions than influx_time() in tlist. */
	bool		has_time_key;			/* mark true if comparison with time key column */
	bool		has_sub_or_add_operator; 	/* mark true if expression has '-' or '+' operator */
	bool		is_comparison;				/* mark true if has comparison */
} foreign_loc_cxt;

bool iotdb_is_foreign_expr(PlannerInfo *root, RelOptInfo *baserel, Expr *expr,
                           bool for_tlist);