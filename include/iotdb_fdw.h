// #include "c.h"
// #include "nodes/pg_list.h"

#include "postgres.h"
#include "fmgr.h"
#include "nodes/pathnodes.h"
// #include "nodes/bitmapset.h"
// #include "nodes/nodes.h"

#include "foreign/foreign.h"

// #include "postgres_ext.h"
// #include "utils/palloc.h"
#include "query_cpp.h"
#include "utils/relcache.h"
// #include "stdbool.h"
// #include "utils/relcache.h"

#define CODE_VERSION 10000
typedef struct IotDBFdwRelationInfo {
  /*
   * True means that the relation can be pushed down. Always true for simple
   * foreign scan.
   */
  bool pushdown_safe;

  /* baserestrictinfo clauses, broken down into safe and unsafe subsets. */
  List *remote_conds;
  List *local_conds;

  /* Actual remote restriction clauses for scan (sans RestrictInfos) */
  List *final_remote_exprs;

  /* Bitmap of attr numbers we need to fetch from the remote server. */
  Bitmapset *attrs_used;

  /* True means that the query_pathkeys is safe to push down */
  bool qp_is_pushdown_safe;

  /* Cost and selectivity of local_conds. */
  QualCost local_conds_cost;
  Selectivity local_conds_sel;

  /* Selectivity of join conditions */
  Selectivity joinclause_sel;

  /* Estimated size and cost for a scan or join. */
  double rows;
  int width;
  Cost startup_cost;
  Cost total_cost;

  /* Costs excluding costs for transferring data from the foreign server */
  double retrieved_rows;
  Cost rel_startup_cost;
  Cost rel_total_cost;

  /* Options extracted from catalogs. */
  bool use_remote_estimate;
  Cost fdw_startup_cost;
  Cost fdw_tuple_cost;
  List *shippable_extensions; /* OIDs of whitelisted extensions */

  /* Cached catalog information. */
  ForeignTable *table;
  ForeignServer *server;
  UserMapping *user; /* only set in use_remote_estimate mode */

  int fetch_size; /* fetch size for this remote table */

  /*
   * Name of the relation, for use while EXjPLAINing ForeignScan.  It is used
   * for join and upper relations but is set for all relations.  For a base
   * relation, this is really just the RT index as a string; we convert that
   * while producing EXPLAIN output.  For join and upper relations, the name
   * indicates which base foreign tables are included and the join type or
   * aggregation type used.
   */
  char *relation_name;

  /* Join information */
  RelOptInfo *outerrel;
  RelOptInfo *innerrel;
  JoinType jointype;
  /* joinclauses contains only JOIN/ON conditions for an outer join */
  List *joinclauses; /* List of RestrictInfo */

  /* Upper relation information */
  UpperRelationKind stage;

  /* Grouping information */
  List *grouped_tlist;

  /* Subquery information */
  bool make_outerrel_subquery; /* do we deparse outerrel as a
                                * subquery? */
  bool make_innerrel_subquery; /* do we deparse innerrel as a
                                * subquery? */
  Relids lower_subquery_rels;  /* all relids appearing in lower
                                * subqueries */

  /*
   * Index of the relation.  It is used to create an alias to a subquery
   * representing the relation.
   */
  int relation_index;

  bool is_tlist_func_push_down;

  //
  bool add_fieldtag;
  /* JsonB column list */
  List *slcols;
} IotDBFdwRelationInfo;

typedef struct iotdb_opt {
  char *svr_database; // IotDB  database name  -->IoTDB Save group
  char *svr_table;    // IotDB  table name(timeseries) -->device
  char *svr_address;  // IotDB  server ip addr
  int svr_port;       // IotDB  port
  char *svr_username; // IotDB  user name
  char *svr_password; // IotDB  password
  List *tag_list;     // tag list for query

} iotdb_opt;

typedef enum IotDBType {
  IOTDB_INT64,
  IOTDB_DOUBLE,
  IOTDB_STRING,
  IOTDB_BOOLEAN,
  IOTDB_TIME,
} IotDBType;

// information for foreignScanState fdw_State
typedef struct IotDBFdwExecState {
  char *query;           // query string
  Relation rel;          // relcache entry for teh foreigntable
  Oid relid;             // relation oid
  UserMapping *user;     // user mapping for foreign server
  List *retrieved_Attrs; // List of target attrbute numbers
  char **params;
  bool cursor_exits;              // Existing cursor?
  int numParams;                  // number of parameters passed to the query
  FmgrInfo *param_finfo;          // output convension functions for them
  List *param_exprs;              // executable expressions for param values
  const char **param_values;      // textual  values of query parameters
  Oid *param_types;               // type of query parameters
  IotDBType *param_iotdb_types;   // IotDB type of query parameter
  IotDBValue *param_iotdb_values; // IotDB values
  IotDBColumnInfo *param_column_info; // information of columns
  int p_nums;                         // number of parameters to transmit
  FmgrInfo *p_flinfo;                 // output conversion functions for them
  iotdb_opt *iotdbFdwOptions;         // IotDB FDW options

  List *attr_list;   // query attributes list
  List *column_list; // column list of iotDB column

  int64 row_nums;     // number of rows;
  Datum **rows;       //	all rows of scan
  int64 rowidx;       // current index of rows
  bool **rows_isnull; // is null ?
  List *tlist;        // target list

  MemoryContext temp_context; // context for per-tuple temporary data
  AttrNumber *junk_idx;

  void *temp_result;
} IotDBFdwExecState;

// returns result set
extern struct IotDBQuery_return IotDBQuery(char *query, UserMapping *user,
                                           iotdb_opt *opts, IotDBType *ctypes,
                                           IotDBValue *cvalues, int cparamNUm);

enum FdwPathPrivateIndex {
  FdwPathPrivateHasFinalSort, // has-final-sort
  FdwPathPrivateHasLimit,     // has-limit
};

/*
 * Definitions to check mixing aggregate function
 * and non-aggregate function in target list
 */
#define IOTDB_TARGETS_MARK_COLUMN (1u << 0)
#define IOTDB_TARGETS_MARK_AGGREF (1u << 1)
#define IOTDB_TARGETS_MIXING_AGGREF_UNSAFE                                     \
  (IOTDB_TARGETS_MARK_COLUMN | IOTDB_TARGETS_MARK_AGGREF)
#define IOTDB_TARGETS_MIXING_AGGREF_SAFE (0u)

#define IOTDB_TIME_COLUMN "time"
#define IOTDB_TIME_TEXT_COLUMN "time_text"
#define IOTDB_TAGS_COLUMN "tags"
#define IOTDB_IS_TIME_COLUMN(X)                                                \
  (strcmp(X, IOTDB_TIME_COLUMN) == 0 || strcmp(X, IOTDB_TIME_TEXT_COLUMN) == 0)
#define IOTDB_IS_TIME_TYPE(typeoid)                                            \
  ((typeoid == TIMESTAMPTZOID) || (typeoid == TIMEOID) ||                      \
   (typeoid == TIMESTAMPOID))

#define DEFAULT_FDW_SORT_MULTIPLIER 1.2

// option.c
extern iotdb_opt *iotdb_get_options(Oid foreignoid, Oid userid);

// iotdb_fdw.c
extern int iotdb_set_transmission_modes(void);
extern void iotdb_reset_transmisson_modes(int nestlevel);

// deparse.c
char *iotdb_get_column_name(Oid relid, int attnum);
extern char *iotdb_escape_json_string(char *string);
extern Datum iotdb_convert_record_to_datum(Oid pgtyp, int pgtypmod, char **row, int attnum, int ntags, int nfield,
  char **column, char *opername, Oid relid, int ncol, bool is_schemaless);
extern Datum
  iotdb_convert_to_pg(Oid pgtyp, int pgtypmod, char *value);
extern void iotdb_deparse_select_stmt_for_rel(
    StringInfo buf, PlannerInfo *root, RelOptInfo *rel, List *tlist,
    List *remote_conds, List *pathkeys, bool is_subquery,
    List **retrieved_attrs, List **params_list, bool has_limit);

extern bool iotdb_is_tag_key(const char *colname, Oid reloid);

extern bool iotdb_is_foreign_expr(PlannerInfo *root, RelOptInfo *baserel,
                                  Expr *expr, bool for_tlist);

extern void iotdb_bind_sql_var(Oid type, int attnum, Datum value,
                               IotDBColumnInfo *param_col_info,
                               IotDBType *param_iotdb_types,
                               IotDBValue *param_iotdb_values);
                    
extern char *iotdb_get_table_name(Relation rel);

extern List *iotdb_build_tlist_to_deparse(RelOptInfo *foreignrel);