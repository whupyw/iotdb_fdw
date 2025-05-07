#include "catalog/pg_foreign_server_d.h"
#include "catalog/pg_user_mapping_d.h"
#include "catalog/pg_foreign_table_d.h"

#include "postgres_ext.h"
#include "postgres.h"

#include "fmgr.h"

struct IotDBFdwOption{
    const char *optname;
    Oid         optcontext;
} ;

struct IotDBFdwOption iotdb_valid_options[] = 
{
    [0] = {"host",ForeignServerRelationId},
    [1] = {"port",ForeignServerRelationId},
    [2] = {"dbname",ForeignServerRelationId},  // stroage group 
    [3] = {"user",UserMappingRelationId},
    [4] = {"password",UserMappingRelationId},
    [5] = {"table",ForeignTableRelationId}

};

PG_FUNCTION_INFO_V1(iotdb_fdw_validator);
Datum iotdb_fdw_validator(PG_FUNCTION_ARGS);

bool iotdb_is_valid_option(const char *option, Oid context);

bool starts_with_http_or_https(const char *url);

//char *str_tolower(const char *str, size_t len);