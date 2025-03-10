#include "include/iotdb_fdw.h"
#include <regex.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include "include/option.h"


#include "access/reloptions.h"

#include "catalog/pg_collation_d.h"
#include "commands/defrem.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "regex/regex.h"
#include "utils/elog.h"
#include "utils/formatting.h"
//#include <cassert>
//#include <cstring>


Datum
iotdb_fdw_validator(PG_FUNCTION_ARGS)
{
    List *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
    Oid catalog = PG_GETARG_OID(1);
    ListCell *cell;

    foreach(cell, options_list)
    {
        DefElem     *def = (DefElem *)lfirst(cell);

        if( !iotdb_is_valid_option(def->defname, catalog))
        {
            struct IotDBFdwOption *opt;
            StringInfoData buf;

            initStringInfo(&buf);
            for(opt = iotdb_valid_options; opt->optname; opt++)
            {
                if(catalog == opt->optcontext)
                    appendStringInfo(&buf, "%s%s", (buf.len > 0) ? ", " : "", opt->optname);
            }

            ereport(ERROR,
                (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                 errmsg("invalid option \"%s\"", def->defname),
                 buf.len > 0
                 ? errhint("Valid options in this context are: %s",
                           buf.data)
                 : errhint("There are no valid options in this context.")));
        }
        
        // check whether the hostaddr is valid in the context
        if( strcmp(def->defname, "host") == 0 )
        {
            // regex_t regex;
            // int ret;
            char *host = str_tolower(defGetString(def), strlen(defGetString(def)), C_COLLATION_OID);


            // ret = regcomp(&regex, "~((http|https)://)", REG_EXTENDED);
            // Assert( ret == 0); // for debug invalid regex pattern
            // ret = regexec(&regex, host, 0, NULL, 0);
            // if( ret == REG_NOMATCH )
            // {
            //     regfree(&regex);
            //     elog(ERROR, "invalid host format, should be http:// or https://");
            // }
            
            // regfree(&regex);

            if( starts_with_http_or_https(host) == false )
            {
                elog(ERROR, "invalid host format, should be http:// or https://");
            }
            
        }
    }
    PG_RETURN_VOID();
}

bool iotdb_is_valid_option(const char *option, Oid context)
{
    struct IotDBFdwOption *opt;

    for(opt = iotdb_valid_options; opt->optname; opt++)
    {
        if(context == opt->optcontext && strcmp(opt->optname, option) == 0)
            return true;
    }

    return false;
}

// char *str_tolower(const char *buff, size_t nbytes)
// {
//     char *lower_Str = (char *)malloc(nbytes + 1);
//     if( !lower_Str )
//     {
//         elog(ERROR, "out of memory");
//     }

//     for( size_t i = 0; i < nbytes; i++ )
//     {
//         lower_Str[i] = tolower((unsigned char)buff[i]);
//     }
//     lower_Str[nbytes] = '\0';

//     return lower_Str;
// }

bool starts_with_http_or_https(const char *str)
{
    if( !str ) return false;
    if( strncmp(str, "http://", 7) == 0 || strncmp(str, "https://", 8) == 0 )
        return true;
    return false;
}

iotdb_opt *iotdb_get_options(Oid foreignoid, Oid userid) {
    ForeignTable *f_table = NULL;
    ForeignServer *f_server = NULL;
    UserMapping *f_mapping;
    List *options;
    ListCell *lc;
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
  
    foreach (lc, options) {
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