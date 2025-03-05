CREATE FUNCTION iotdb_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION iotdb_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION iotdb_fdw_version()
    RETURNS pg_catalog.int4 STRICT
    AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FOREIGN DATA WRAPPER iotdb_fdw_handler  
    HANDLER iotdb_fdw_handler
    VALIDATOR iotdb_fdw_validator;