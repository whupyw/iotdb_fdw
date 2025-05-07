#define QUERY_REST 1
typedef struct IotDBRow {
  char **tuple;
} IotDBRow;

typedef struct TblInfo {
  char *measurement;
  char **tag;
  char **field;
  char **field_type;
  int tag_len;
  int field_len;
} TblInfo;

typedef union IoTDBValue {
  long long int i;
  double d;
  int b;
  char *s;
} IotDBValue;

typedef enum IotDBColumnType {
  IOTDB_UNKNOWN_KEY,
  IOTDB_TIME_KEY,
  IOTDB_TAG_KEY,
  IOTDB_FIELD_KEY,
} IoTDBColumnType;

typedef struct IotDBColumnInfo {
  char *colname;
  IoTDBColumnType col_type;
} IotDBColumnInfo;

typedef struct IotDBResult {
  IotDBRow *rows;
  int ncol;
  int nrow;
  char **colnums;
  char **tagkeys;
  int ntag;
} IotDBResult ;

struct IotDBQuery_return {
  IotDBResult *r0;
  char *r1;
};