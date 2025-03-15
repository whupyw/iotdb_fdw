
#include <memory>
#include <sstream>
#include <string>
#include <vector>
// #include "utils/palloc.h"
extern "C" {
#include "include/iotdb_fdw.h"
}

// #include "include/query_cpp.h"
#include "include/include/Session.h"

#include <cstddef>
/*
  typedef struct InfluxDBResult {
        InfluxDBRow *rows;  // tuples
        int ncol;           // num of columns
        int nrow;           // num of rows
        char **columns;     // co-col name
        char **tagkeys;     // tags
        int ntag;           // num of tags
} InfluxDBResult
*/

std::vector<std::string> splitString(const std::string &str, char delimiter) {
  std::vector<std::string> result;
  std::stringstream ss(str);
  std::string token;

  while (std::getline(ss, token, delimiter)) {
    result.push_back(token);
  }

  return result;
}

extern "C" struct IotDBQuery_return
IotDBQuery(char *query, UserMapping *user, iotdb_opt *opts, IotDBType *ctypes,
           IotDBValue *cvalues, int cparamNum) {
  
  // init res
  IotDBQuery_return *res =
      (IotDBQuery_return *)palloc0(sizeof(IotDBQuery_return));
      // init r0
  res->r0 = (IotDBResult *)palloc0(sizeof(IotDBResult));
  
      Session *session = new Session(opts->svr_address, opts->svr_port,
                                 opts->svr_username, opts->svr_password);

  //   new Session("127.0.0.1", opts->svr_port, opts->svr_username,
  //               opts->svr_password);
  session->open();

  // query
  auto sessionDataset = session->executeQueryStatement(std::string(query));

  // res->r0->ncol = sessionDataset->getColumnSize();
  //  init result->colname && ncol
  std::vector<std::string> columnNames = sessionDataset->getColumnNames();

  res->r0->ncol = columnNames.size();
  res->r0->colnums = (char **)palloc(sizeof(char *) * res->r0->ncol);

  for (size_t i = 0; i < columnNames.size(); ++i) {
    const std::string &name = columnNames[i];
    res->r0->colnums[i] = pstrdup(name.c_str());
  }

  sessionDataset->setFetchSize(1024);
  res->r0->nrow = 0;

  // calculate rows
  int row_cnt = 0;
  // here need to optimize
  auto tempSessionData = session->executeQueryStatement(std::string(query));
  while (tempSessionData->hasNext()) {
    tempSessionData->next();
    row_cnt++;
  }

  res->r0->rows = (IotDBRow *)palloc(sizeof(IotDBRow) * row_cnt);

  // traversal all rows and fill in res->r0->rows
  int current_row = 0;
  while (sessionDataset->hasNext()) {

    std::string rowStr = sessionDataset->next()->toString();
    IotDBRow &row = res->r0->rows[current_row];

    std::vector<std::string> fields = splitString(rowStr, '\t');

    row.tuple = (char **)palloc(sizeof(char *) * res->r0->ncol);

    for (int i = 0; i < res->r0->ncol; i++) {
      row.tuple[i] = pstrdup(fields[i].c_str());
    }

    current_row++;
  }
  res->r0->nrow = row_cnt;

  return *res;
}
