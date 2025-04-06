
extern "C" {
#include "include/iotdb_fdw.h"
// #include "utils/palloc.h"
#include "postgres.h"
// #include "utils/elog.h"
}
#include <memory>
#include <sstream>
#include <string>
#include <vector>

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
  size_t timeColumnIndex = std::string::npos;

  try {
    session->open();

    // Execute query and get dataset
    auto sessionDataset = session->executeQueryStatement(std::string(query));

    // Get column names and initialize result structure
    std::vector<std::string> columnNames = sessionDataset->getColumnNames();
    res->r0->ncol = columnNames.size();
    res->r0->colnums = (char **)palloc(sizeof(char *) * res->r0->ncol);

    for (size_t i = 0; i < columnNames.size(); ++i) {
      const std::string &name = columnNames[i];
      res->r0->colnums[i] = pstrdup(name.c_str());
    }

    sessionDataset->setFetchSize(1024);
    res->r0->nrow = 0;

    // Calculate rows count
    int row_cnt = 0;
    auto tempSessionData = session->executeQueryStatement(std::string(query));
    while (tempSessionData->hasNext()) {
      tempSessionData->next();
      row_cnt++;
    }

    res->r0->rows = (IotDBRow *)palloc(sizeof(IotDBRow) * row_cnt);

    // 查找时间戳列
    for (size_t i = 0; i < columnNames.size(); ++i) {
      if (columnNames[i] == "Time") {
        timeColumnIndex = i;
        break;
      }
    }

    // 处理所有行并填充 res->r0->rows
    int current_row = 0;
    while (sessionDataset->hasNext()) {
      std::string rowData = sessionDataset->next()->toString();
      std::vector<std::string> fields = splitString(rowData, '\t');
      IotDBRow &row = res->r0->rows[current_row];

      row.tuple = (char **)palloc(sizeof(char *) * res->r0->ncol);

      for (size_t i = 0; i < static_cast<size_t>(res->r0->ncol);
           i++) { // 强制转换 res->r0->ncol 到 size_t
        if (i == timeColumnIndex &&
            timeColumnIndex !=
                std::string::npos) { // 使用 std::string::npos 替代 -1
          // Convert timestamp from milliseconds to PostgreSQL TIMESTAMPTZ
          // format
          long long timestampMs = std::stoll(fields[i]);
          long long seconds = timestampMs / 1000;
          int milliseconds = timestampMs % 1000;

          char timestampStr[50];
          snprintf(timestampStr, sizeof(timestampStr), "%lld.%03d", seconds,
                   milliseconds);
          struct tm tmTime;
          memset(&tmTime, 0, sizeof(tmTime));
          strptime(timestampStr, "%s", &tmTime); // Assuming UTC time zone

          char formattedTime[50];
          strftime(formattedTime, sizeof(formattedTime), "%Y-%m-%d %H:%M:%S",
                   &tmTime);

          row.tuple[i] = pstrdup(formattedTime);
        } else {
          row.tuple[i] = pstrdup(fields[i].c_str());
        }
      }

      current_row++;
    }
    res->r0->nrow = row_cnt;
  } catch (const std::exception &e) {
    res->r1 = (char *)palloc0(sizeof(char *) * (strlen(e.what()) + 1));
    strcpy(res->r1, e.what());
  }

  return *res;
}
