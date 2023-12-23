#ifndef kafka_CORE_STREAM_STREAM_TYPE_H_
#define kafka_CORE_STREAM_STREAM_TYPE_H_

#include <assert.h>
#include <ctype.h>
#include <stddef.h>
#include <string.h>
#include <stdlib.h>

typedef enum {
  STREAMTYPE_LB=0,

  // Core types
  STREAMTYPE_DATA,
  STREAMTYPE_META,

  // Execution Extension
  STREAMTYPE_EXECUTION_COMMAND,

  // Hardware Extension
  STREAMTYPE_HARDWARE_STATUS,
  STREAMTYPE_HARDWARE_CONFIGURATION,

  // Logging Extension
  STREAMTYPE_LOGGING_LOG,
  STREAMTYPE_LOGGING_HEARTBEAT,

  STREAMTYPE_UB,
  STREAMTYPE_INVALID
} stream_type_t;

stream_type_t createStreamTypeFromString(const char* streamTypeString) {
  size_t stringLen = strlen(streamTypeString);
  char* lowerCaseString = (char*)malloc(sizeof(char) * (stringLen + 1));
  for (size_t i = 0; i < stringLen; ++i) {
    lowerCaseString[i] = tolower(streamTypeString[i]);
  }
  lowerCaseString[stringLen] = '\0';

  stream_type_t streamType = STREAMTYPE_INVALID;
  if (strcmp("data", lowerCaseString) == 0) {
    streamType = STREAMTYPE_DATA;
  } else if (strcmp("meta", lowerCaseString) == 0) {
    streamType = STREAMTYPE_META;
  } else if (strcmp("command", lowerCaseString) == 0) {
    streamType = STREAMTYPE_EXECUTION_COMMAND;
  } else if (strcmp("status", lowerCaseString) == 0) {
    streamType = STREAMTYPE_HARDWARE_STATUS;
  } else if (strcmp("configuration", lowerCaseString) == 0) {
    streamType = STREAMTYPE_HARDWARE_CONFIGURATION;
  } else if (strcmp("log", lowerCaseString) == 0) {
    streamType = STREAMTYPE_LOGGING_LOG;
  } else if (strcmp("heartbeat", lowerCaseString) == 0) {
    streamType = STREAMTYPE_LOGGING_HEARTBEAT;
  }

  free(lowerCaseString);
  return streamType;
}


// TODO: Return result rather than string
const char* streamTypeToString(stream_type_t type) {
  switch (type) {
    case STREAMTYPE_DATA: return "Data";
    case STREAMTYPE_META: return "Meta";

    case STREAMTYPE_EXECUTION_COMMAND: return "Command";

    case STREAMTYPE_HARDWARE_STATUS:        return "HardwareStatus";
    case STREAMTYPE_HARDWARE_CONFIGURATION: return "HardwareConfiguration";

    case STREAMTYPE_LOGGING_LOG:       return "LoggingLog";
    case STREAMTYPE_LOGGING_HEARTBEAT: return "LoggingHeartbeat";

    default: assert(0);
  }

  return NULL;
}

#endif // kafka_CORE_STREAM_STREAM_TYPE_H_
