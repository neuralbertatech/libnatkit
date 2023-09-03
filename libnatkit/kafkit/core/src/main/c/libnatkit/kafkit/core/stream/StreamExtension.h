#ifndef KAFKIT_CORE_STREAM_STREAM_EXTENSION_H_
#define KAFKIT_CORE_STREAM_STREAM_EXTENSION_H_

#include <assert.h>

typedef enum {
  STREAMEXTENSION_LB=0,
  STREAMEXTENSION_CORE,
  STREAMEXTENSION_EXECUTION,
  STREAMEXTENSION_HARDWARE,
  STREAMEXTENSION_LOGGING,
  STREAMEXTENSION_UB
} stream_extension_t;


// TODO: Return result rather than string
const char* streamExtensionToString(stream_extension_t extension) {
  switch (extension) {
    case STREAMEXTENSION_CORE:      return "Core";
    case STREAMEXTENSION_EXECUTION: return "Execution";
    case STREAMEXTENSION_HARDWARE:  return "Hardware";
    case STREAMEXTENSION_LOGGING:   return "Logging";
    default: assert(0);
  }
}

#endif // KAFKIT_CORE_STREAM_STREAM_EXTENSION_H_
