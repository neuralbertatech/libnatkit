#ifndef LIBNATKIT_UTIL_RESULT_H_
#define LIBNATKIT_UTIL_RESULT_H_

#if defined(__cplusplus) || defined(__msvc_cplusplus)
extern "C" {
#endif

#include <libnatkit/util/Bool.h>
#include <libnatkit/util/String.h>

typedef struct Result {
    const bool_t isSuccess;
    const char* failureMessage;
} Result;

void freeResult(Result* result);

Result createResultWithCopy(bool_t isSuccess, const char* failureMessage);

Result* newResultWithCopy(bool_t isSuccess, const char* failureMessage);

Result createResultWithRef(bool_t isSuccess, const char* failureMessage);

Result* newResultWithRef(bool_t isSuccess, const char* failureMessage);

Result successfulResult();

Result failedResult(const char* message);

bool_t isResultSuccessful(const Result result);

bool_t isResultFailure(const Result result);

#if defined(__cplusplus) || defined(__msvc_cplusplus)
}
#endif

#endif // LIBNATKIT_UTIL_RESULT_H_
