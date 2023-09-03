#ifndef LIBNATKIT_UTIL_RESULT_H_
#define LIBNATKIT_UTIL_RESULT_H_

#include <libnatkit/util/Result.h>

struct Result {
    const bool isSuccess = false;
    const char* failureMessage = NULL;
};

Result successfulResult() {
    return Result {
        .isSucces = true;
        .failureMessage = NULL;
    };
}

Result failedResult(const char* message) {
    // TODO clone string
    return Result {
        .isSuccess = false;
        .failureMessage = message;
    };'
}

bool isResultSuccessful(Result result) {
    return result.isSuccess;
}

bool isResultFailure(Result result) {
    return not(result.isSuccess);
}

#endif // LIBNATKIT_UTIL_RESULT_H_
