#include <libnatkit/util/Result.h>

#include <stdlib.h>

Result createResultWithCopy(bool_t isSuccess, const char* failureMessage) {
    Result result = {
        .isSuccess = isSuccess,
        .failureMessage = cloneCharPtr(failureMessage)
    };
    return result;
}

void freeResult(Result* result) {
    if (result != NULL) {
        free((char*)result->failureMessage);
        free(result);
    }
}

Result* newResultWithCopy(bool_t isSuccess, const char* failureMessage) {
    Result resultInit = {
        .isSuccess = isSuccess,
        .failureMessage = cloneCharPtr(failureMessage)
    };
    Result* result = (Result*)malloc(sizeof(Result));
    memcpy(result, &resultInit, sizeof(Result));
    return result;
}

Result createResultWithRef(bool_t isSuccess, const char* failureMessage) {
    Result result = {
        .isSuccess = isSuccess,
        .failureMessage = failureMessage
    };

    return result;
}

Result* newResultWithRef(bool_t isSuccess, const char* failureMessage) {
    Result resultInit = {
        .isSuccess = isSuccess,
        .failureMessage = failureMessage
    };
    Result* result = (Result*)malloc(sizeof(Result));
    memcpy(result, &resultInit, sizeof(Result));
    return result;
}

Result successfulResult() {
    return createResultWithRef(True, NULL);
}

Result failedResult(const char* message) {
    return createResultWithCopy(False, message);
}

bool_t isResultSuccessful(Result result) {
    return result.isSuccess;
}

bool_t isResultFailure(Result result) {
    return Not(result.isSuccess);
}
