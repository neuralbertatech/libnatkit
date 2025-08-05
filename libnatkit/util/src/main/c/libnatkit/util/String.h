#ifndef LIBNATKIT_UTIL_STRING_H_
#define LIBNATKIT_UTIL_STRING_H_

#if defined(__cplusplus) || defined(__msvc_cplusplus)
extern "C" {
#endif

#include <libnatkit/util/Bool.h>
#include <stdlib.h>
#include <string.h>

typedef struct String {
    char* data;
    const size_t size;
    size_t* numberOfReferences;
    const bool_t heapAllocated;
} String;

char* cloneCharPtr(const char* string);
void freeAndClear(void** mem);
void freeStringValue(String* string);
void freeStringPtr(String** string);
void freeString(String** string);
String* newStringWithClone(const char* data, size_t size);
String createStringWithClone(const char* data, size_t size);
String createStringFromCharPtrWithClone(const char* data);
String createEmptyString();
String copyString(String string);


#if defined(__cplusplus) || defined(__msvc_cplusplus)
}
#endif

#endif // LIBNATKIT_UTIL_STRING_H_
