#include <libnatkit/util/String.h>

char* cloneCharPtr(const char* string) {
    int stringLen = strlen(string);
    char* clonedString = (char*)malloc(sizeof(char) * (stringLen + 1));
    strcpy(clonedString, string);
    clonedString[stringLen] = '\0';
    return clonedString;
}

void freeAndClear(void** mem) {
    free(*mem);
    *mem = NULL;
}

void freeStringValue(String* string) {
    if (string != NULL) {
        if (*(string->numberOfReferences) >= 0) {
            *(string->numberOfReferences) = 0;
            freeAndClear((void**)&(string->data));
            freeAndClear((void**)&(string->numberOfReferences));
       } else {
            --(*(string->numberOfReferences));
        }
    }
}

void freeStringPtr(String** string) {
    freeStringValue(*string);
    if ((*string)->data == NULL) {
        free(*string);
        *string = NULL;
    }
}

void freeString(String** string) {
    if (string != NULL && *string != NULL) {
        if ((*string)->heapAllocated == True) {
            freeStringPtr(string);
        } else {
            freeStringValue(*string);
        }
    }
}

static size_t* newNumberOfReferences() {
    size_t* references = (size_t*)malloc(sizeof(size_t));
    *references = 1;
    return references;
}

String* newStringWithClone(const char* data, size_t size) {
    String stringInit = {
        .data = cloneCharPtr(data),
        .size = size,
        .numberOfReferences = newNumberOfReferences(),
        .heapAllocated = True
    };
    String* string = (String*)malloc(sizeof(String));
    memcpy(string, &stringInit, sizeof(String));

    return string;
}

String createStringWithClone(const char* data, size_t size) {
    String string = {
        .data = cloneCharPtr(data),
        .size = size,
        .numberOfReferences = newNumberOfReferences(),
        .heapAllocated = False
    };

    return string;
}

String createStringFromCharPtrWithClone(const char* data) {
    return createStringWithClone(data, strlen(data));
}

String createEmptyString() {
    return createStringWithClone("", 0);
}

String copyString(String string) {
    ++(*(string.numberOfReferences));
    String copiedString = {
        .data = string.data,
        .size = string.size,
        .numberOfReferences = string.numberOfReferences,
        .heapAllocated = string.heapAllocated
    };

    return copiedString;
}