#ifndef KAFKIT_CORE_STREAM_STREAM_H_
#define KAFKIT_CORE_STREAM_STREAM_H_

#include <libnatkit/kafkit/core/stream/StreamId.h>
#include <libnatkit/kafkit/core/stream/StreamType.h>
#include <libnatkit/util/Result.h>

#include <stdio.h>

#if defined(__cplusplus) || defined(__msvc_cplusplus)
extern "C" {
#endif

typedef struct Stream {
  const char* name;
  const stream_type_t type;
  const stream_id_t id;
  const char* encoder;
  const char* schema;
} Stream;

Stream* constructStreamByCopy(const char* name, const stream_type_t* type, const stream_id_t id, const char* encoder, const char* schema) {
  Stream* stream = (Stream*)malloc(sizeof(Stream));
  char* _name = (char*)malloc(sizeof(char) * 128);
  char* _encoder = (char*)malloc(sizeof(char) * 128);
  char* _schema = (char*)malloc(sizeof(char) * 128);

  strcpy(_name, name);
  strcpy(_encoder, encoder);
  strcpy(_schema, schema);

  Stream streamInit = {
    .name = _name,
    .type = *type,
    .id = id,
    .encoder = _encoder,
    .schema = _schema
  };
  memcpy(stream, &streamInit, sizeof(Stream));
  return stream;
}

void freeStream(Stream* stream) {
  if (stream != NULL) {
    free((char*)stream->name);
    free((char*)stream->encoder);
    free((char*)stream->schema);
    free(stream);
  }
}

static Result parseStreamType(const char* streamTypeString, unsigned int streamTypeStringLen, stream_type_t* returnValue) {
  for (int i = (int)(STREAMTYPE_LB + 1); i < (int)STREAMTYPE_UB; ++i) {
    stream_type_t stream_type = (stream_type_t)i;
    if (strcmp(streamTypeToString(stream_type), streamTypeString) == 0) {
      *returnValue = stream_type;
      return successfulResult();
    }
  }
  return failedResult("Could not find stream type");
  ///return 0;
}

Result parseStreamString(const char* streamString, Stream** returnStream) {
  const char* typeSeperator = strchr(streamString, '-');
  if (typeSeperator == NULL) {
    return failedResult("Could not find any '-' character in Stream string");
    //printf("Could not find any '-' character in Stream string '%s'\n", streamString);
    //return 0;
  }
  const char* typeString = (char*)streamString;
  unsigned int typeStringLen = typeSeperator - streamString;
  char* copiedTypeString = (char*)malloc(sizeof(char) * (typeStringLen + 1));
  strncpy(copiedTypeString, typeString, typeStringLen);
  copiedTypeString[typeStringLen] = '\0';
  stream_type_t streamType = createStreamTypeFromString(copiedTypeString);
  free(copiedTypeString);

  const char* idSeperator = strchr(typeSeperator+sizeof(char), '-');
  if (idSeperator == NULL) {
    return failedResult("Could not find second '-' character in Stream string");
    //uprintf("Could not find second '-' character in Stream string");
    //return 0;
  }
  char* idString = (char*)(typeSeperator + sizeof(char));
  unsigned int idStringLen = idSeperator - idString;
  char* idStringEnd = idString + idStringLen;

  const char* encoderSeperator = strchr(idSeperator+sizeof(char), '-');
  if (encoderSeperator == NULL) {
    return failedResult("Could not find third '-' character in Stream string");
    //printf("Could not find third '-' character in Stream string");
    //return 0;
  }
  const char* encoderString = idSeperator + 1;
  unsigned int encoderStringLen = encoderSeperator - encoderString;
  const char* encoderStringEnd = encoderString + encoderStringLen;

  const char* schemaString = encoderSeperator + 1;
  unsigned int schemaStringLen = strlen(streamString) - (schemaString - streamString);

  Result streamTypeParseResult = parseStreamType(typeString, typeStringLen, &streamType);
  if (isResultFailure(streamTypeParseResult)) {
    return streamTypeParseResult;
  }
  stream_id_t streamId = strtoll(idString, &idStringEnd, 10);
  char* streamEncoder = (char*)malloc((encoderStringLen + 1) * sizeof(char));
  char* streamSchema = (char*)malloc((schemaStringLen + 1) * sizeof(char));
  strncpy(streamEncoder, encoderString, encoderStringLen);
  strncpy(streamSchema, schemaString, schemaStringLen);
  streamEncoder[encoderStringLen] = '\0';
  streamSchema[schemaStringLen] = '\0';

  *returnStream = constructStreamByCopy(streamString, &streamType, streamId, streamEncoder, streamSchema);
  free(streamEncoder);
  free(streamSchema);

  return successfulResult();
  //return 1;
}

#if defined(__cplusplus) || defined(__msvc_cplusplus)
}
#endif

#endif // KAFKIT_CORE_STREAM_STREAM_H_
