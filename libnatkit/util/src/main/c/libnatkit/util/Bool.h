#ifndef LIBNATKIT_UTIL_BOOL_H_
#define LIBNATKIT_UTIL_BOOL_H_

#if defined(__cplusplus) || defined(__msvc_cplusplus)
extern "C" {
#endif


///typedef char bool_t;

//#if !defined(__cplusplus) && !defined(__msvc_cplusplus)
#define True 1
#define False 0
#define bool_t char

bool_t Not(bool_t val) {
    if (val == 0) {
        return True;
    } else {
        return False;
    }
}

//#endif

#if defined(__cplusplus) || defined(__msvc_cplusplus)
}
#endif

#endif // LIBNATKIT_UTIL_BOOL_H_
