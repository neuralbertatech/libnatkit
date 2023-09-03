#ifndef LIBNATKIT_UTIL_BOOL_H_
#define LIBNATKIT_UTIL_BOOL_H_

typedef char bool;
#define true 1
#define false 0

bool not(bool val) {
    if (val == 0) {
        return false;
    } else {
        return true;
    }
}

#endif // LIBNATKIT_UTIL_BOOL_H_
