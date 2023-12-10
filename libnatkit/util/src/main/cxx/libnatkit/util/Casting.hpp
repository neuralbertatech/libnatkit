#pragma once

#include <memory>


namespace nat::util {

template<typename T, typename S, typename Deleter>
auto dynamic_pointer_cast(std::unique_ptr<S, Deleter>&& p) noexcept
{
    auto converted = std::unique_ptr<T, Deleter>{dynamic_cast<T*>(p.get())};
    if (converted) {
        std::swap(converted.get_deleter(), p.get_deleter());
        p.release();            // no longer owns the pointer
    }
    return converted;
}

template<typename T>
std::shared_ptr<T> asShared(std::unique_ptr<T>&& ptr) {
  return ptr;
}

}
