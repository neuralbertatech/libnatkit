#pragma once

#include <memory>
#include <vector>

namespace nat::util::Vectors {

template <typename T>
std::vector<std::unique_ptr<T>> wrapContainedValueWithUnique(const std::vector<T>& vec) {
  std::vector<std::unique_ptr<T>> wrappedVec{};
  for (const auto& val : vec) {
	  wrappedVec.emplace_back(std::make_unique<T>(val));
  }

  return wrappedVec;
}

}
