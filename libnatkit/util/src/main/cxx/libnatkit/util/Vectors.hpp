#pragma once

#include <memory>
#include <sstream>
#include <string>
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

template <typename T>
std::string toString(const std::vector<T>& vec) {
  std::stringstream ss;
  ss << "[";
  //std::string str = "[";
  for (unsigned int i = 0; i < vec.size(); ++i) {
    ss << vec[i];
    //str.append(std::to_string(vec[i]));
    if (i < vec.size() - 1)
      ss << ", ";
      //str.append(", ");
  }
  ss << "]";
  //str.append("]");
  return ss.str();
  //return str;
}

}
