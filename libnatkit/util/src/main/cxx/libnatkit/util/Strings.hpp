#pragma once

#include <vector>
#include <string>
#include <algorithm>

namespace nat::util::Strings {

std::vector<std::string> split(const std::string& string, char delimiter);

std::string toLowercase(const std::string& string);

char toLowercase(char character);

}
