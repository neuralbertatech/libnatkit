#include <libnatkit/util/Strings.hpp>


namespace nat::util::Strings {


std::vector<std::string> split(const std::string& string, char delimiter) {
    size_t prevIndex = 0;
    size_t nextIndex = 0;
    std::vector<std::string> strings;
    while(true) {
        nextIndex = string.find(delimiter, prevIndex);
        if (nextIndex == std::string::npos) {
        break;
    }
        strings.emplace_back(string.substr(prevIndex, nextIndex-prevIndex));
        prevIndex = nextIndex + 1;
    }
    strings.emplace_back(string.substr(prevIndex, string.size()-prevIndex));

    return strings;
}

std::string toLowercase(const std::string& string) {
    std::string lowercaseString{string};
    std::transform(lowercaseString.begin(), lowercaseString.end(), lowercaseString.begin(), [](const auto& character) { return std::tolower(character); });
    return lowercaseString;
}

char toLowercase(char character) {
    return (char)std::tolower(character);
}

}
