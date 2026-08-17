#include "RecordingState.hpp"

namespace nat {
namespace tools {
namespace {
std::atomic<bool> gRecordingActive{false};
}  // namespace

void setRecordingActive(const bool active) {
  gRecordingActive.store(active, std::memory_order_relaxed);
}

bool isRecordingActive() {
  return gRecordingActive.load(std::memory_order_relaxed);
}

}  // namespace tools
}  // namespace nat
