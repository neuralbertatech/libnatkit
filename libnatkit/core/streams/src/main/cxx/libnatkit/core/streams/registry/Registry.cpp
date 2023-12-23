#include <memory>

#include <libnatkit/core/streams/registry/Registry.hpp>
#include <libnatkit/core/streams/schemas/BasicMetaInfoSchema.hpp>

namespace nat::kafka {

std::unique_ptr<Registry> Registry::createDefaultInitalizeRegistry() {
  auto registry = std::make_unique<Registry>();
  BasicMetaInfoSchema::registerWithRegistry(*registry);

  return std::move(registry);
}

}
