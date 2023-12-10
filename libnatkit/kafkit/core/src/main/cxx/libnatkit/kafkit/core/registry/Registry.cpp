#include <memory>

#include <libnatkit/kafkit/core/registry/Registry.hpp>
#include <libnatkit/kafkit/core/schemas/BasicMetaInfoSchema.hpp>

namespace nat::kafkit {

std::unique_ptr<Registry> Registry::createDefaultInitalizeRegistry() {
  auto registry = std::make_unique<Registry>();
  BasicMetaInfoSchema::registerWithRegistry(*registry);

  return std::move(registry);
}

}
