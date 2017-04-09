#include "common/harness.h"

#include <vector>

#include "common/logger.h"
#include "optimizer/stats/column_stats.h"
#include "type/type.h"
#include "type/value.h"
#include "type/value_factory.h"

#define private public

namespace peloton {
namespace test {

using namespace optimizer;

class ColumnStatsTests : public PelotonTest {};

TEST_F(ColumnStatsTests, BasicTests) {
  ColumnStats colstats{0, 0, 0, type::Type::TypeId::INTEGER};
  for (int i = 0; i < 100000; i++) {
    type::Value v = type::ValueFactory::GetIntegerValue(i % 8765);
    colstats.AddValue(v);
  }

  printf("cardinality is %f \n", colstats.GetCardinality());
  std::vector<double> bounds = colstats.GetHistogramBound();
  EXPECT_EQ(colstats.GetFracNull(), 0);
}

} /* namespace test */
} /* namespace peloton */
