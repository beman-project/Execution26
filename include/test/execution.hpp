// Copyright © 2024 Beman Project
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#ifndef INCLUDED_TEST_EXECUTION
#define INCLUDED_TEST_EXECUTION

#include <beman/stop_token.hpp>
#include <cassert>
#include <cstddef>

namespace test_std    = ::beman::cpp26;
namespace test_detail = ::beman::cpp26::detail;

namespace test
{
    template <typename> auto type_exists() {}
}

#endif // INCLUDED_TEST_EXECUTION