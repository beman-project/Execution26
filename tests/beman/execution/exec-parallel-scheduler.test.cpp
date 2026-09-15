// src/beman/execution/tests/exec-parallel-scheduler.test.cpp       -*-C++-*-
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include <test/execution.hpp>
#include <beman/execution/detail/common.hpp>
#ifdef BEMAN_HAS_IMPORT_STD
import std;
#else
#include <atomic>
#include <concepts>
#include <condition_variable>
#include <cstddef>
#include <exception>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
#include <queue>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>
#endif
#ifdef BEMAN_HAS_MODULES
import beman.execution;
import beman.execution.detail.schedule_result_t;
import beman.execution.detail.thread_pool_backend;
#else
#include <beman/execution.hpp>
#include <beman/execution/detail/thread_pool_backend.hpp>
#endif

namespace {
namespace replaceability = test_std::parallel_scheduler_replacement;

struct proxy : replaceability::receiver_proxy {
    auto set_value() noexcept -> void override {}
    auto set_error(::std::exception_ptr) noexcept -> void override {}
    auto set_stopped() noexcept -> void override {}
};

struct bulk_proxy : replaceability::bulk_item_receiver_proxy {
    auto set_value() noexcept -> void override {}
    auto set_error(::std::exception_ptr) noexcept -> void override {}
    auto set_stopped() noexcept -> void override {}
    auto execute(::std::size_t, ::std::size_t) noexcept -> void override {}
};

struct backend : replaceability::parallel_scheduler_backend {
    auto schedule(replaceability::receiver_proxy&, ::std::span<::std::byte>) noexcept -> void override {}
    auto schedule_bulk_chunked(::std::size_t,
                               replaceability::bulk_item_receiver_proxy&,
                               ::std::span<::std::byte>) noexcept -> void override {}
    auto schedule_bulk_unchunked(::std::size_t,
                                 replaceability::bulk_item_receiver_proxy&,
                                 ::std::span<::std::byte>) noexcept -> void override {}
};

auto test_parallel_scheduler_synopsis() -> void {
    static_assert(!::std::default_initializable<test_std::parallel_scheduler>);
    static_assert(::std::copy_constructible<test_std::parallel_scheduler>);
    static_assert(::std::move_constructible<test_std::parallel_scheduler>);
    static_assert(test_std::scheduler<test_std::parallel_scheduler>);

    static_assert(::std::same_as<decltype(test_std::get_parallel_scheduler()), test_std::parallel_scheduler>);
    static_assert(::std::same_as<test_std::schedule_result_t<test_std::parallel_scheduler>,
                                 test_std::parallel_scheduler::sender>);
    static_assert(test_std::sender<test_std::parallel_scheduler::sender>);
    static_assert(::std::same_as<decltype(test_std::get_completion_signatures<test_std::parallel_scheduler::sender>()),
                                 test_std::completion_signatures<test_std::set_value_t(),
                                                                 test_std::set_error_t(::std::exception_ptr),
                                                                 test_std::set_stopped_t()>>);

    static_assert(
        noexcept(test_std::get_forward_progress_guarantee(::std::declval<const test_std::parallel_scheduler&>())));
    static_assert(::std::same_as<decltype(test_std::get_forward_progress_guarantee(
                                     ::std::declval<const test_std::parallel_scheduler&>())),
                                 test_std::forward_progress_guarantee>);
}

auto test_replaceability_synopsis() -> void {
    static_assert(::std::is_abstract_v<replaceability::receiver_proxy>);
    static_assert(::std::is_abstract_v<replaceability::bulk_item_receiver_proxy>);
    static_assert(::std::is_abstract_v<replaceability::parallel_scheduler_backend>);
    static_assert(::std::derived_from<bulk_proxy, replaceability::receiver_proxy>);
    static_assert(::std::derived_from<backend, replaceability::parallel_scheduler_backend>);
    static_assert(::std::same_as<decltype(::std::declval<proxy&>().template try_query<int>(0)), ::std::optional<int>>);
    static_assert(::std::same_as<decltype(replaceability::query_parallel_scheduler_backend()),
                                 ::std::shared_ptr<replaceability::parallel_scheduler_backend>>);
}

auto test_parallel_scheduler_schedule() -> void {
    auto sch = test_std::get_parallel_scheduler();
    {
        int i = 0;
        test_std::sync_wait(test_std::schedule(sch) | test_std::then([&i]() noexcept { i = 114514; }));
        ASSERT(i == 114514);
    }
    {
        bool invoked = false;
        test_std::sync_wait(test_std::schedule(sch) |
                            test_std::bulk(test_std::par, 0uz, [&invoked](std::size_t) noexcept { invoked = true; }));
        ASSERT(not invoked);
        test_std::sync_wait(
            test_std::schedule(sch) |
            test_std::bulk(test_std::unseq, 0uz, [&invoked](std::size_t) noexcept { invoked = true; }));
        ASSERT(not invoked);
        test_std::sync_wait(test_std::schedule(sch) |
                            test_std::bulk_chunked(test_std::par, 0uz, [&invoked](std::size_t, std::size_t) noexcept {
                                invoked = true;
                            }));
        ASSERT(not invoked);
        test_std::sync_wait(
            test_std::schedule(sch) |
            test_std::bulk_unchunked(test_std::par, 0uz, [&invoked](std::size_t) noexcept { invoked = true; }));
        ASSERT(not invoked);
    }
    {
        for (auto size : {1uz, 4uz, 8uz, 16uz, 32uz}) {
            std::vector<int> vec(size);
            std::iota(vec.begin(), vec.end(), 0);

            test_std::sync_wait(
                test_std::schedule(sch) |
                test_std::bulk(test_std::par, vec.size(), [&vec](std::size_t i) noexcept { vec[i] = 2 * vec[i]; }));
            for (std::size_t i = 0; i < vec.size(); ++i) {
                ASSERT(vec[i] == 2 * static_cast<int>(i));
            }

            test_std::sync_wait(
                test_std::schedule(sch) |
                test_std::bulk(test_std::seq, vec.size(), [&vec](std::size_t i) noexcept { ++vec[i]; }));
            for (std::size_t i = 0; i < vec.size(); ++i) {
                ASSERT(vec[i] == 2 * static_cast<int>(i) + 1);
            }
        }

        for (auto size : {1uz, 4uz, 8uz, 16uz, 32uz}) {
            std::vector<int> vec(size);
            std::iota(vec.begin(), vec.end(), 0);

            test_std::sync_wait(test_std::schedule(sch) |
                                test_std::bulk_unchunked(test_std::par, vec.size(), [&vec](std::size_t i) noexcept {
                                    vec[i] = 2 * vec[i];
                                }));
            for (std::size_t i = 0; i < vec.size(); ++i) {
                ASSERT(vec[i] == 2 * static_cast<int>(i));
            }

            test_std::sync_wait(
                test_std::schedule(sch) |
                test_std::bulk_unchunked(test_std::seq, vec.size(), [&vec](std::size_t i) noexcept { ++vec[i]; }));
            for (std::size_t i = 0; i < vec.size(); ++i) {
                ASSERT(vec[i] == 2 * static_cast<int>(i) + 1);
            }
        }
    }
}
} // namespace

#if !BEMAN_EXECUTION_WITH_DEFAULT_PARALLEL_SCHEDULER_BACKEND
namespace beman::execution::parallel_scheduler_replacement {
auto query_parallel_scheduler_backend() -> std::shared_ptr<parallel_scheduler_backend> {
    static auto backend = std::make_shared<::test_detail::thread_pool_backend>();
    return backend;
}
} // namespace beman::execution::parallel_scheduler_replacement
#endif // BEMAN_EXECUTION_WITH_DEFAULT_PARALLEL_SCHEDULER_BACKEND

TEST(exec_parallel_scheduler) {
    test_parallel_scheduler_synopsis();
    test_replaceability_synopsis();
    test_parallel_scheduler_schedule();
}
