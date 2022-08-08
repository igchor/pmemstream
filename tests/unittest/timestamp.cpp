// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2022, Intel Corporation */

#include <random>

#include "libpmemstream.h"
#include "rapidcheck_helpers.hpp"
#include "stream_helpers.hpp"
#include "thread_helpers.hpp"
#include "unittest.hpp"
#include <signal.h>

int error_in_miniasync = 0;
thread_local sigjmp_buf longjmp_buf;

void sighandler(int sig)
{
	if (sig == SIGSEGV)
		siglongjmp(longjmp_buf, sig);
	else
		exit(128 + sig);
}


int handle_sigjump()
{
	thread_local int first = 0;
	if (first == 0) {
		signal(SIGSEGV, sighandler);
		first = 1;
		return 0;
	}

	if (sigsetjmp(longjmp_buf, 1) != 0) {
		test_dump_backtrace();
		error_in_miniasync = 1;
		return 1;
	}

	return 0;
}

void handle_miniasync_error()
{
	if (error_in_miniasync) {
		throw std::runtime_error("Signal in miniasync!");
	}
}

/**
 * timestamp - unit test for testing method pmemstream_entry_timestamp()
 */

void multithreaded_asynchronous_append(pmemstream_test_base &stream, const std::vector<pmemstream_region> &regions,
				       const std::vector<std::vector<std::string>> &data)
{
	stream.helpers.thread_mover_handle = std::unique_ptr<struct data_mover_threads, decltype(&data_mover_threads_delete)>(data_mover_threads_new(12, 128, FUTURE_NOTIFIER_WAKER, handle_sigjump), &data_mover_threads_delete);

	using future_type = decltype(stream.helpers.async_append(regions[0], data[0]));
	std::vector<std::vector<future_type>> futures(data.size());

	parallel_exec(data.size(), [&](size_t thread_id) {
		if (sigsetjmp(longjmp_buf, 1) != 0) {
			throw std::runtime_error("Signal handled!");
		}

		for (auto &chunk : data) {
			futures[thread_id].emplace_back(stream.helpers.async_append(regions[thread_id], chunk));
		}

		handle_miniasync_error();
	});
	handle_miniasync_error();
	for (auto &future_sequence : futures) {
		std::mt19937_64 g(*rc::gen::arbitrary<size_t>());
		std::shuffle(future_sequence.begin(), future_sequence.end(), g);
	}

	parallel_exec(data.size(), [&](size_t thread_id) {
		handle_miniasync_error();
		for (auto &fut : futures[thread_id]) {
			while (fut.poll() != FUTURE_STATE_COMPLETE)
				handle_miniasync_error();
		}
	});
}

void multithreaded_synchronous_append(pmemstream_test_base &stream, const std::vector<pmemstream_region> &regions,
				      const std::vector<std::vector<std::string>> &data)
{
	parallel_exec(data.size(), [&](size_t thread_id) {
		for (auto &chunk : data) {
			stream.helpers.append(regions[thread_id], chunk);
		}
	});
}

size_t get_concurrency_level(test_config_type &config, const std::vector<pmemstream_region> &regions)
{
	return std::min(regions.size(), config.max_concurrency);
}

std::tuple<std::vector<pmemstream_region>, size_t> generate_and_append_data(pmemstream_with_multi_empty_regions &stream,
									    test_config_type &test_config, bool async)
{
	auto regions = stream.helpers.get_regions();
	size_t concurrency_level = get_concurrency_level(test_config, regions);

	RC_TAG(concurrency_level);

	/* Multithreaded append to many regions with global ordering. */
	const auto data = *rc::gen::container<std::vector<std::vector<std::string>>>(
	 	concurrency_level, rc::gen::arbitrary<std::vector<std::string>>());

	if (async) {
		multithreaded_asynchronous_append(stream, regions, data);
	} else {
		multithreaded_synchronous_append(stream, regions, data);
	}

	size_t elements = 0;
	for (auto &chunk : data) {
		elements += chunk.size();
	}

	return std::tie(regions, elements);
}

size_t remove_random_region(pmemstream_with_multi_empty_regions &stream, std::vector<pmemstream_region> &regions)
{
	size_t pos = *rc::gen::inRange<size_t>(0, regions.size());
	auto region_to_remove = regions[pos];
	auto region_size = stream.sut.region_size(region_to_remove);
	UT_ASSERTeq(stream.helpers.remove_region(region_to_remove.offset), 0);
	regions.erase(regions.begin() + static_cast<int>(pos));
	return region_size;
}

int main(int argc, char *argv[])
{
	if (argc != 2) {
		std::cout << "Usage: " << argv[0] << " file-path" << std::endl;
		return -1;
	}

	struct test_config_type test_config;
	test_config.filename = std::string(argv[1]);

	signal(SIGSEGV, sighandler);

	return run_test(test_config, [&] {
		return_check ret;

		ret += rc::check(
			"timestamp values should globally increase in multi-region environment after asynchronous append",
			[&](pmemstream_with_multi_empty_regions &&stream) {
				if (sigsetjmp(longjmp_buf, 1) != 0) {
					throw std::runtime_error("Signal handled!");
				}

				auto [regions, elements] =
					generate_and_append_data(stream, test_config, true /* async */);

				handle_miniasync_error();

				/* Global ordering validation */
				UT_ASSERT(stream.helpers.validate_timestamps_no_gaps(regions));
			});
	});
}
