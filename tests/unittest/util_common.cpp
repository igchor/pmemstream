// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2022, Intel Corporation */

/* util_common.cpp -- tests for common helpers in util.c */

#include "common/util.h"

#include <rapidcheck.h>

#include "unittest.hpp"
#include "thread_helpers.hpp"
#include <cstring>

void test_versioned_lock()
{
	std::vector<std::string> possible_strings = {
		std::string(127, 'a'),
		std::string(127, 'b'),
		std::string(127, 'c'),
		std::string(127, 'd'),
		std::string(127, 'e')
	};

	char shared_buf[128];
	memcpy(shared_buf, possible_strings[0].data(), 128);

	static constexpr uint64_t locked_bit = (1ULL << 63);
	uint64_t version_lock = 0;

	parallel_exec(10, [&](size_t id){
		if (id == 0) {
			for (size_t i = 0; i < 100; i++) {
				uint64_t next_value = version_lock + 1;
				__atomic_store_n(&version_lock, next_value | locked_bit, __ATOMIC_RELAXED);
				__atomic_thread_fence(__ATOMIC_RELEASE);

				memcpy(shared_buf, possible_strings[i % possible_strings.size()].data(), 128);

				__atomic_thread_fence(__ATOMIC_RELEASE);
				__atomic_store_n(&version_lock, next_value, __ATOMIC_RELAXED);

				std::this_thread::sleep_for(std::chrono::milliseconds(1));
			}
		} else {
			for (size_t i = 0; i < 1000; i++) {
				char buf[128];
				uint64_t val = __atomic_load_n(&version_lock, __ATOMIC_RELAXED);
				while (true) {
					if (val & locked_bit) {
						val = __atomic_load_n(&version_lock, __ATOMIC_RELAXED);
						continue;
					}

					uint64_t read_val;
					optimistic_read(&version_lock, &read_val, buf, shared_buf, 128);
					
					if (val == read_val) {
						for (size_t i = 0; i < 127; i++) {
							if (buf[i] != buf[0]) throw std::runtime_error(buf);
						}
						break;
					}

					val = read_val;
				}
			}
		}
	});	
}

int main()
{
	return run_test([] {
		return_check ret;

		ret += rc::check("check if IS_POW2 == (x == 1 << log2(x))", [](const size_t value) {
			UT_ASSERTeq(IS_POW2(value), (value == (1UL << log2_uint(value))));
		});

		test_versioned_lock();
	});
}
