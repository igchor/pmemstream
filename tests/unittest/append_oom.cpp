// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2021-2022, Intel Corporation */

#include <cstdint>
#include <vector>

#include "rapidcheck_helpers.hpp"
#include "stream_helpers.hpp"
#include "unittest.hpp"

int main(int argc, char *argv[])
{
	if (argc != 2) {
		std::cout << "Usage: " << argv[0] << " file-test_config.filename" << std::endl;
		return -1;
	}

	test_config.filename = std::string(argv[1]);

	return run_test([&] {
		return_check ret;

		ret += rc::check(
			"verify append will work until OOM", [&](pmemstream_empty &&stream, const std::string &value) {
				RC_PRE(value.size() > 0);

				auto region = stream.sut.helpers.initialize_single_region(
					REGION_METADATA_SIZE + *rc::gen::inRange<size_t>(0, TEST_DEFAULT_REGION_SIZE),
					{});

				while (true) {
					auto [ret, new_entry] = stream.sut.append(region, value);
					if (ret != 0) {
						/* XXX: should be updated with the real error code, when available */
						UT_ASSERTeq(ret, -1);
						break;
					}
				}
			});
	});
}
