// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2021-2022, Intel Corporation */

#include <cstdint>
#include <vector>

#include <rapidcheck.h>

#include "stream_helpers.hpp"
#include "unittest.hpp"

namespace
{
void verify_region_empty(struct pmemstream *stream, struct pmemstream_region region)
{
	verify(stream.get(), region, {}, {});
}
} // namespace

int main(int argc, char *argv[])
{
	if (argc != 2) {
		std::cout << "Usage: " << argv[0] << " file-path" << std::endl;
		return -1;
	}

	auto path = std::string(argv[1]);

	return run_test([&] {
		return_check ret;

		ret += rc::check(
			"verify if iteration return proper elements after append", [&](const std::string &data) {
				auto stream = make_pmemstream(path, TEST_DEFAULT_BLOCK_SIZE, TEST_DEFAULT_STREAM_SIZE);
				auto region =
					initialize_stream_single_region(stream.get(), TEST_DEFAULT_REGION_SIZE, data);

				UT_ASSERTeq(pmemstream_append(stream.get(), region, nullptr, data.c_str(), data.size(),
							      nullptr),
					    0);

				verify_region_empty(stream.get(), region);

				UT_ASSERTeq(pmemstream_sync(stream.get(), region, PMEMSTREAM_MAX_OFFSET), 0);

				verify(stream.get(), region, {data}, {});
			});
	});
}
