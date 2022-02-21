// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2022, Intel Corporation */

#include "region.h"

#include <vector>

#include "thread_helpers.hpp"
#include "unittest.hpp"

namespace
{
static constexpr size_t concurrency = 100;
static constexpr size_t num_repeats = 1000;

// auto make_region_runtimes_map = make_instance_ctor(region_runtimes_map_new, region_runtimes_map_destroy);
} // namespace

/* XXX: create similar test for region_runtime_initialize_for_write_locked */
int main(int argc, char *argv[])
{
	if (argc != 2) {
		std::cout << "Usage: " << argv[0] << " file-path" << std::endl;
		return -1;
	}

	// XXX

	auto path = std::string(argv[1]);

	return 0;
}
