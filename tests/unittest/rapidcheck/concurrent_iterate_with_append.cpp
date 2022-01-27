// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2022, Intel Corporation */

#include "../concurrent_iterate_with_append.hpp"

#include <vector>
#include <functional>

#include <rapidcheck.h>

#include "env_setter.hpp"

int main(int argc, char *argv[])
{
	if (argc != 2) {
		std::cout << "Usage: " << argv[0] << " path" << std::endl;
		return -1;
	}

	auto path = std::string(argv[1]);

	return run_test([&] {
		return_check ret;

		/* Disable shrinking and set max_size of entries. */
		/* XXX: can we do this via rapidcheck API? */
		std::string rapidcheck_config = "noshrink=1 max_size=" + std::to_string(max_size);
		env_setter setter("RC_PARAMS", rapidcheck_config, false);

		ret += rc::check(
			"verify_if_iterators_concurrent_to_append_work_do_not_return_garbage_no_preinit",
			[&](std::vector<std::string> &&extra_data, bool use_region_runtime) {
				verify_if_iterators_concurrent_to_append_work_do_not_return_garbage_no_preinit(path, std::move(extra_data), use_region_runtime);
			});

		ret += rc::check("verify if iterators concurrent to append work do not return garbage",
					[&](const std::vector<std::string> &data, const std::vector<std::string> &extra_data, bool use_region_runtime) {
				verify_if_iterators_concurrent_to_append_work_do_not_return_garbage(path, data, extra_data, use_region_runtime);
			});

		ret += rc::check(
			"verify if iterators concurrent to append work do not return garbage after reopen",
								[&](const std::vector<std::string> &data, const std::vector<std::string> &extra_data, bool use_region_runtime) {
				verify_if_iterators_concurrent_to_append_work_do_not_return_garbage_after_reopen(path, data, extra_data, use_region_runtime);
			});
	});
}
