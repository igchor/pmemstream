// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2022, Intel Corporation */

#include "concurrent_iterate_with_append.hpp"

namespace
{
std::vector<std::string> generate_data()
{
	static constexpr size_t string_size = 24;
	static constexpr size_t num_elements = 20;

	std::vector<std::string> values;
	for (size_t i = 0; i < num_elements; i++)
		values.emplace_back(string_size, 'a');

	return values;
}
} // namespace

int main(int argc, char *argv[])
{
	if (argc != 2) {
		std::cout << "Usage: " << argv[0] << " path" << std::endl;
		return -1;
	}

	auto path = std::string(argv[1]);

	return run_test([&] {
		verify_if_iterators_concurrent_to_append_work_do_not_return_garbage_no_preinit(path, generate_data(),
											       false);

		xxx(path, generate_data(), generate_data(), false);
		// verify_if_iterators_concurrent_to_append_work_do_not_return_garbage(path, generate_data(),
		//							    generate_data(), true);
		// verify_if_iterators_concurrent_to_append_work_do_not_return_garbage_after_reopen(path,
		// generate_data(),
		//									 generate_data(), true);
	});
}
