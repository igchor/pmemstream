// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2021-2022, Intel Corporation */

#include <vector>

#include <rapidcheck.h>

#include "stream_helpers.hpp"
#include "unittest.hpp"

int main(int argc, char *argv[])
{
	if (argc != 2) {
		std::cout << "Usage: " << argv[0] << " file-path" << std::endl;
		return -1;
	}

	auto path = std::string(argv[1]);

	return run_test([&] {
		return_check ret;

		/* 1. Allocate region and init it with data.
		 * 2. Verify that all data matches.
		 * 3. Append extra_data to the end.
		 * 4. Verify that all data matches.
		 */
		ret += rc::check(
			"verify if iteration return proper elements after append",
			[&](const std::vector<std::string> &data, const std::vector<std::string> &extra_data,
			    bool reopen_after_init, bool reopen_after_append, bool user_created_runtime) {
				auto stream = make_pmemstream(path, TEST_DEFAULT_BLOCK_SIZE, TEST_DEFAULT_STREAM_SIZE);
				auto region =
					initialize_stream_single_region(stream.get(), TEST_DEFAULT_REGION_SIZE, data);
				verify(stream.get(), region, data, {});

				if (reopen_after_init)
					stream.reopen();

				pmemstream_region_runtime *runtime = NULL;
				if (user_created_runtime) {
					pmemstream_get_region_runtime(stream.get(), region, &runtime);
				}

				append(stream.get(), region, runtime, extra_data);

				if (reopen_after_append)
					stream.reopen();

				verify(stream.get(), region, data, extra_data);
			});

		/* verify if an entry of size = 0 can be appended and entry with size > region's size cannot */
		{
			const size_t max_size = 1024UL;
			auto stream = make_pmemstream(path, max_size, TEST_DEFAULT_STREAM_SIZE);
			auto region = initialize_stream_single_region(stream.get(), max_size, {});
			verify(stream.get(), region, {}, {});

			/* append an entry with size = 0 */
			std::string entry;
			auto ret =
				pmemstream_append(stream.get(), region, nullptr, entry.data(), entry.size(), nullptr);
			UT_ASSERTeq(ret, 0);
			verify(stream.get(), region, {entry}, {});

			/* and try to append entry with size bigger than region's size */
			entry = std::string(max_size + 1, 'W');
			ret = pmemstream_append(stream.get(), region, nullptr, entry.data(), entry.size(), nullptr);
			UT_ASSERTeq(ret, -1);
		}

		ret += rc::check("verify append will work until OOM", [&](bool reopen) {
			auto stream = make_pmemstream(path, TEST_DEFAULT_BLOCK_SIZE, TEST_DEFAULT_STREAM_SIZE);
			auto region = initialize_stream_single_region(stream.get(), TEST_DEFAULT_REGION_SIZE, {});

			size_t elems = 10;
			const size_t e_size = TEST_DEFAULT_REGION_SIZE / elems - TEST_DEFAULT_BLOCK_SIZE;
			std::string e = *rc::gen::container<std::string>(e_size, rc::gen::character<char>());

			if (reopen)
				stream.reopen();

			struct pmemstream_entry ne = {0}, prev_ne = {0};
			while (elems-- > 0) {
				auto ret = pmemstream_append(stream.get(), region, nullptr, e.data(), e.size(), &ne);
				RC_ASSERT(ret == 0);
				RC_ASSERT(ne.offset > prev_ne.offset);
				prev_ne = ne;
			}
			/* next append should not fit */
			auto ret = pmemstream_append(stream.get(), region, nullptr, e.data(), e.size(), &ne);
			RC_ASSERT(ne.offset == prev_ne.offset);
			/* XXX: should be updated with the real error code, when available */
			RC_ASSERT(ret == -1);
			e.resize(4);
			/* ... but smaller entry should fit just in */
			ret = pmemstream_append(stream.get(), region, nullptr, e.data(), e.size(), &ne);
			RC_ASSERT(ne.offset > prev_ne.offset);
			RC_ASSERT(ret == 0);
		});
	});
}
