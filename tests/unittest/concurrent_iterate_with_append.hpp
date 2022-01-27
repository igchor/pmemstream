// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2022, Intel Corporation */

#include <vector>

#include "stream_helpers.hpp"
#include "thread_helpers.hpp"
#include "unittest.hpp"

static constexpr size_t concurrency = 4;
static constexpr size_t max_size = 1024; /* Max number of elements in stream and max size of single entry. */
static constexpr size_t stream_size = max_size * max_size * concurrency * 10 /* 10x-margin */;
static constexpr size_t region_size = stream_size - STREAM_METADATA_SIZE;

namespace
{
void concurrent_iterate_verify(pmemstream *stream, pmemstream_region region, const std::vector<std::string> &data,
			       const std::vector<std::string> &extra_data)
{
	std::vector<std::string> result;

	struct pmemstream_entry_iterator *eiter;
	UT_ASSERT(pmemstream_entry_iterator_new(&eiter, stream, region) == 0);

	struct pmemstream_entry entry;

	/* Loop until all entries are found. */
	while (result.size() < data.size() + extra_data.size()) {
		int next = pmemstream_entry_iterator_next(eiter, NULL, &entry);
		if (next == 0) {
			auto data_ptr = reinterpret_cast<const char *>(pmemstream_entry_data(stream, entry));
			result.emplace_back(data_ptr, pmemstream_entry_length(stream, entry));
		}
	}

	UT_ASSERT(std::equal(data.begin(), data.end(), result.begin()));
	UT_ASSERT(
		std::equal(extra_data.begin(), extra_data.end(), result.begin() + static_cast<long long>(data.size())));

	pmemstream_entry_iterator_delete(&eiter);
}
} // namespace

void verify_if_iterators_concurrent_to_append_work_do_not_return_garbage_no_preinit(
	const std::string &path, std::vector<std::string> &&extra_data, bool use_region_runtime)
{
	auto stream = make_pmemstream(path, TEST_DEFAULT_BLOCK_SIZE, stream_size);
	auto region = initialize_stream_single_region(stream.get(), region_size, {});

	parallel_exec(concurrency, [&](size_t tid) {
		if (tid == 0) {
			/* appender */
			pmemstream_region_runtime *region_runtime = nullptr;
			if (use_region_runtime) {
				pmemstream_get_region_runtime(stream.get(), region, &region_runtime);
			}
			append(stream.get(), region, region_runtime, extra_data);
			verify(stream.get(), region, extra_data, {});
		} else {
			/* iterators */
			concurrent_iterate_verify(stream.get(), region, extra_data, {});
		}
	});
}

void verify_if_iterators_concurrent_to_append_work_do_not_return_garbage(const std::string &path,
									 const std::vector<std::string> &data,
									 const std::vector<std::string> &extra_data,
									 bool use_region_runtime)
{
	auto stream = make_pmemstream(path, TEST_DEFAULT_BLOCK_SIZE, stream_size);
	auto region = initialize_stream_single_region(stream.get(), region_size, data);

	parallel_exec(concurrency, [&](size_t tid) {
		if (tid == 0) {
			/* appender */
			pmemstream_region_runtime *region_runtime = nullptr;
			if (use_region_runtime) {
				pmemstream_get_region_runtime(stream.get(), region, &region_runtime);
			}
			append(stream.get(), region, region_runtime, extra_data);
			verify(stream.get(), region, data, extra_data);
		} else {
			/* iterators */
			concurrent_iterate_verify(stream.get(), region, data, extra_data);
		}
	});
}

void verify_if_iterators_concurrent_to_append_work_do_not_return_garbage_after_reopen(
	const std::string &path, const std::vector<std::string> &data, const std::vector<std::string> &extra_data,
	bool use_region_runtime)
{
	pmemstream_region region;
	{
		auto stream = make_pmemstream(path, TEST_DEFAULT_BLOCK_SIZE, stream_size);
		region = initialize_stream_single_region(stream.get(), region_size, data);
	}

	auto stream = make_pmemstream(path, TEST_DEFAULT_BLOCK_SIZE, stream_size, false);
	parallel_exec(concurrency, [&](size_t tid) {
		if (tid == 0) {
			/* appender */
			pmemstream_region_runtime *region_runtime = nullptr;
			if (use_region_runtime) {
				pmemstream_get_region_runtime(stream.get(), region, &region_runtime);
			}
			append(stream.get(), region, region_runtime, extra_data);
			verify(stream.get(), region, data, extra_data);
		} else {
			/* iterators */
			concurrent_iterate_verify(stream.get(), region, data, extra_data);
		}
	});
}

void xxx(const std::string &path, const std::vector<std::string> &data, const std::vector<std::string> &extra_data,
	 bool use_region_runtime)
{
	auto stream = make_pmemstream(path, TEST_DEFAULT_BLOCK_SIZE, stream_size);
	auto region = initialize_stream_single_region(stream.get(), region_size, data);

	pmemstream_region_runtime *region_runtime = nullptr;
	if (use_region_runtime) {
		pmemstream_get_region_runtime(stream.get(), region, &region_runtime);
	}
	append(stream.get(), region, region_runtime, extra_data);

	parallel_xexec(concurrency, [&](size_t tid, std::function<void(void)> syncthreads) {
		if (tid == 0) {
			/* appender */

			syncthreads();

			append(stream.get(), region, region_runtime, {std::string(1024, 'a')});

		} else {
			/* iterators */

			struct pmemstream_entry_iterator *eiter;
			UT_ASSERT(pmemstream_entry_iterator_new(&eiter, stream.get(), region) == 0);

			struct pmemstream_entry entry;

			/* Loop until all entries are found. */
			while (pmemstream_entry_iterator_next(eiter, NULL, &entry) == 0) {
				/* NOP */
			}

			syncthreads();

			int ret = pmemstream_entry_iterator_next(eiter, NULL, &entry);
			if (ret == 0) {
				auto data_ptr =
					reinterpret_cast<const char *>(pmemstream_entry_data(stream.get(), entry));
				auto str = std::string(data_ptr, pmemstream_entry_length(stream.get(), entry));
				UT_ASSERT(str == std::string(1024, 'a'));
			}
		}
	});
}
