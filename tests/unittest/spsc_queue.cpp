// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2021, Intel Corporation */

#include "spsc_queue.h"

#include <memory>
#include <rapidcheck.h>

#include "common/util.h"
#include "unittest.hpp"

int main()
{
	return run_test([] {
		return_check ret;

		ret += rc::check("verify expected number of enqueues", []() {
			const auto size = *rc::gen::inRange<std::size_t>(0, 1024) * 64;
			const std::string data(8, 'a');
			const size_t entry_size = data.size() + sizeof(uint64_t);
			const size_t usable_size = size - 8; /* One slot is reserved. */
			const size_t expected_inserted =
				(usable_size < entry_size) ? 0 : (usable_size - usable_size % entry_size);

			std::unique_ptr<spsc_queue, decltype(&spsc_queue_destroy)> queue(spsc_queue_new(size),
											 &spsc_queue_destroy);

			if (size == 0) {
				RC_ASSERT(queue.get() == nullptr);
				return;
			}

			size_t inserted = 0;
			while (true) {
				struct spsc_queue_src_descriptor descriptor = {.size = data.size(),
									       .data = (const uint8_t *)data.data()};
				int ret = spsc_queue_try_enqueue(queue.get(), &descriptor, 1);
				if (ret) {
					RC_ASSERT(inserted == expected_inserted);
					return;
				}

				inserted += entry_size;
			}
		});

		ret += rc::check("verify consume in fully populated queue", [](const std::string &data) {
			RC_PRE(data.size() > 0);

			const size_t entry_size = data.size() + sizeof(uint64_t);
			const auto min_size = entry_size + sizeof(uint64_t);
			const auto size = ALIGN_UP(*rc::gen::inRange<std::size_t>(min_size, min_size * 1024), 64ULL);

			std::unique_ptr<spsc_queue, decltype(&spsc_queue_destroy)> queue(spsc_queue_new(size),
											 &spsc_queue_destroy);

			const auto num_repeats = size / entry_size;
			std::vector<size_t> num_inserted(num_repeats, 0);
			std::vector<size_t> num_read(num_repeats, 0);

			/* Insert and read all the data num_repeats times. */
			for (size_t i = 0; i < num_repeats; i++) {
				while (true) {
					struct spsc_queue_src_descriptor descriptor = {
						.size = data.size(), .data = (const uint8_t *)data.data()};
					int ret = spsc_queue_try_enqueue(queue.get(), &descriptor, 1);
					if (ret) {
						break;
					}

					num_inserted[i]++;
				}

				while (true) {
					struct spsc_queue_src_descriptor descriptor1;
					struct spsc_queue_src_descriptor descriptor2;

					int ret = spsc_queue_try_dequeue_start(queue.get(), &descriptor1, &descriptor2);
					if (ret) {
						break;
					}

					RC_ASSERT(descriptor1.size + descriptor2.size == data.size());
					std::string read_data((char *)descriptor1.data, descriptor1.size);
					if (descriptor2.size) {
						read_data.append((char *)descriptor2.data, descriptor2.size);
					}

					RC_ASSERT(read_data == data);

					num_read[i]++;

					spsc_queue_dequeue_finish(queue.get(), descriptor1.size + descriptor2.size);
				}

				RC_ASSERT(num_inserted[i] == num_read[i]);
			}

			RC_ASSERT(num_inserted[0] == num_inserted[1]);
			// RC_ASSERT(all_equal(num_inserted));
			// RC_ASSERT(all_equal(num_read));
		});
	});
}
