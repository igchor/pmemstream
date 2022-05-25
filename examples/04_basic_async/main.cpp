// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2022, Intel Corporation */

#include "examples_helpers.h"
#include "libpmemstream.h"

#include <cstdio>
#include <libminiasync.h>
#include <libpmem2.h>

#define EXAMPLE_ASYNC_COUNT 3

struct data_entry {
	uint64_t data;
};

/**
 * Show example usage of sync and async appends.
 * Each async append is executed in a different region.
 */
int main(int argc, char *argv[])
{
	if (argc != 2) {
		printf("Usage: %s file\n", argv[0]);
		return -1;
	}

	/* prepare stream and allocate or get a region */
	struct pmem2_map *map = example_map_open(argv[1], EXAMPLE_STREAM_SIZE);
	if (map == NULL) {
		pmem2_perror("pmem2_map");
		return -1;
	}

	struct pmemstream *stream;
	int ret = pmemstream_from_map(&stream, 4096, map);
	if (ret) {
		fprintf(stderr, "pmemstream_from_map failed\n");
		return ret;
	}

	/* get or allocate regions */
	struct pmemstream_region regions[EXAMPLE_ASYNC_COUNT];
	struct pmemstream_region_iterator *riter;
	ret = pmemstream_region_iterator_new(&riter, stream);
	if (ret) {
		fprintf(stderr, "pmemstream_region_iterator_new failed\n");
		return ret;
	}

	int i = 0;
	for (; i < EXAMPLE_ASYNC_COUNT; ++i) {
		ret = pmemstream_region_iterator_next(riter, &regions[i]);
		if (ret != 0) {
			break;
		}
	}
	/* if regions are missing - allocate them */
	for (; i < EXAMPLE_ASYNC_COUNT; ++i) {
		ret = pmemstream_region_allocate(stream, EXAMPLE_REGION_SIZE, &regions[i]);
		if (ret != 0) {
			fprintf(stderr, "pmemstream_region_allocate failed\n");
			return ret;
		}
	}
	pmemstream_region_iterator_delete(&riter);

	/* stream and regions are prepared, let's get to action */

	struct data_entry example_data[EXAMPLE_ASYNC_COUNT];
	example_data[0].data = 1;
	example_data[1].data = UINT64_MAX;
	example_data[2].data = 10000;
	struct pmemstream_entry entry;

	/*
	 * Example synchronous (regular) append
	 */
	ret = pmemstream_append(stream, regions[0], NULL, &example_data[0], sizeof(example_data[0]), &entry);
	if (ret) {
		fprintf(stderr, "pmemstream_append failed\n");
		return ret;
	}
	const struct data_entry *read_data = (const struct data_entry *)pmemstream_entry_data(stream, entry);
	printf("regular, synchronous append read data: %lu\n", read_data->data);

	/*
	 * Example asynchronous append, executed with libminiasync functions
	 */
	/* Prepare environment and start async appends. */
	struct data_mover_threads *dmt = data_mover_threads_default();
	if (dmt == NULL) {
		fprintf(stderr, "Failed to allocate data mover.\n");
		return -1;
	}
	struct vdm *thread_mover = data_mover_threads_get_vdm(dmt);

	uint64_t timestamp;
	for (int i = 0; i < EXAMPLE_ASYNC_COUNT; ++i) {
		ret = pmemstream_async_append(stream, thread_mover, regions[i], NULL, &example_data[i],
							    sizeof(example_data[i]), &timestamp);
		if (ret) {
			fprintf(stderr, "pmemstream_async_append failed\n");
			return ret;
		}
	}

	/* Now, wait until all appends complete. */
	pmemstream_async_wait_fut future = pmemstream_async_wait_persisted(stream, timestamp);

	bool completed = false;
	do {
		completed = future_poll(FUTURE_AS_RUNNABLE(&future)) == FUTURE_STATE_COMPLETED;

		/*
		 * an additional user/application work could be done here
		 */
		printf("User work done here...\n");
	} while (!completed);

	/* cleanup */
	data_mover_threads_delete(dmt);
	pmemstream_delete(&stream);
	pmem2_map_delete(&map);

	return 0;
}
