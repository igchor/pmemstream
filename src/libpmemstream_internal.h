// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2021, Intel Corporation */

/* Internal Header */

#ifndef LIBPMEMSTREAM_INTERNAL_H
#define LIBPMEMSTREAM_INTERNAL_H

#include "iterator.h"
#include "libpmemstream.h"
#include "region.h"
#include "span.h"
#include "spsc_queue.h"
#include "ringbuf.h"

#ifdef __cplusplus
extern "C" {
#endif

#define PMEMSTREAM_SIGNATURE ("PMEMSTREAM")
#define PMEMSTREAM_SIGNATURE_SIZE (64)

struct pmemstream_data {
	struct pmemstream_header {
		char signature[PMEMSTREAM_SIGNATURE_SIZE];
		uint64_t stream_size;
		uint64_t block_size;
	} header;
	span_bytes spans[];
};

struct memory_entry {
	uint64_t offset;
	uint8_t data[];
};

// XXX: replace this cb and cond with FUTURE?
typedef void (*pmemstream_on_append_commit_fn)(struct pmemstream *stream, uint64_t offset);

struct pmemstream_lane {
	uint64_t offset;
	uint64_t commited;
	pthread_cond_t cond;
	pmemstream_on_append_commit_fn* cb;
};

struct pmemstream {
	struct pmemstream_data *data;
	size_t stream_size;
	size_t usable_size;
	size_t block_size;

	pmem2_memcpy_fn memcpy;
	pmem2_memset_fn memset;
	pmem2_flush_fn flush;
	pmem2_drain_fn drain;
	pmem2_persist_fn persist;

	struct region_contexts_map *region_contexts_map;

	// XXX: iterators/users can read from spsc_queue using optimistic concurrency control,
	// to read commited but not yet persisted data.
	struct spsc_queue *spsc_memory_buffer;

	// XXX: cacheline aligned???
	struct pmemstream_lane lanes[1024];
	ringbuf_t *lanes_ringbuf;
	ringbuf_worker_t *ringbuf_workers[1024];
};

static inline uint8_t *pmemstream_offset_to_ptr(struct pmemstream *stream, uint64_t offset)
{
	return (uint8_t *)stream->data->spans + offset;
}

#ifdef __cplusplus
} /* end extern "C" */
#endif
#endif /* LIBPMEMSTREAM_INTERNAL_H */
