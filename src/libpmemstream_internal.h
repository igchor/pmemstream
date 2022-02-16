// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2021-2022, Intel Corporation */

/* Internal Header */

#ifndef LIBPMEMSTREAM_INTERNAL_H
#define LIBPMEMSTREAM_INTERNAL_H

#include <assert.h>

#include "iterator.h"
#include "libpmemstream.h"
#include "region.h"
#include "span.h"

#ifdef __cplusplus
extern "C" {
#endif

#define PMEMSTREAM_SIGNATURE ("PMEMSTREAM")
#define PMEMSTREAM_SIGNATURE_SIZE (64)

struct pmemstream_header {
	char signature[PMEMSTREAM_SIGNATURE_SIZE];
	uint64_t stream_size;
	uint64_t block_size;
	uint64_t persisted_timestamp;
};

struct pmemstream_data_runtime {
	span_bytes *spans;

	pmem2_memcpy_fn memcpy;
	pmem2_memset_fn memset;
	pmem2_flush_fn flush;
	pmem2_drain_fn drain;
	pmem2_persist_fn persist;
};

struct pmemstream {
	/* Points to pmem-resided header. */
	struct pmemstream_header *header;

	/* Describes data location and memory operations. */
	struct pmemstream_data_runtime data;

	size_t stream_size;
	size_t usable_size;
	size_t block_size;

	uint64_t next_timestamp;

	struct region_runtimes_map *region_runtimes_map;
};

static inline const uint8_t *pmemstream_offset_to_ptr(const struct pmemstream_data_runtime *data, uint64_t offset)
{
	return (const uint8_t *)data->spans + offset;
}

/* Convert offset to pointer to span. offset must be 8-bytes aligned. */
static inline const struct span_base *span_offset_to_span_ptr(const struct pmemstream_data_runtime *data,
							      uint64_t offset)
{
	assert(offset % sizeof(struct span_base) == 0);
	return (const struct span_base *)pmemstream_offset_to_ptr(data, offset);
}

static inline uint64_t pmemstream_next_timestamp(struct pmemstream* stream)
{
	return __atomic_fetch_add(&stream->next_timestamp, 1, __ATOMIC_RELAXED);
}

#ifdef __cplusplus
} /* end extern "C" */
#endif
#endif /* LIBPMEMSTREAM_INTERNAL_H */
