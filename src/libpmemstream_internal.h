// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2021-2022, Intel Corporation */

/* Internal Header */

#ifndef LIBPMEMSTREAM_INTERNAL_H
#define LIBPMEMSTREAM_INTERNAL_H

#include "iterator.h"
#include "libpmemstream.h"
#include "region.h"
#include "span.h"
#include "offset_manager.h"

#ifdef __cplusplus
extern "C" {
#endif

#define PMEMSTREAM_PUBLISH_PERSIST (0)
#define PMEMSTREAM_PUBLISH_NOFLUSH_DATA (1 << 0)
/* don't flush nor drain span's metadata + data while publishing;
 * it makes sense only if a persist function is called later on. */
#define PMEMSTREAM_PUBLISH_NOFLUSH (11 << 0)

#define PMEMSTREAM_SIGNATURE ("PMEMSTREAM")
#define PMEMSTREAM_SIGNATURE_SIZE (64)

#define PMEMSTREAM_MAX_CONCURRENCY 64

struct pmemstream_data {
	struct pmemstream_header {
		char signature[PMEMSTREAM_SIGNATURE_SIZE];
		uint64_t stream_size;
		uint64_t block_size;
	} header;
	span_bytes spans[];
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

	// XXX: make this per region (move to region_context)
	struct offset_manager *offset_manager;

	// XXX: turn it into a datastructure so that thread_ids can be reused
	uint64_t thread_id_counter;
	pthread_key_t thread_id;
};

static inline uint8_t *pmemstream_offset_to_ptr(struct pmemstream *stream, uint64_t offset)
{
	return (uint8_t *)stream->data->spans + offset;
}

#ifdef __cplusplus
} /* end extern "C" */
#endif
#endif /* LIBPMEMSTREAM_INTERNAL_H */
