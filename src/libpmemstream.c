// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2021, Intel Corporation */

/* Implementation of public C API */

#include "common/util.h"
#include "libpmemstream_internal.h"

#include <assert.h>
#include <errno.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

static int pmemstream_is_initialized(struct pmemstream *stream)
{
	if (strcmp(stream->data->header.signature, PMEMSTREAM_SIGNATURE) != 0) {
		return -1;
	}
	if (stream->data->header.block_size != stream->block_size) {
		return -1; // todo: fail with incorrect args or something
	}
	if (stream->data->header.stream_size != stream->stream_size) {
		return -1; // todo: fail with incorrect args or something
	}

	return 0;
}

static void pmemstream_init(struct pmemstream *stream)
{
	stream->memset(stream->data->header.signature, 0, PMEMSTREAM_SIGNATURE_SIZE,
		       PMEM2_F_MEM_NONTEMPORAL | PMEM2_F_MEM_NODRAIN);
	stream->data->header.stream_size = stream->stream_size;
	stream->data->header.block_size = stream->block_size;
	stream->persist(stream->data, sizeof(struct pmemstream_data));

	span_create_empty(stream, 0, stream->usable_size - SPAN_EMPTY_METADATA_SIZE);
	stream->memcpy(stream->data->header.signature, PMEMSTREAM_SIGNATURE, strlen(PMEMSTREAM_SIGNATURE),
		       PMEM2_F_MEM_NONTEMPORAL);
}

int pmemstream_from_map(struct pmemstream **stream, size_t block_size, size_t buffer_size, struct pmem2_map *map)
{
	struct pmemstream *s = malloc(sizeof(struct pmemstream));
	s->data = pmem2_map_get_address(map);
	s->stream_size = pmem2_map_get_size(map);
	s->usable_size = ALIGN_DOWN(s->stream_size - sizeof(struct pmemstream_data), block_size);
	s->block_size = block_size;

	s->memcpy = pmem2_get_memcpy_fn(map);
	s->memset = pmem2_get_memset_fn(map);
	s->persist = pmem2_get_persist_fn(map);
	s->flush = pmem2_get_flush_fn(map);
	s->drain = pmem2_get_drain_fn(map);

	if (pmemstream_is_initialized(s) != 0) {
		pmemstream_init(s);
	}

	s->region_contexts_map = region_contexts_map_new();
	if (!s->region_contexts_map) {
		return -1;
	}

	s->spsc_memory_buffer = spsc_queue_new(buffer_size);
	if (!s->spsc_memory_buffer) {
		// XXX: free regions
		return -1;
	}

	*stream = s;

	return 0;
}

void pmemstream_delete(struct pmemstream **stream)
{
	struct pmemstream *s = *stream;
	region_contexts_map_destroy(s->region_contexts_map);
	free(s);
	*stream = NULL;
}

// stream owns the region object - the user gets a reference, but it's not
// necessary to hold on to it and explicitly delete it.
int pmemstream_region_allocate(struct pmemstream *stream, size_t size, struct pmemstream_region *region)
{
	const uint64_t offset = 0;
	struct span_runtime srt = span_get_runtime(stream, offset);

	if (srt.type != SPAN_EMPTY)
		return -1;

	size = ALIGN_UP(size, stream->block_size);

	span_create_region(stream, offset, size);
	region->offset = offset;

	return 0;
}

size_t pmemstream_region_size(struct pmemstream *stream, struct pmemstream_region region)
{
	struct span_runtime region_srt = span_get_region_runtime(stream, region.offset);

	return region_srt.region.size;
}

int pmemstream_region_free(struct pmemstream *stream, struct pmemstream_region region)
{
	struct span_runtime srt = span_get_runtime(stream, region.offset);

	if (srt.type != SPAN_REGION)
		return -1;

	span_create_empty(stream, 0, stream->usable_size - SPAN_EMPTY_METADATA_SIZE);

	region_contexts_map_remove(stream->region_contexts_map, region);

	return 0;
}

// returns pointer to the data of the entry
void *pmemstream_entry_data(struct pmemstream *stream, struct pmemstream_entry entry)
{
	struct span_runtime entry_srt = span_get_entry_runtime(stream, entry.offset);

	return pmemstream_offset_to_ptr(stream, entry_srt.data_offset);
}

// returns the size of the entry
size_t pmemstream_entry_length(struct pmemstream *stream, struct pmemstream_entry entry)
{
	struct span_runtime entry_srt = span_get_entry_runtime(stream, entry.offset);

	return entry_srt.entry.size;
}

int pmemstream_get_region_context(struct pmemstream *stream, struct pmemstream_region region,
				  struct pmemstream_region_context **region_context)
{
	return region_contexts_map_get_or_create(stream->region_contexts_map, region, region_context);
}

static int pmemstream_submit_data(struct pmemstream *stream, uint64_t offset, const void *data, size_t size)
{
	struct spsc_queue_src_descriptor descriptors[2];
	descriptors[0].size = sizeof(offset);
	descriptors[0].data = (const uint8_t *)&offset;

	descriptors[1].size = size;
	descriptors[1].data = data;

	return spsc_queue_try_enqueue(stream->spsc_memory_buffer, descriptors, 2);
}

// XXX: alternative submit data: do memcpy in main thread, just offload flushing
// static int pmemstream_submit_data(struct pmemstream *stream, uint64_t offset, const void *data, size_t size)
// {
// 	struct spsc_queue_src_descriptor descriptor;
// 	descriptor.size = size;
// 	descriptor.data = data;

// 	stream->memcpy(stream->span->data + offset, data, size, PMEM2_F_MEM_NODRAIN);

// 	return spsc_queue_try_enqueue(stream->spsc_memory_buffer, &descriptor, 1);
// }

static void pmemstream_set_persisted_offset(struct pmemstream *stream, struct pmemstream_region region,
					    uint64_t persisted_offset)
{
	span_bytes *region_span = span_offset_to_span_ptr(stream, region.offset);
	region_span[1] = persisted_offset;
	stream->persist(region_span + 1, sizeof(region_span[1]));
}

static uint64_t pmemstream_get_persisted_offset(struct pmemstream *stream, struct pmemstream_region region)
{
	span_bytes *region_span = span_offset_to_span_ptr(stream, region.offset);
	return region_span[1];
}

// Might be called from different thread than append
void pmemstream_persist(struct pmemstream *stream, struct pmemstream_region region, struct pmemstream_entry entry)
{
	uint64_t persisted_offset = pmemstream_get_persisted_offset(stream, region);

	if (persisted_offset >= entry.offset)
		return;

	while (persisted_offset < entry.offset) {
		struct spsc_queue_src_descriptor descriptor1;
		struct spsc_queue_src_descriptor descriptor2;
		int ret = spsc_queue_try_dequeue_start(stream->spsc_memory_buffer, &descriptor1, &descriptor2);
		/* There is nothing in the queue, we must have persisted everything */
		// XXX: what about writes which omitted the buffer?
		assert(ret == 0);

		// XXX: strict aliasing?
		struct memory_entry *me = (struct memory_entry *)descriptor1.data;

		span_create_entry(stream, me->offset, me->data, descriptor1.size);
		persisted_offset = me->offset + descriptor1.size;

		spsc_queue_dequeue_finish(stream->spsc_memory_buffer, descriptor1.size);
		// XXX - handle descriptor 2
	}
	assert(persisted_offset > pmemstream_get_persisted_offset(stream, region));
	stream->drain();

	pmemstream_set_persisted_offset(stream, region, persisted_offset);
}

// XXX: alternative pmemstream_persist which just flushes:
// TODO

// synchronously appends data buffer to the end of the region
int pmemstream_append(struct pmemstream *stream, struct pmemstream_region region,
		      struct pmemstream_region_context *region_context, const void *data, size_t size,
		      struct pmemstream_entry *new_entry)
{
	size_t entry_total_size = size + SPAN_ENTRY_METADATA_SIZE;
	size_t entry_total_size_span_aligned = ALIGN_UP(entry_total_size, sizeof(span_bytes));
	struct span_runtime region_srt = span_get_region_runtime(stream, region.offset);
	int ret = 0;

	if (!region_context) {
		ret = pmemstream_get_region_context(stream, region, &region_context);
		if (ret) {
			return ret;
		}
	}

	ret = region_try_recover_locked(stream, region, region_context);
	if (ret) {
		return ret;
	}

	uint64_t offset = region_context->append_offset;
	uint64_t new_offset = offset + entry_total_size_span_aligned;

	/* region overflow (no space left) or offset outside of region. */
	if (new_offset + entry_total_size_span_aligned > region.offset + region_srt.total_size) {
		return -1;
	}
	/* offset outside of region */
	if (new_offset < region_srt.data_offset) {
		return -1;
	}

	if (new_entry) {
		new_entry->offset = offset;
	}

	ret = pmemstream_submit_data(stream, offset, data, size);
	if (ret) {
		// XXX: just copy the data ourselves, without using buffer?
	}

	region_context->append_offset = new_offset;
	region_context->commited_offset = new_offset;

	return 0;
}
