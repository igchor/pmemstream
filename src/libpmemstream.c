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

	for (int i = 0; i < 1024; i++) {
		// XXX: error
		pthread_cond_init(&s->lanes[i].cond, NULL);
	}

	size_t ringbuf_obj_size;
	size_t ringbuf_worker_size;
	ringbuf_get_sizes(1024, &ringbuf_obj_size, &ringbuf_worker_size);
	
	// XXX: errors
	s->lanes_ringbuf = malloc(ringbuf_obj_size);
	ringbuf_setup(s->lanes_ringbuf, 1024, 1024);

	for (int i = 0; i < 1024; i++) {
		s->lane[i].worker = ringbuf_register(s->lanes_ringbuf, i);
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

// static int pmemstream_submit_data(struct pmemstream *stream, uint64_t offset, const void *data, size_t size)
// {
// 	struct spsc_queue_src_descriptor descriptors[2];
// 	descriptors[0].size = sizeof(offset);
// 	descriptors[0].data = (const uint8_t *)&offset;

// 	descriptors[1].size = size;
// 	descriptors[1].data = data;

// 	return spsc_queue_try_enqueue(stream->spsc_memory_buffer, descriptors, 2);
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

// Returns a worker to pool
static void pmemstream_release_lane(struct pmemstream *stream, struct pmemstream_lane *lane)
{
	// XXX:
}

// Returns first available worker. Waits if none is available
static struct pmemstream_lane* pmemstream_acquire_lane(struct pmemstream *stream)
{
	// XXX (use some kind of lock-free stack????)
	return &stream->lanes[0];
}

static void pmemstream_on_append_commit_cb(struct pmemstream *stream, struct pmemstream_lane *lane)
{
	
}

static int pmemstream_submit_data(struct pmemstream *stream, ringbuf_worker_t *worker, struct pmemstream_region_context *region_context, uint64_t offset, const void *data, size_t size, void *cb_arg)
{
	/* local_append_offset is basically X bytes after append_offset. X is equal to sum of sizes from other appends which happened before. */
	/* As a bonuse local_append_offset is also lane ID. NOT REAYLLU, 1 has to be replaced to make offset thing work */
	size_t local_append_offset = ringbuf_acquire(stream->lanes_ringbuf, worker, 1);
	size_t append_offset = pmemstream_get_persisted_offset(stream, region) + local_append_offset;

	struct pmemstream_lane *lane = &stream->lanes[local_append_offset];

	stream->memcpy(pmemstream_offset_to_ptr(stream, append_offset), data, size, PMEM2_F_MEM_NODRAIN);
	lane->offset = append_offset;

	ringbuf_produce(stream->lanes_ringbuf, worker);

	// XXX: wrap those into futures
	pthread_cond_wait(&lane->commit_cond, /*TODO: MUTEX*/);   
	pthread_cond_wait(&lane->persistent_cond, /*TODO: MUTEX*/);   
}

// Might be called from different thread than append
void pmemstream_persist(struct pmemstream *stream, struct pmemstream_region region, struct pmemstream_entry entry)
{
	uint64_t persisted_offset = pmemstream_get_persisted_offset(stream, region);

	while (persisted_offset < entry.offset) {
		/* process transactions in order of offsets. */
		size_t local_append_offset;
		size_t to_consume = ringbuf_consume(stream->lanes_ringbuf, &local_append_offset);

		for (size_t i = 0; i < to_consume)

		stream->persist(pmemstream_offset_to_ptr(stream, persisted_offset + local_append_offset), to_consume);

		persisted_offset += to_consume;
		pmemstream_set_persisted_offset(stream, region, persisted_offset);

		// XXX: avoid spin, add conditional variable or something
	}
}

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

	struct pmemstream_lane *lane = pmemstream_acquire_lane(stream);

	pmemstream_submit_data(stream, lane, offset, data, size);

	region_context->append_offset = new_offset;
	region_context->commited_offset = new_offset;

	return 0;
}
