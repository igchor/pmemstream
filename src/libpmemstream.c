// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2021-2022, Intel Corporation */

/* Implementation of public C API */

#include "common/util.h"
#include "libpmemstream_internal.h"
#include "region_allocator/region_allocator.h"

#include <assert.h>
#include <errno.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

static int pmemstream_is_initialized(struct pmemstream *stream)
{
	if (strcmp(stream->header->signature, PMEMSTREAM_SIGNATURE) != 0) {
		return -1;
	}
	if (stream->header->block_size != stream->block_size) {
		return -1; // todo: fail with incorrect args or something
	}
	if (stream->header->stream_size != stream->stream_size) {
		return -1; // todo: fail with incorrect args or something
	}

	return 0;
}

static void pmemstream_init(struct pmemstream *stream)
{
	stream->data.memset(stream->header->signature, 0, PMEMSTREAM_SIGNATURE_SIZE,
			    PMEM2_F_MEM_NONTEMPORAL | PMEM2_F_MEM_NODRAIN);

	allocator_initialize(&stream->data, &stream->header->region_allocator_header, stream->usable_size);

	stream->header->stream_size = stream->stream_size;
	stream->header->block_size = stream->block_size;
	stream->header->timestamp = 0;
	stream->data.persist(stream->header, sizeof(struct pmemstream_header));

	stream->data.memcpy(stream->header->signature, PMEMSTREAM_SIGNATURE, strlen(PMEMSTREAM_SIGNATURE),
			    PMEM2_F_MEM_NONTEMPORAL);
}

static size_t pmemstream_header_size_aligned(size_t block_size)
{
	return ALIGN_UP(sizeof(struct pmemstream_header), block_size);
}

static size_t pmemstream_usable_size(size_t stream_size, size_t block_size)
{
	assert(stream_size >= pmemstream_header_size_aligned(block_size));
	assert(stream_size - pmemstream_header_size_aligned(block_size) >= block_size);
	return ALIGN_DOWN(stream_size - pmemstream_header_size_aligned(block_size), block_size);
}

static int pmemstream_validate_sizes(size_t block_size, struct pmem2_map *map)
{
	if (block_size == 0) {
		return -1;
	}

	if (block_size % CACHELINE_SIZE != 0) {
		return -1;
	}

	if (!IS_POW2(block_size)) {
		return -1;
	}

	if (map == NULL) {
		return -1;
	}

	size_t stream_size = pmem2_map_get_size(map);
	if (stream_size > PTRDIFF_MAX) {
		return -1;
	}

	if (stream_size <= pmemstream_header_size_aligned(block_size)) {
		return -1;
	}

	if (pmemstream_usable_size(stream_size, block_size) <= sizeof(struct span_region)) {
		return -1;
	}

	if (pmemstream_usable_size(stream_size, block_size) < block_size) {
		return -1;
	}

	return 0;
}

int pmemstream_from_map(struct pmemstream **stream, size_t block_size, struct pmem2_map *map)
{
	if (!stream) {
		return -1;
	}

	if (pmemstream_validate_sizes(block_size, map)) {
		return -1;
	}

	struct pmemstream *s = malloc(sizeof(struct pmemstream));
	if (!s) {
		return -1;
	}

	size_t spans_offset = pmemstream_header_size_aligned(block_size);
	s->header = pmem2_map_get_address(map);
	s->stream_size = pmem2_map_get_size(map);
	s->usable_size = pmemstream_usable_size(s->stream_size, block_size);
	s->block_size = block_size;

	s->data.base = ((uint8_t *)pmem2_map_get_address(map)) + spans_offset;
	s->data.memcpy = pmem2_get_memcpy_fn(map);
	s->data.memset = pmem2_get_memset_fn(map);
	s->data.persist = pmem2_get_persist_fn(map);
	s->data.flush = pmem2_get_flush_fn(map);
	s->data.drain = pmem2_get_drain_fn(map);

	if (pmemstream_is_initialized(s) != 0) {
		pmemstream_init(s);
	}

	allocator_runtime_initialize(&s->data, &s->header->region_allocator_header);

	s->region_runtimes_map = region_runtimes_map_new(&s->data);
	if (!s->region_runtimes_map) {
		goto err;
	}

	*stream = s;
	return 0;

err:
	free(s);
	return -1;
}

void pmemstream_delete(struct pmemstream **stream)
{
	if (!stream) {
		return;
	}
	struct pmemstream *s = *stream;
	region_runtimes_map_destroy(s->region_runtimes_map);
	free(s);
	*stream = NULL;
}

static size_t pmemstream_region_total_size_aligned(struct pmemstream *stream, size_t size)
{
	struct span_region span_region = {.span_base = span_base_create(size, SPAN_REGION)};
	return ALIGN_UP(span_get_total_size(&span_region.span_base), stream->block_size);
}

// stream owns the region object - the user gets a reference, but it's not
// necessary to hold on to it and explicitly delete it.
int pmemstream_region_allocate(struct pmemstream *stream, size_t size, struct pmemstream_region *region)
{
	// XXX: lock

	if (!stream || !size) {
		return -1;
	}

	size_t total_size = pmemstream_region_total_size_aligned(stream, size);
	size_t requested_size = total_size - sizeof(struct span_region);

	const uint64_t offset =
		allocator_region_allocate(&stream->data, &stream->header->region_allocator_header, requested_size);
	if (offset == PMEMSTREAM_INVALID_OFFSET) {
		return -1;
	}

	if (region) {
		region->offset = offset;
	}

#ifndef NDEBUG
	const struct span_base *span_base = span_offset_to_span_ptr(&stream->data, offset);
	const struct span_region *span_region = (const struct span_region *)span_base;
	assert(offset % stream->block_size == 0);
	assert(span_get_type(span_base) == SPAN_REGION);
	assert(span_get_total_size(span_base) == total_size);
	assert(((uintptr_t)span_region->data) % CACHELINE_SIZE == 0);
#endif

	return 0;
}

size_t pmemstream_region_size(struct pmemstream *stream, struct pmemstream_region region)
{
	int ret = pmemstream_validate_stream_and_offset(stream, region.offset);
	if (ret) {
		return 0;
	}

	const struct span_base *span_region = span_offset_to_span_ptr(&stream->data, region.offset);
	assert(span_get_type(span_region) == SPAN_REGION);
	return span_get_size(span_region);
}

int pmemstream_region_free(struct pmemstream *stream, struct pmemstream_region region)
{
	// XXX: unlock

	int ret = pmemstream_validate_stream_and_offset(stream, region.offset);
	if (ret) {
		return ret;
	}

	allocator_region_free(&stream->data, &stream->header->region_allocator_header, region.offset);
	region_runtimes_map_remove(stream->region_runtimes_map, region);

	return 0;
}

// returns pointer to the data of the entry
const void *pmemstream_entry_data(struct pmemstream *stream, struct pmemstream_entry entry)
{
	int ret = pmemstream_validate_stream_and_offset(stream, entry.offset);
	if (ret) {
		return NULL;
	}

	struct span_entry *span_entry = (struct span_entry *)span_offset_to_span_ptr(&stream->data, entry.offset);
	return span_entry->data;
}

// returns the size of the entry
size_t pmemstream_entry_length(struct pmemstream *stream, struct pmemstream_entry entry)
{
	int ret = pmemstream_validate_stream_and_offset(stream, entry.offset);
	if (ret) {
		return 0;
	}
	struct span_entry *span_entry = (struct span_entry *)span_offset_to_span_ptr(&stream->data, entry.offset);
	return span_get_size(&span_entry->span_base);
}

int pmemstream_region_runtime_initialize(struct pmemstream *stream, struct pmemstream_region region,
					 struct pmemstream_region_runtime **region_runtime)
{
	int ret = pmemstream_validate_stream_and_offset(stream, region.offset);
	if (ret) {
		return ret;
	}

	ret = region_runtimes_map_get_or_create(stream->region_runtimes_map, region, region_runtime);
	if (ret) {
		return ret;
	}

	assert(*region_runtime);

	return region_runtime_initialize_for_write_locked(stream, region, *region_runtime, PMEMSTREAM_OFFSET_INVALID);
}

static size_t pmemstream_entry_total_size_aligned(size_t size)
{
	struct span_entry span_entry = {.span_base = span_base_create(size, SPAN_ENTRY)};
	return span_get_total_size(&span_entry.span_base);
}

int pmemstream_reserve(struct pmemstream *stream, struct pmemstream_region region,
		       struct pmemstream_region_runtime *region_runtime, size_t size,
		       struct pmemstream_entry *reserved_entry, void **data_addr)
{
	int ret = pmemstream_validate_stream_and_offset(stream, region.offset);
	if (ret) {
		return ret;
	}

	size_t entry_total_size_span_aligned = pmemstream_entry_total_size_aligned(size);
	const struct span_base *span_region = span_offset_to_span_ptr(&stream->data, region.offset);
	assert(span_get_type(span_region) == SPAN_REGION);

	if (!reserved_entry) {
		return -1;
	}

	if (!region_runtime) {
		ret = pmemstream_region_runtime_initialize(stream, region, &region_runtime);
		if (ret) {
			return ret;
		}
	}

	uint64_t offset = region_runtime_get_append_offset_acquire(region_runtime);
	assert(offset >= region.offset + offsetof(struct span_region, data));
	if (offset + entry_total_size_span_aligned > region.offset + span_get_total_size(span_region)) {
		return -1;
	}

	region_runtime_increase_append_offset(region_runtime, entry_total_size_span_aligned);

	reserved_entry->offset = offset;
	/* data is right after the entry metadata */
	*data_addr = (void *)span_offset_to_span_ptr(&stream->data, offset + sizeof(struct span_entry));

	return ret;
}

int pmemstream_publish(struct pmemstream *stream, struct pmemstream_region region,
		       struct pmemstream_region_runtime *region_runtime, const void *data, size_t size,
		       struct pmemstream_entry reserved_entry)
{
	int ret = pmemstream_validate_stream_and_offset(stream, region.offset);
	if (ret) {
		return ret;
	}
	if (stream->header->stream_size <= reserved_entry.offset) {
		return -1;
	}

	if (!region_runtime) {
		ret = pmemstream_region_runtime_initialize(stream, region, &region_runtime);
		if (ret) {
			return ret;
		}
	}

	struct span_entry span_entry = {.span_base = span_base_create(size, SPAN_ENTRY)};

	uint8_t *destination = (uint8_t *)span_offset_to_span_ptr(&stream->data, reserved_entry.offset);

	// get timestamp for the entry(ies)
	uint64_t timestamp = acquire_timestamp(stream);

	stream->data.memcpy(destination, &span_entry, sizeof(span_entry), PMEM2_F_MEM_NOFLUSH);
	stream->data.memcpy(destination + pmemstream_entry_total_size_aligned(size), &timestamp, sizeof(timestmap), PMEM2_F_MEM_NOFLUSH);

	// memset data right after entry to make recovery easier
	stream->data.memset(destination + pmemstream_entry_total_size_aligned(size) + sizeof(timestamp), 0, 8, PMEM2_F_MEM_NOFLUSH);
	/* 'data' is already copied - we only need to persist. */
	stream->data.persist(destination, pmemstream_entry_total_size_aligned(size));

	// mark timestamp as ready to be commited/persisted - at this point data is fully written to pmem.
	produce_timestamp(stream, timestamp);

	return 0;
}

// synchronously appends data buffer to the end of the region
int pmemstream_append(struct pmemstream *stream, struct pmemstream_region region,
		      struct pmemstream_region_runtime *region_runtime, const void *data, size_t size,
		      struct pmemstream_entry *new_entry)
{
	if (!region_runtime) {
		int ret = pmemstream_region_runtime_initialize(stream, region, &region_runtime);
		if (ret) {
			return ret;
		}
	}

	struct pmemstream_entry reserved_entry;
	void *reserved_dest;
	int ret = pmemstream_reserve(stream, region, region_runtime, size, &reserved_entry, &reserved_dest);
	if (ret) {
		return ret;
	}

	stream->data.memcpy(reserved_dest, data, size, PMEM2_F_MEM_NOFLUSH);

	pmemstream_publish(stream, region, region_runtime, data, size, reserved_entry);

	if (new_entry) {
		new_entry->offset = reserved_entry.offset;
	}

	return 0;
}

static void publish_to_append_map(struct future_context *publish_ctx, struct future_context *append_ctx, void *arg)
{
	struct pmemstream_async_publish_output *publish_output = future_context_get_output(publish_ctx);
	struct pmemstream_async_append_output *append_output = future_context_get_output(append_ctx);

	append_output->status = publish_output->status;
}

static enum future_state pmemstream_async_publish_impl(struct future_context *ctx, struct future_notifier *notifier)
{
	/* XXX: properly use/fix notifier */
	if (notifier != NULL) {
		notifier->notifier_used = FUTURE_NOTIFIER_NONE;
	}

	struct pmemstream_async_publish_data *data = future_context_get_data(ctx);
	struct pmemstream_async_publish_output *out = future_context_get_output(ctx);

	int ret = pmemstream_publish(data->stream, data->region, data->region_runtime, data->data, data->size,
				     data->reserved_entry);
	out->status = ret;

	return FUTURE_STATE_COMPLETE;
}

struct pmemstream_async_publish_fut pmemstream_async_publish(struct pmemstream *stream, struct pmemstream_region region,
							     struct pmemstream_region_runtime *region_runtime,
							     const void *data, size_t size,
							     struct pmemstream_entry reserved_entry)
{
	struct pmemstream_async_publish_fut future = {0};
	future.data.stream = stream;
	future.data.region = region;
	future.data.region_runtime = region_runtime;
	future.data.data = data;
	future.data.size = size;
	future.data.reserved_entry = reserved_entry;

	FUTURE_INIT(&future, pmemstream_async_publish_impl);

	return future;
}

// asynchronously appends data buffer to the end of the region
struct pmemstream_async_append_fut pmemstream_async_append(struct pmemstream *stream, struct vdm *vdm,
							   struct pmemstream_region region,
							   struct pmemstream_region_runtime *region_runtime,
							   const void *data, size_t size)
{
	struct pmemstream_async_append_fut future = {0};

	if (!region_runtime) {
		int ret = pmemstream_region_runtime_initialize(stream, region, &region_runtime);
		if (ret) {
			/* return future already completed, with the error code set */
			future.output.status = ret;
			FUTURE_INIT_COMPLETE(&future);
			return future;
		}
	}

	struct pmemstream_entry reserved_entry;
	void *reserved_dest;
	int ret = pmemstream_reserve(stream, region, region_runtime, size, &reserved_entry, &reserved_dest);
	if (ret) {
		/* return future already completed, with the error code set */
		future.output.status = ret;
		FUTURE_INIT_COMPLETE(&future);
		return future;
	}
	future.output.new_entry = reserved_entry;

	/* at this point, we have to chain tasks needed to complete an append and initialize the future */
	FUTURE_CHAIN_ENTRY_INIT(&future.data.memcpy,
				vdm_memcpy(vdm, reserved_dest, (void *)data, size, PMEM2_F_MEM_NOFLUSH), NULL, NULL);
	FUTURE_CHAIN_ENTRY_INIT(&future.data.publish,
				pmemstream_async_publish(stream, region, region_runtime, data, size, reserved_entry),
				publish_to_append_map, NULL);
	// FUTURE_CHAIN_ENTRY_INIT(&future.data.persist, ...); // this future, on poll would just query the
	// persisted offset and if persisted one is >= than this entry offset it would return FUTURE_COMPLETED.

	// Additionally, 'status' variable in *_output structures should specify which stage the future is at.
	// E.g. it should be set to STATE_COMMITED after publish completes and STATE_PERSISTED after offset is
	// persisted.

	// XXX: do we want to have 2 types of futures (just for checking state and for doing actual work)?
	// Rationale: runtime can implement efficient checks for multiple in-progress futures
	// Instead of using chained futures, we could create pmemstream_async_publish_output which would contain
	// worker_persist_future and checker_persist_future which can be polled by the user.

	FUTURE_CHAIN_INIT(&future);

	return future;
}

// XXX: possible variants:
// - advance as far as possible and return persisted timestamp (pmemstream_sync_all)
// - advance as far as possible and wait if nothing to do since last call (similar to waiting on iouring completion)
// - advance to specified offset (pmemstream_sync_to)
uint64_t pmemstream_sync_all(struct pmemstream *stream)
{
	uint64_t timestamp = consume_timestamp(stream);
	if (timestamp > stream->header->timestamp) {
		... // updated stream->header->timestamp and persist it
	}

	return timestamp;
}

uint64_t pmemstream_sync_to(struct pmemstream *stream, uint64_t timestamp)
{
	do {
		uint64_t persisted_timestamp = pmemstream_sync_all(stream);
		// XXX: sleep?
	} while (persisted_timestamp < timestamp);
}

uint64_t pmemstream_persisted_timestamp(struct pmemstream *stream)
{
	return stream->header->timestamp;
}


/////////////

uint64_t acquire_timestamp(stream)
{
	uint64_t producer_id = thread_id_get(stream->thread_id);
	// XXX: ideally we want to allow single thread to have multiple ids
	// this is necessary for async publish

	return mpmc_queue_acquire(..., producer_id);
}

uint64_t produce_timestamp(stream, timestamp)
{
	uint64_t producer_id = thread_id_get(stream->thread_id);
	// XXX: ideally we want to allow single thread to have multiple ids
	// this is necessary for async publish

	return mpmc_queue_produce(stream, producer_id);
}
