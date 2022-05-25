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
	stream->header->persisted_timestamp = PMEMSTREAM_INVALID_TIMESTAMP;
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

/* XXX: this function could be made asynchronous perhaps? */
// XXX: test this: crash before commiting new entry and then
// on restart, add new entry (should have same timestamp), verify
// that the unfinished entry is not visible.
static int pmemstream_mark_regions_for_recovery(struct pmemstream *stream)
{
	struct pmemstream_region_iterator *iterator;
	int ret = pmemstream_region_iterator_new(&iterator, stream);
	if (ret) {
		return ret;
	}

	/* XXX: we could keep list of active regions in stream header/lanes and only iterate over them. */
	struct pmemstream_region region;
	while (pmemstream_region_iterator_next(iterator, &region) == 0) {
		struct span_region *span_region =
			(struct span_region *)span_offset_to_span_ptr(&stream->data, region.offset);
		if (span_region->max_valid_timestamp == PMEMSTREAM_INVALID_TIMESTAMP) {
			span_region->max_valid_timestamp = stream->header->persisted_timestamp;
			stream->data.flush(&span_region->max_valid_timestamp, sizeof(span_region->max_valid_timestamp));
		} else {
			/* If max_valid_timestamp points is equal to a valid timestamp, this means that this regions
			 * hasn't recovered after previous restart yet, skip it. */
		}
	}
	stream->data.drain();

	pmemstream_region_iterator_delete(&iterator);

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

	s->committed_timestamp = s->header->persisted_timestamp;
	s->next_timestamp = s->header->persisted_timestamp + 1;

	allocator_runtime_initialize(&s->data, &s->header->region_allocator_header);

	int ret = pmemstream_mark_regions_for_recovery(s);
	if (ret) {
		return ret;
	}

	s->region_runtimes_map = region_runtimes_map_new(&s->data);
	if (!s->region_runtimes_map) {
		goto err_region_runtimes;
	}

	int locks_initialized;
	for (locks_initialized = 0; locks_initialized < PMEMSTREAM_MAX_CONCURRENCY; locks_initialized++) {
		ret = pthread_mutex_init(&s->async_ops[locks_initialized], NULL);
		if (ret) {
			goto err_commit_lock;
		}
	}

	*stream = s;
	return 0;

err_commit_lock:
	for (int i = 0; i < locks_initialized; i++) {
		pthread_mutex_destroy(&s->async_ops[i]);
	}
err_region_runtimes:
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
	for (int i = 0; i < PMEMSTREAM_MAX_CONCURRENCY; i++) {
		pthread_mutex_destroy(&s->async_ops[i]);
	}
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

uint64_t pmemstream_entry_timestamp(struct pmemstream *stream, struct pmemstream_entry entry)
{
	int ret = pmemstream_validate_stream_and_offset(stream, entry.offset);
	if (ret) {
		return 0;
	}
	struct span_entry *span_entry = (struct span_entry *)span_offset_to_span_ptr(&stream->data, entry.offset);
	return span_entry->timestamp;
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

	return region_runtime_iterate_and_initialize_for_write_locked(stream, region, *region_runtime);
}

static size_t pmemstream_entry_total_size_aligned(size_t size)
{
	struct span_entry span_entry = {.span_base = span_base_create(size, SPAN_ENTRY)};
	return span_get_total_size(&span_entry.span_base);
}

int pmemstream_reserve(struct pmemstream *stream, struct pmemstream_region region,
		       struct pmemstream_region_runtime *region_runtime, size_t size,
		       struct pmemstream_entry *reserved_entry, void **data_addr, uint64_t *timestamp)
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
	uint8_t *destination = (uint8_t *)pmemstream_offset_to_ptr(&stream->data, offset);
	assert(offset >= region.offset + offsetof(struct span_region, data));
	if (offset + entry_total_size_span_aligned > region.offset + span_get_total_size(span_region)) {
		return -1;
	}

	region_runtime_increase_append_offset(region_runtime, entry_total_size_span_aligned);

	reserved_entry->offset = offset;
	/* data is right after the entry metadata */
	*data_addr = destination + sizeof(struct span_entry);
	*timestamp = pmemstream_acquire_timestamp(stream);

	/* Store metadata. */
	struct span_entry span_entry = {.span_base = span_base_create(size, SPAN_ENTRY), .timestamp = *timestamp};
	stream->data.memcpy(destination, &span_entry, sizeof(span_entry), PMEM2_F_MEM_NOFLUSH);

	/* Clear next entry metadata. Rely on publish to flush.
	 * This must be done here, because all other operations (data copy and pmemstream_publish can be called
	 * in arbitrary order which could result in override actual entry metadata).
	 */
	stream->data.memset(destination + entry_total_size_span_aligned, 0, sizeof(struct span_entry),
			    PMEM2_F_MEM_NOFLUSH);

	return ret;
}

static uint64_t pmemstream_acquire_timestamp(struct pmemstream *stream)
{
	uint64_t timestamp = __atomic_fetch_add(&stream->next_timestamp, 1, __ATOMIC_RELAXED);

	assert(timestamp - __atomic_load_n(&stream->committed_timestamp, __ATOMIC_RELAXED) < PMEMSTREAM_MAX_CONCURRENCY);
	uint64_t ops_index = timestamp % PMEMSTREAM_MAX_CONCURRENCY;
	struct async_operation *async_op = stream->async_ops + ops_index;

	pthread_mutex_lock(&async_op->lock);

	// XXX: we could call pmemstream_async_wait_commited here, but we would need to make
	// sure that there is no recursive locking
	while(future_poll(FUTURE_AS_RUNNABLE(&async_op->future)) != FUTURE_STATE_COMPLETE);
}

static void pmemstream_publish_timestamp(struct pmemstream *stream, uint64_t timestamp)
{
	assert(timestamp - __atomic_load_n(&stream->committed_timestamp, __ATOMIC_RELAXED) < PMEMSTREAM_MAX_CONCURRENCY);
	uint64_t ops_index = timestamp % PMEMSTREAM_MAX_CONCURRENCY;
	struct async_operation *async_op = stream->async_ops + ops_index;

	// XXX: since we are still holding a lock, we can check if mempcy finished
	// if so, we can increase committed_offset immediately

	// This requires publish to be called on the same thread as reserve.
	// XXX: use semaphores instead?
	pthread_mutex_unlock(&async_op->lock);
}

int pmemstream_publish(struct pmemstream *stream, struct pmemstream_region region,
		       struct pmemstream_region_runtime *region_runtime, uint64_t timestamp)
{
	int ret = pmemstream_async_publish(stream, region, region_runtime, timestamp);

	while (future_poll(FUTURE_AS_RUNNABLE(pmemstream_async_wait_committed(stream, timestamp))) != FUTURE_STATE_COMPLETE);

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
	uint64_t timestamp;
	int ret = pmemstream_reserve(stream, region, region_runtime, size, &reserved_entry, &reserved_dest, &timestamp);
	if (ret) {
		return ret;
	}

	stream->data.memcpy(reserved_dest, data, size, PMEM2_F_MEM_NOFLUSH);

	pmemstream_publish(stream, region, region_runtime, timestamp);

	if (new_entry) {
		new_entry->offset = reserved_entry.offset;
	}

	return 0;
}

int pmemstream_async_publish(struct pmemstream *stream, struct pmemstream_region region,
							     struct pmemstream_region_runtime *region_runtime,
							     uint64_t timestamp)
{
	int ret = pmemstream_validate_stream_and_offset(stream, region.offset);
	if (ret) {
		return ret;
	}

	if (!region_runtime) {
		ret = pmemstream_region_runtime_initialize(stream, region, &region_runtime);
		if (ret) {
			return ret;
		}
	}

	pmemstream_publish_timestamp(stream, timestamp);

	return 0;
}

// asynchronously appends data buffer to the end of the region
// current flow:
// 1. reserve will acquire timestamp from a ringbuffer (will wait if there is no available one.
//    this can happen if ther are more than PMEMSTREAM_MAX_CONCURRENCY concurrent operations).
//    reserve will also write the metadata of the entry to pmem
// 2. prepare and poll miniasync memcpy future.
// 3. mark timestamp as published. This is done by releasing a lock taken in step 1.
// 
// To make sure that the entry is acually stored/commited one must call
// pmemstream_async_wait_committed and poll returned future to completion.
int pmemstream_async_append(struct pmemstream *stream, struct vdm *vdm,
							   struct pmemstream_region region,
							   struct pmemstream_region_runtime *region_runtime,
							   const void *data, size_t size, uint64_t *timestamp)
{
	if (!region_runtime) {
		int ret = pmemstream_region_runtime_initialize(stream, region, &region_runtime);
		if (ret) {
			return ret;
		}
	}

	struct pmemstream_entry reserved_entry;
	void *reserved_dest;
	int ret = pmemstream_reserve(stream, region, region_runtime, size, &reserved_entry, &reserved_dest, timestamp);
	if (ret) {
		return ret;
	}

	uint64_t ops_index = *timestamp % PMEMSTREAM_MAX_CONCURRENCY;
	struct async_operation *async_op = stream->async_ops + ops_index;

	async_op->future = vdm_memcpy(vdm, reserved_dest, (void *)data, size, PMEM2_F_MEM_NOFLUSH);
	async_op->region = region;
	async_op->region_runtime = region_runtime;
	async_op->entry = reserved_entry;
	async_op->size = size;

	future_poll(FUTURE_AS_RUNNABLE(&async_ops->future), NULL);

	pmemstream_publish_timestamp(*timestmp);

	return 0;
}

static enum future_state pmemstream_async_wait_committed_impl(struct future_context *ctx, struct future_notifier *notifier)
{
	/* XXX: properly use/fix notifier */
    if (notifier != NULL) {
        notifier->notifier_used = FUTURE_NOTIFIER_NONE;

	struct pmemstream_async_wait_committed_data *data = future_context_get_data(ctx);
	struct pmemstream_async_wait_committed_output *out = future_context_get_output(ctx);

	// XXX
	out->error_code = 0;

	while (true) {
		uint64_t committed_timestamp = __atomic_load_n(&data->stream->committed_timestamp, __ATOMIC_RELAXED);

		if (data->timestamp < commited_timestamp)
			return FUTURE_STATE_COMPLETE;

		uint64_t ops_index = (commited_timestamp + 1) % PMEMSTREAM_MAX_CONCURRENCY;
		struct async_operation *async_op = data->stream->async_ops + ops_index;

		bool op_completed = false;
		pthread_mutex_lock(&async_op->lock);
		// XXX: what if future was already completed, can we still poll it?
		op_completed = future_poll(FUTURE_AS_RUNNABLE(&async_op->future), NULL) == FUTURE_STATE_COMPLETE;
		pthread_mutex_unlock(&async_op->lock);

		if (op_completed) {
			// XXX: or use global lock for commits to avoid CAS?
			if (__atomic_compare_exchange_n(&data->stream->committed_timestamp, &committed_timestamp, commited_timestamp + 1, false,
						      __ATOMIC_RELAXED, __ATOMIC_RELAXED)) {
				/* Persist data, metadata and next entry metadata (either valid metadata or zeroed by reserve). */
				uint8_t *destination = (uint8_t *)span_offset_to_span_ptr(&data->stream->data, async_op->entry.offset);
				data->stream->data.persist(destination, async_op->size + sizeof(struct span_entry));

				__atomic_store_n(&data->stream->header->persisted_timestamp, commited_timestamp + 1, __ATOMIC_RELEASE);
				data->stream->data.persist(&data->stream->header->persisted_timestamp, sizeof(uint64_t));

				region_runtime_increase_committed_offset(async_op->region_runtime, async_op->size);
			}
			// If CAS failed, it means that some other thread increased the commited_offset, just move forward
		} else {
			return FUTURE_STATE_RUNNING;
		}
	}
}

struct pmemstream_async_wait_committed_fut pmemstream_async_wait_committed(struct pmemstream *stream, uint64_t timestamp)
{
	uint64_t committed_timestamp = __atomic_load_n(&stream->committed_timestamp, __ATOMIC_RELAXED);

	if (committed_timestamp > timestamp) {
		// XXX: return completed future
	}

	if (commited_timestamp == __atomic_load_n(&stream->next_timestamp, __ATOMIC_RELAXED) - 1) {
		// XXX: return state idle?
	}

	struct pmemstream_async_wait_committed_fut future;
	future.data.stream = stream;
	future.timestamp = timestamp;

	FUTURE_INIT(&future, pmemstream_async_wait_committed_impl);
}
