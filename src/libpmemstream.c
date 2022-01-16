// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2021-2022, Intel Corporation */

/* Implementation of public C API */

#include "common/util.h"
#include "libpmemstream_internal.h"

#include <libminiasync.h>

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

static void pmemstream_destroy_thread_id(void *arg)
{
	// TODO: recycle ID
}

int pmemstream_from_map(struct pmemstream **stream, size_t block_size, struct pmem2_map *map)
{
	if (block_size == 0) {
		return -1;
	}

	struct pmemstream *s = malloc(sizeof(struct pmemstream));
	if (!s) {
		return -1;
	}

	s->data = pmem2_map_get_address(map);
	s->stream_size = pmem2_map_get_size(map);
	s->usable_size = ALIGN_DOWN(s->stream_size - sizeof(struct pmemstream_data), block_size);
	s->block_size = block_size;
	s->thread_id_counter = 0;

	s->memcpy = pmem2_get_memcpy_fn(map);
	s->memset = pmem2_get_memset_fn(map);
	s->persist = pmem2_get_persist_fn(map);
	s->flush = pmem2_get_flush_fn(map);
	s->drain = pmem2_get_drain_fn(map);

	if (pmemstream_is_initialized(s) != 0) {
		pmemstream_init(s);
	}

	s->region_runtimes_map = region_runtimes_map_new();
	if (!s->region_runtimes_map) {
		goto err_contexts_map;
	}

	s->mpmc_queue = mpmc_queue_new(PMEMSTREAM_MAX_CONCURRENCY);
	if (!s->mpmc_queue) {
		goto err_mpmc_queue;
	}

	int ret = pthread_key_create(&s->thread_id, pmemstream_destroy_thread_id);
	if (ret) {
		goto err_pthread_key_create;
	}

	ret = pthread_mutex_init(&s->recover_region_lock, NULL);
	if (ret) {
		goto err_recover_region_lock;
	}

	*stream = s;
	return 0;

err_recover_region_lock:
	pthread_key_delete(s->thread_id);
err_pthread_key_create:
	mpmc_queue_destroy(s->mpmc_queue);
err_mpmc_queue:
	region_runtimes_map_destroy(s->region_runtimes_map);
err_contexts_map:
	free(s);
	return -1;
}

void pmemstream_delete(struct pmemstream **stream)
{
	struct pmemstream *s = *stream;
	region_runtimes_map_destroy(s->region_runtimes_map);
	mpmc_queue_destroy(s->mpmc_queue);
	pthread_key_delete(s->thread_id);
	pthread_mutex_destroy(&s->recover_region_lock);
	free(s);
	*stream = NULL;
}

// stream owns the region object - the user gets a reference, but it's not
// necessary to hold on to it and explicitly delete it.
/* XXX: add test for multiple regions, when supported */
int pmemstream_region_allocate(struct pmemstream *stream, size_t size, struct pmemstream_region *region)
{
	const uint64_t offset = 0;
	struct span_runtime srt = span_get_runtime(stream, offset);

	if (srt.type != SPAN_EMPTY)
		return -1;

	size = ALIGN_UP(size, stream->block_size);

	if (size > srt.empty.size)
		return -1;

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

	region_runtimes_map_remove(stream->region_runtimes_map, region);

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

int pmemstream_get_region_runtime(struct pmemstream *stream, struct pmemstream_region region,
				  struct pmemstream_region_runtime **region_runtime)
{
	return region_runtimes_map_get_or_create(stream->region_runtimes_map, region, region_runtime);
}

static void pmemstream_clear_region_remaining(struct pmemstream *stream, struct pmemstream_region region, uint64_t tail)
{
	struct span_runtime region_rt = span_get_region_runtime(stream, region.offset);
	size_t region_end_offset = region.offset + region_rt.total_size;
	size_t remaining_size = region_end_offset - tail;
	span_create_empty(stream, tail, remaining_size - SPAN_EMPTY_METADATA_SIZE);
}

static uint64_t pmemstream_thread_id(struct pmemstream *stream)
{
	uint64_t thread_id = (uint64_t)pthread_getspecific(stream->thread_id);
	if (thread_id == 0) {
		thread_id = __atomic_fetch_add(&stream->thread_id_counter, 1, __ATOMIC_RELAXED);
		// XXX - return error instead
		assert(thread_id < PMEMSTREAM_MAX_CONCURRENCY);

		/* Always store real thread_id + 1 to not collide with NULL. */
		thread_id++;
		pthread_setspecific(stream->thread_id, (const void *)thread_id);
	}

	return thread_id - 1;
}

int pmemstream_reserve(struct pmemstream *stream, struct pmemstream_region region,
		       struct pmemstream_region_runtime *region_runtime, size_t size,
		       struct pmemstream_entry *reserved_entry, void **data_addr)
{
	size_t entry_total_size = size + SPAN_ENTRY_METADATA_SIZE + 8 /*XXX: constant */;
	size_t entry_total_size_span_aligned = ALIGN_UP(entry_total_size, sizeof(span_bytes));
	struct span_runtime region_srt = span_get_region_runtime(stream, region.offset);
	int ret = 0;

	if (!region_runtime) {
		ret = pmemstream_get_region_runtime(stream, region, &region_runtime);
		if (ret) {
			return ret;
		}
	}

	ret = region_try_runtime_initialize_locked(stream, region, region_runtime);
	if (ret) {
		return ret;
	}

	uint64_t append_offset =
		mpmc_queue_acquire(stream->mpmc_queue, pmemstream_thread_id(stream), entry_total_size_span_aligned);
	assert(append_offset > region_srt.data_offset);

	if (append_offset + entry_total_size_span_aligned > region.offset + region_srt.total_size) {
		return -1;
	}

	reserved_entry->offset = append_offset;
	/* data is right after the entry metadata */
	*data_addr = span_offset_to_span_ptr(stream, append_offset + SPAN_ENTRY_METADATA_SIZE);

	return ret;
}

/* Try to consume offset from mpmc_queue - returns 0 if consumed offset + size is >= target_offset,
 * -1 otherwise. */
static int pmemstream_poll_prev_appends(struct pmemstream *stream, uint64_t target_offset)
{
	uint64_t ready_offset;
	uint64_t size = mpmc_queue_consume(stream->mpmc_queue, &ready_offset);

	if (ready_offset + size >= target_offset) {
		return 0;
	}

	return -1;
}

static enum future_state pmemstream_async_commit_impl(struct future_context *ctx, struct future_notifier *notifier)
{
	struct pmemstream_async_commit_data *data = future_context_get_data(ctx);
	if (pmemstream_poll_prev_appends(data->stream, data->target_offset) == 0) {

		// APPROACH 2:
		uint64_t *mark_ptr = (uint64_t*) pmemstream_offset_to_ptr(data->stream, data->target_offset - 8);
		data->stream->memset(mark_ptr, 1, 8, PMEM2_F_MEM_NOFLUSH); // should we flush here?

		return FUTURE_STATE_COMPLETE;
	}

	return FUTURE_STATE_RUNNING; // XXX: should it be idle?
}

static struct pmemstream_commit_future pmemstream_async_commit(struct pmemstream *stream, uint64_t target_offset)
{
	struct pmemstream_commit_future future;
	future.data.stream = stream;
	future.data.target_offset = target_offset;

	FUTURE_INIT(&future, pmemstream_async_commit_impl);

	return future;
}

static int pmemstream_internal_publish(struct pmemstream *stream, struct pmemstream_region region, const void *data,
				       size_t size, struct pmemstream_entry *reserved_entry, int flags,
				       struct pmemstream_commit_future *commit_future)
{
	/* XXX: Handle possible garbage after this entry:
	 * Approach 1 (no extra 8 bytes, just write 0 where next entry metadata will be):
	 *   __atomic_exchange to 0, if previous value was not 0 AND append_offset has been increased,
	 *   revert to the old value (metadata for new entry). Iterators must also be aware of this:
	 *   if metadata of entry with offset < commited_offset is 0 then it should spin for a while
	 *   (wait for the revert to happen).
	 * 
	 *   For storing metadata we still use memcpy (XXX: will this sync correctly with CAS?)
	 * Approach 2:
	 *    Always set last 8 bytes to 0 (BEFORE calling mpmc_queue_produce). Every append which is
	 *    executed next, will have to set the bytes to some other value (all 1s?) just before marking
	 *    future as completed (but after calling mpmc_queue_produce).
	 */

	span_create_entry(stream, reserved_entry->offset, data, size, util_popcount_memory(data, size), flags);
	struct span_runtime entry_rt = span_get_entry_runtime(stream, reserved_entry->offset); // XXX: this read data written using NONTEMPORALS
	uint64_t target_offset = reserved_entry->offset + entry_rt.entry.size;

	// APPROACH 2 (XXX: no iterator support as of now):
	stream->memset((void*)(target_offset - 8), 0, 8);

	mpmc_queue_produce(stream->mpmc_queue, pmemstream_thread_id(stream));

	struct pmemstream_commit_future future = pmemstream_async_commit(stream, target_offset);
	if (commit_future) {
		*commit_future = future;
	} else {
		/* If user have not provided ptr to future, we will wait for completion. */
		FUTURE_BUSY_POLL(&future);
	}

	return 0;
}

int pmemstream_publish(struct pmemstream *stream, struct pmemstream_region region, const void *data, size_t size,
		       struct pmemstream_entry *reserved_entry, struct pmemstream_commit_future *commit_future)
{
	return pmemstream_internal_publish(stream, region, data, size, reserved_entry, PMEMSTREAM_PUBLISH_PERSIST,
					   commit_future);
}

// synchronously appends data buffer to the end of the region
int pmemstream_append(struct pmemstream *stream, struct pmemstream_region region,
		      struct pmemstream_region_runtime *region_runtime, const void *data, size_t size,
		      struct pmemstream_entry *new_entry, struct pmemstream_commit_future *commit_future)
{
	struct pmemstream_entry reserved_entry;
	void *reserved_dest;
	int ret = pmemstream_reserve(stream, region, region_runtime, size, &reserved_entry, &reserved_dest);
	if (ret) {
		return ret;
	}

	stream->memcpy(reserved_dest, data, size, PMEM2_F_MEM_NONTEMPORAL | PMEM2_F_MEM_NODRAIN);
	ret = pmemstream_internal_publish(stream, region, data, size, &reserved_entry, PMEMSTREAM_PUBLISH_NOFLUSH_DATA,
					  commit_future);
	if (ret) {
		return ret;
	}

	if (new_entry) {
		new_entry->offset = reserved_entry.offset;
	}

	return 0;
}
