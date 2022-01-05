// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2021-2022, Intel Corporation */

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

static void pmemstream_destroy_thread_id(void *arg)
{
	// TODO: recycle ID
}

int pmemstream_from_map(struct pmemstream **stream, size_t block_size, struct pmem2_map *map)
{
	struct pmemstream *s = malloc(sizeof(struct pmemstream));
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

	s->region_contexts_map = region_contexts_map_new();
	if (!s->region_contexts_map) {
		goto err_contexts_map;
	}

	s->offset_manager = offset_manager_new(PMEMSTREAM_MAX_CONCURRENCY);
	if (!s->offset_manager) {
		goto err_offset_manager;
	}

	int ret = pthread_key_create(&s->thread_id, pmemstream_destroy_thread_id);
	if (ret) {
		goto err_pthread_key_create;
	}

	*stream = s;
	return 0;

err_pthread_key_create:
	offset_manager_destroy(s->offset_manager);
err_offset_manager:
	region_contexts_map_destroy(s->region_contexts_map);
err_contexts_map:
	free(s);
	return -1;
}

void pmemstream_delete(struct pmemstream **stream)
{
	struct pmemstream *s = *stream;
	region_contexts_map_destroy(s->region_contexts_map);
	offset_manager_destroy(s->offset_manager);
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

static void pmemstream_clear_region_remaining(struct pmemstream *stream, struct pmemstream_region region, uint64_t tail)
{
	struct span_runtime region_rt = span_get_region_runtime(stream, region.offset);
	size_t region_end_offset = region.offset + region_rt.total_size;
	size_t remaining_size = region_end_offset - tail;
	span_create_empty(stream, tail, remaining_size - SPAN_EMPTY_METADATA_SIZE);
}

static uint64_t pmemstream_thread_id(struct pmemstream *stream)
{
	uint64_t thread_id = (uint64_t) pthread_getspecific(stream->thread_id);
	if (thread_id == NULL) {
		thread_id = __atomic_fetch_add(&stream->thread_id_counter, 1, __ATOMIC_RELAXED);
		// XXX - return error instead
		assert(thread_id < PMEMSTREAM_MAX_CONCURRENCY);
		pthread_setspecific(stream->thread_id, (const void*)thread_id);
	}

	return thread_id;
}

int pmemstream_reserve(struct pmemstream *stream, struct pmemstream_region region,
		       struct pmemstream_region_context *region_context, size_t size,
		       struct pmemstream_entry *reserved_entry, void **data_addr)
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

	ret = region_try_initialize_append_offset_locked(stream, region, region_context);
	if (ret) {
		return ret;
	}

	uint64_t offset = __atomic_load_n(&region_context->append_offset, __ATOMIC_RELAXED);
	if (offset & PMEMSTREAM_OFFSET_DIRTY_BIT) {
		pthread_mutex_lock(&stream->region_lock);

		if (offset == __atomic_load_n(&region_context->append_offset, __ATOMIC_RELAXED)) {
			offset &= PMEMSTREAM_OFFSET_DIRTY_MASK;
			pmemstream_clear_region_remaining(stream, region, offset);
			__atomic_store_n(&region_context->append_offset, offset, __ATOMIC_RELEASE);

			offset_manager_reset(offset);
		}

		pthread_mutex_unlock(&stream->region_lock);
	}

	uint64_t append_offset = offset_manager_acquire(stream->offset_manager, pmemstream_thread_id(stream), entry_total_size_span_aligned);

	/* XXX: should we revert to offset if there is no space? */
	if (append_offset + entry_total_size_span_aligned > region.offset + region_srt.total_size) {
		return -1;
	}
	/* offset outside of region */
	if (append_offset < region_srt.data_offset) {
		return -1;
	}

	reserved_entry->offset = append_offset;
	/* data is right after the entry metadata */
	*data_addr = span_offset_to_span_ptr(stream, append_offset + SPAN_ENTRY_METADATA_SIZE);

	return ret;
}

static int pmemstream_poll_prev_appends(struct pmemstream *stream, uint64_t target_offset)
{
	uint64_t ready_offset;
	uint64_t size = offset_manager_consume(stream->offset_manager, &ready_offset);

	if (ready_offset + size >= target_offset) {
		return 0;
	}

	return -1;
}

static int pmemstream_internal_publish(struct pmemstream *stream, struct pmemstream_region region, const void *data,
				       size_t size, struct pmemstream_entry *reserved_entry, int flags)
{
	span_create_entry(stream, reserved_entry->offset, data, size, util_popcount_memory(data, size), flags);

	offset_manager_produce(stream->offset_manager, pmemstream_thread_id(stream));

	return 0;
}

int pmemstream_publish(struct pmemstream *stream, struct pmemstream_region region, const void *data, size_t size,
		       struct pmemstream_entry *reserved_entry)
{
	int ret = pmemstream_internal_publish(stream, region, data, size, reserved_entry, PMEMSTREAM_PUBLISH_PERSIST);
	if (ret) {
		return ret;
	}

	struct span_runtime entry_rt = span_get_entry_runtime(stream, reserved_entry->offset);
	uint64_t target_offset = reserved_entry->offset + entry_rt.entry.size;

	while (pmemstream_poll_prev_appends(stream, target_offset) == -1) {
		/* SPIN */
	}

	return 0;
}

// synchronously appends data buffer to the end of the region
int pmemstream_append(struct pmemstream *stream, struct pmemstream_region region,
		      struct pmemstream_region_context *region_context, const void *data, size_t size,
		      struct pmemstream_entry *new_entry)
{
	struct pmemstream_entry reserved_entry;
	void *reserved_dest;
	int ret = pmemstream_reserve(stream, region, region_context, size, &reserved_entry, &reserved_dest);
	if (ret) {
		return ret;
	}

	stream->memcpy(reserved_dest, data, size, PMEM2_F_MEM_NONTEMPORAL);
	ret = pmemstream_internal_publish(stream, region, data, size, &reserved_entry, PMEMSTREAM_PUBLISH_NOFLUSH_DATA);
	if (ret) {
		return ret;
	}

	if (new_entry) {
		new_entry->offset = reserved_entry.offset;
	}

	return 0;
}
