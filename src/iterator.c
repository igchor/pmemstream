// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2021-2022, Intel Corporation */

#include "iterator.h"
#include "common/util.h"
#include "libpmemstream_internal.h"
#include "region.h"

#include <assert.h>
#include <stdbool.h>
#include <string.h>

int pmemstream_region_iterator_new(struct pmemstream_region_iterator **iterator, struct pmemstream *stream)
{
	if (!stream || !iterator) {
		return -1;
	}

	struct pmemstream_region_iterator *iter = malloc(sizeof(*iter));
	if (!iter) {
		return -1;
	}

	*(struct pmemstream **)&iter->stream = stream;

	/* XXX: lock */
	iter->region.offset = stream->header->region_allocator_header.allocated_list.head;
	*iterator = iter;

	return 0;
}

int pmemstream_region_iterator_next(struct pmemstream_region_iterator *it, struct pmemstream_region *region)
{
	if (!it) {
		return -1;
	}

	if (it->region.offset == SLIST_INVALID_OFFSET) {
		return -1;
	}

	*region = it->region;

	/* XXX: hide this in allocator? */
	/* XXX: lock */
	it->region.offset = SLIST_NEXT(struct span_region, &it->stream->data, it->region.offset,
				       allocator_entry_metadata.next_allocated);
	return 0;
}

void pmemstream_region_iterator_delete(struct pmemstream_region_iterator **iterator)
{
	struct pmemstream_region_iterator *iter = *iterator;

	free(iter);
	*iterator = NULL;
}

int entry_iterator_initialize(struct pmemstream_entry_iterator *iterator, struct pmemstream *stream,
			      struct pmemstream_region region,
			      region_runtime_initialize_fn_type region_runtime_initialize_fn)
{
	int ret = pmemstream_validate_stream_and_offset(stream, region.offset);
	if (ret) {
		return ret;
	}

	assert(span_get_type(span_offset_to_span_ptr(&stream->data, region.offset)) == SPAN_REGION);

	struct pmemstream_region_runtime *region_rt;
	ret = region_runtimes_map_get_or_create(stream->region_runtimes_map, region, &region_rt);
	if (ret) {
		return ret;
	}

	struct pmemstream_entry_iterator iter = {.stream = stream,
						 .offset = region.offset + offsetof(struct span_region, data),
						 .region = region,
						 .region_runtime = region_rt,
						 .region_runtime_initialize_fn = region_runtime_initialize_fn};
	memcpy(iterator, &iter, sizeof(struct pmemstream_entry_iterator));

	return 0;
}

int pmemstream_entry_iterator_new(struct pmemstream_entry_iterator **iterator, struct pmemstream *stream,
				  struct pmemstream_region region)
{
	if (!iterator) {
		return -1;
	}

	struct pmemstream_entry_iterator *iter = malloc(sizeof(*iter));
	if (!iter) {
		return -1;
	}

	int ret = entry_iterator_initialize(iter, stream, region, &region_runtime_initialize_for_write_locked);
	if (ret) {
		goto err;
	}

	*iterator = iter;

	return 0;

err:
	free(iter);
	return ret;
}

#ifndef NDEBUG
static bool pmemstream_entry_iterator_offset_is_inside_region(struct pmemstream_entry_iterator *iterator)
{
	const struct span_base *span_base = span_offset_to_span_ptr(&iterator->stream->data, iterator->region.offset);
	uint64_t region_end_offset = iterator->region.offset + span_get_total_size(span_base);
	return iterator->offset >= iterator->region.offset && iterator->offset <= region_end_offset;
}
#endif

static bool validate_entry(const struct pmemstream *stream, struct span_base span)
{
	if (span_get_type(&span) != SPAN_ENTRY) {
		return false;
	}

	const struct span_entry *span_entry = (const struct span_entry *)&span;
	const void *entry_data = span_entry->data;
	if (util_popcount_memory(entry_data, span_get_size(&span)) == span_entry->popcount) {
		return true;
	}
	return false;
}

/* Precondition: region_runtime is initialized. */
static bool pmemstream_entry_iterator_offset_is_below_committed(struct pmemstream_entry_iterator *iterator)
{
	assert(region_runtime_get_state_acquire(iterator->region_runtime) == REGION_RUNTIME_STATE_WRITE_READY);

	/* Make sure that we didn't go beyond committed entries. */
	uint64_t committed_offset = region_runtime_get_committed_offset_acquire(iterator->region_runtime);
	if (iterator->offset >= committed_offset) {
		return false;
	}

	assert(pmemstream_entry_iterator_offset_is_inside_region(iterator));
	assert(validate_entry(iterator->stream, *span_offset_to_span_ptr(&iterator->stream->data, iterator->offset)) ==
	       0);

	return true;
}

static bool pmemstream_entry_iterator_offset_at_valid_entry(struct pmemstream_entry_iterator *iterator)
{
	assert(pmemstream_entry_iterator_offset_is_inside_region(iterator));
	enum region_runtime_state state = region_runtime_get_state_acquire(iterator->region_runtime);

	if (state == REGION_RUNTIME_STATE_READ_READY) {
		struct span_base span = *span_offset_to_span_ptr(&iterator->stream->data, iterator->offset);
		enum region_runtime_state new_state = region_runtime_get_state_acquire(iterator->region_runtime);

		if (new_state == state) {
			uint64_t region_end_offset = iterator->region.offset + span_get_total_size(&span);
			return iterator->offset < region_end_offset && validate_entry(iterator->stream, span);
		}
	}

	return pmemstream_entry_iterator_offset_is_below_committed(iterator);
}

static void pmemstream_entry_iterator_advance(struct pmemstream_entry_iterator *iterator)
{
	/* Verify that all metadata and data fits inside the region before and after iterator
	 * increment - those checks should not fail unless stream was corrupted. */
	assert(pmemstream_entry_iterator_offset_is_inside_region(iterator));

	const struct span_base *span_base = span_offset_to_span_ptr(&iterator->stream->data, iterator->offset);
	iterator->offset += span_get_total_size(span_base);

	assert(pmemstream_entry_iterator_offset_is_inside_region(iterator));
}

/* Advances entry iterator by one. Verifies entry integrity and initializes region runtime if end of data is found. */
int pmemstream_entry_iterator_next(struct pmemstream_entry_iterator *iterator, struct pmemstream_region *region,
				   struct pmemstream_entry *user_entry)
{
	if (!iterator) {
		return -1;
	}

	if (region) {
		*region = iterator->region;
	}

	if (pmemstream_entry_iterator_offset_at_valid_entry(iterator)) {
		if (user_entry) {
			user_entry->offset = iterator->offset;
		}
		pmemstream_entry_iterator_advance(iterator);
		return 0;
	}

	/* Lazy (re-)initialization of region, when needed. */
	iterator->region_runtime_initialize_fn(iterator->region_runtime, iterator->offset);
	return -1;
}

void pmemstream_entry_iterator_delete(struct pmemstream_entry_iterator **iterator)
{
	struct pmemstream_entry_iterator *iter = *iterator;

	free(iter);
	*iterator = NULL;
}
