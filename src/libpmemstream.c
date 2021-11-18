// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2021, Intel Corporation */

#include "libpmemstream.h"

#include <assert.h>
#include <libpmem2.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

#define ALIGN_UP(size, align) (((size) + (align)-1) & ~((align)-1))
#define ALIGN_DOWN(size, align) ((size) & ~((align)-1))

#define PMEMSTREAM_SIGNATURE ("PMEMSTREAM")
#define PMEMSTREAM_SIGNATURE_LEN (64)
#define PMEMSTREAM_NLANES (1024)

struct pmemstream_header {
	char signature[PMEMSTREAM_SIGNATURE_LEN];
	uint64_t stream_size;
	uint64_t block_size;
};

struct pmemstream_data {
	struct pmemstream_header header;
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
};

struct pmemstream_entry_iterator {
	struct pmemstream *stream;
	struct pmemstream_region region;
	struct pmemstream_span *region_span;
	struct pmemstream_span_runtime region_sr;
	size_t offset;
};

struct pmemstream_region_iterator {
	struct pmemstream *stream;
	struct pmemstream_region region;
};

struct pmemstream_region_context {
	// runtime state for a region, not mt-safe
	struct pmemstream_region region;
	size_t offset;
};

enum pmemstream_span_type {
	PMEMSTREAM_SPAN_FREE = 00ULL << 62,
	PMEMSTREAM_SPAN_REGION = 11ULL << 62,
	PMEMSTREAM_SPAN_TX = 10ULL << 62,
	PMEMSTREAM_SPAN_ENTRY = 01ULL << 62,
};

#define PMEMSTREAM_SPAN_TYPE_MASK (11ULL << 62)
#define PMEMSTREAM_SPAN_EXTRA_MASK (~PMEMSTREAM_SPAN_TYPE_MASK)

struct pmemstream_span {
	uint64_t data[];
};

struct pmemstream_span_runtime {
	enum pmemstream_span_type type;
	size_t total_size;
	uint8_t *data;
	union {
		struct {
			uint64_t size;
		} free;
		struct {
			uint64_t size;
		} region;
		struct {
			uint64_t size;
            uint64_t popcount;
		} entry;
	};
};

static void
pmemstream_span_create_free(struct pmemstream_span *span, size_t data_size)
{
	assert((data_size & PMEMSTREAM_SPAN_TYPE_MASK) == 0);
	span->data[0] = data_size | PMEMSTREAM_SPAN_FREE;
}

static void
pmemstream_span_create_entry(struct pmemstream_span *span, size_t data_size, size_t popcount)
{
	assert((data_size & PMEMSTREAM_SPAN_TYPE_MASK) == 0);
	span->data[0] = data_size | PMEMSTREAM_SPAN_ENTRY;
	span->data[1] = popcount;
}

static void
pmemstream_span_create_region(struct pmemstream_span *span, size_t size)
{
	assert((size & PMEMSTREAM_SPAN_TYPE_MASK) == 0);
	span->data[0] = size & PMEMSTREAM_SPAN_TYPE_MASK;
}

static struct pmemstream_span_runtime
pmemstream_span_get_runtime(struct pmemstream_span *span)
{
	struct pmemstream_span_runtime sr;
	sr.type = span->data[0] & PMEMSTREAM_SPAN_TYPE_MASK;
	uint64_t extra = span->data[0] & PMEMSTREAM_SPAN_EXTRA_MASK;
	switch (sr.type) {
		case PMEMSTREAM_SPAN_FREE:
			sr.free.size = extra;
			sr.data = (uint8_t *)&span->data[1];
			sr.total_size = sr.free.size + sizeof(uint64_t);
			break;
		case PMEMSTREAM_SPAN_ENTRY:
			sr.entry.size = extra;
			sr.entry.popcount = span->data[1];
			sr.data = (uint8_t *)&span->data[2];
			sr.total_size = sr.entry.size + 2 * sizeof(uint64_t);
			break;
		case PMEMSTREAM_SPAN_REGION:
			sr.region.size = extra;
			sr.data = (uint8_t *)&span->data[1];
			sr.total_size = sr.region.size + sizeof(uint64_t);
			break;
		default:
			abort();
	}

	return sr;
}


static int
pmemstream_is_initialized(struct pmemstream *stream)
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

static void
pmemstream_init(struct pmemstream *stream)
{
	memset(stream->data->header.signature, 0, PMEMSTREAM_SIGNATURE_LEN);
	memset(stream->data->lanes, 0, sizeof(stream->data->lanes));

	stream->data->header.stream_size = stream->stream_size;
	stream->data->header.block_size = stream->block_size;
	stream->persist(stream->data, sizeof(struct pmemstream_data));

	stream->memcpy(stream->data->header.signature, PMEMSTREAM_SIGNATURE,
		       PMEMSTREAM_SIGNATURE_LEN, 0);
}

static struct pmemstream_region
pmemstream_region_from_offset(size_t offset)
{
	struct pmemstream_region region;
	region.offset = offset;

	return region;
}

static struct pmemstream_span *
pmemstream_get_span_for_offset(struct pmemstream *stream, size_t offset)
{
	return (struct pmemstream_span *)((uint8_t *)stream->data->spans +
					  offset);
}

int
pmemstream_from_map(struct pmemstream **stream, size_t block_size,
		    struct pmem2_map *map)
{
	struct pmemstream *s = malloc(sizeof(struct pmemstream));
	s->data = pmem2_map_get_address(map);
	s->stream_size = pmem2_map_get_size(map);
	s->usable_size = ALIGN_DOWN(
		s->stream_size - sizeof(struct pmemstream_data), block_size);
	s->block_size = block_size;
	pthread_mutex_init(&s->regions_lock, NULL);

	s->memcpy = pmem2_get_memcpy_fn(map);
	s->memset = pmem2_get_memset_fn(map);
	s->persist = pmem2_get_persist_fn(map);
	s->flush = pmem2_get_flush_fn(map);
	s->drain = pmem2_get_drain_fn(map);

	if (pmemstream_is_initialized(s) != 0) {
		pmemstream_init(s);
	}

	*stream = s;

	return 0;
}

void
pmemstream_delete(struct pmemstream **stream)
{
	struct pmemstream *s = *stream;
	pthread_mutex_destroy(&s->regions_lock);
	free(s);
	*stream = NULL;
}

// stream owns the region object - the user gets a reference, but it's not
// necessary to hold on to it and explicitly delete it.
int
pmemstream_region_allocate(struct pmemstream *stream, size_t size, struct pmemstream_region *region)
{
	pthread_mutex_lock(&stream->regions_lock);
	{
		size = ALIGN_UP(size, stream->block_size);

		struct pmemstream_region new_region;
		new_region.offset = 0;

		struct pmemstream_span *span = pmemstream_get_span_for_offset(stream, new_region.offset);
		pmemstream_span_create_region(span, size);

		*region = new_region;
	}
	pthread_mutex_unlock(&stream->regions_lock);

	return 0; // TODO: check if region already exists
}

int
pmemstream_region_free(struct pmemstream
			  struct pmemstream_region region)
{
	// TODO
	return 0;
}

// clearing a region is less expensive than removing it
int
pmemstream_region_clear(struct pmemstream *stream,
			struct pmemstream_region region)
{
	// TODO
	return 0;
}

int
pmemstream_region_context_new(struct pmemstream_region_context **rcontext,
			      struct pmemstream *stream,
			      struct pmemstream_region region)
{
	// todo: the only reason context exists is to store the offset of the
	// place where we can append inside of the region.
	// maybe we can have this as a per-stream state in some hashtable?

	struct pmemstream_region_context *c = malloc(sizeof(*c));
	c->region = region;
	c->offset = 0;

	struct pmemstream_span *region_span = pmemstream_get_span_for_offset(stream, region.offset);
	struct pmemstream_span_runtime region_sr = pmemstream_span_get_runtime(region_span);
	assert(region_sr.type == PMEMSTREAM_SPAN_REGION);

	while (c->offset < region_sr.region.size) {
		struct pmemstream_span *span =
			(struct pmemstream_span *)(region_sr.data + c->offset);
		struct pmemstream_span_runtime sr =
			pmemstream_span_get_runtime(span);
		if (sr.type == PMEMSTREAM_SPAN_FREE) {
			break;
		}
		c->offset += sr.total_size;
	}

	*rcontext = c;
	return 0;
}

void
pmemstream_region_context_delete(struct pmemstream_region_context **rcontext)
{
	struct pmemstream_region_context *c = *rcontext;
	free(c);
}

// synchronously appends data buffer to the end of the region
int
pmemstream_append(struct pmemstream *stream, struct pmemstream_region_context *rcontext, const void *buf, size_t count,
	struct pmemstream_entry *entry)
{
	size_t entry_total_size = count + 2 * sizeof(uint64_t); // TODO: create a function for this
	struct pmemstream_span *region_span = pmemstream_get_span_for_offset(stream, rcontext->region.offset);
	struct pmemstream_span_runtime region_sr = pmemstream_span_get_runtime(region_span);

	size_t offset = __atomic_fetch_add(&rcontext->offset, entry_total_size, __ATOMIC_RELEASE);
	if (region_sr.region.size < offset + entry_total_size) {
		return -1;
	}

	struct pmemstream_span *entry_span = (struct pmemstream_span *)(region_sr.data + offset);

	if (entry) {
		entry->offset = pmemstream_get_offset_for_span(stream, entry_span);
	}

	pmemstream_span_create_entry(entry_span, count, 0 /* TODO: popcount */);
	// TODO: size and popcount of the entry should be flushed

	// for popcount, we also need to make sure that the memory is zeroed - maybe it can be done by bg thread?

	struct pmemstream_span_runtime entry_rt = pmemstream_span_get_runtime(entry_span);

	tx->stream->memcpy(entry_rt.data, buf, count, 0);

	return 0;
}

// returns pointer to the data of the entry
void *
pmemstream_entry_data(struct pmemstream *stream, struct pmemstream_entry entry)
{
	struct pmemstream_span *entry_span;
	entry_span = pmemstream_get_span_for_offset(stream, entry.offset);
	struct pmemstream_span_runtime entry_sr =
		pmemstream_span_get_runtime(entry_span);
	assert(entry_sr.type == PMEMSTREAM_SPAN_ENTRY);

	return entry_sr.data;
}

// returns the size of the entry
size_t
pmemstream_entry_length(struct pmemstream *stream, struct pmemstream_entry entry)
{
	struct pmemstream_span *entry_span;
	entry_span = pmemstream_get_span_for_offset(stream, entry.offset);
	struct pmemstream_span_runtime entry_sr =
		pmemstream_span_get_runtime(entry_span);
	assert(entry_sr.type == PMEMSTREAM_SPAN_ENTRY);

	return entry_sr.entry.size;
}

int
pmemstream_region_iterator_new(struct pmemstream_region_iterator **iterator, struct pmemstream *stream)
{
	struct pmemstream_region_iterator *iter = malloc(sizeof(*iter));
	iter->stream = stream;
	iter->region.offset = 0;

	*iterator = iter;

	return 0;
}

int
pmemstream_region_iterator_next(struct pmemstream_region_iterator *, struct pmemstream_region *)
{
	return -1;
}

void
pmemstream_region_iterator_delete(struct pmemstream_region_iterator **iterator)
{
	struct pmemstream_region_iterator *iter = *iterator;

	free(iter);
	*iterator = NULL;
}

int
pmemstream_entry_iterator_new(struct pmemstream_entry_iterator **iterator,
			      struct pmemstream *stream,
			      struct pmemstream_region region)
{
	struct pmemstream_entry_iterator *iter = malloc(sizeof(*iter));
	iter->offset = 0;
	iter->region = region;
	iter->stream = stream;
	iter->region_span =
		pmemstream_get_span_for_offset(stream, region.offset);
	iter->region_sr = pmemstream_span_get_runtime(iter->region_span);

	*iterator = iter;

	return 0;
}

int
pmemstream_entry_iterator_next(struct pmemstream_entry_iterator *iter,
			       struct pmemstream_region *region,
			       struct pmemstream_entry *entry)
{
	for (;;) {
		if (iter->offset >= iter->region_sr.region.size) {
			return -1;
		}
		struct pmemstream_span *entry_span =
			(struct pmemstream_span *)(iter->region_sr.data +
						   iter->offset);

		struct pmemstream_span_runtime rt =
			pmemstream_span_get_runtime(entry_span);
		iter->offset += rt.total_size;

		switch (rt.type) {
			case PMEMSTREAM_SPAN_FREE:
				return -1;
				break;
			case PMEMSTREAM_SPAN_REGION:
				return -1;
				break;
			case PMEMSTREAM_SPAN_ENTRY: {
				// TODO: verify checksum / popcount?
				if (entry) {
					entry->offset =
						pmemstream_get_offset_for_span(
							iter->stream,
							entry_span);
				}
				if (region) {
					*region = iter->region;
				}
				return 0;
			} break;
		}
	}

	return -1;
}

void
pmemstream_entry_iterator_delete(struct pmemstream_entry_iterator **iterator)
{
	struct pmemstream_entry_iterator *iter = *iterator;

	free(iter);
	*iterator = NULL;
}
