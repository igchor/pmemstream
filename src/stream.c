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

//////////////////////////////////////// DURABLE COPY ////////////////////////////////////////

struct durable_copy_config {
    int durability_flag; // non/popcout/checksum
    union {
        checksum_fn_type* checksum_fn;
        // popcount impl?
    };
};

// MAKE this public??? - use algorithm like in pmemobj to avoid writing non-full cachelines
// TODO: implement this in set/pmem2? we could even use DSA for this
void durable_memcpy(void *data, void *src, size_t size, durable_copy_config* cfg) {
    if (flag == PMEMSTREAM_NO_DURABILITY) {
        stream->memcpy(dst, srd, size);
    } else if (flag == PMEMSTREAM_CHECKSUM_DURABILITY) {
        auto cksum = cfg->checksum_fn(data, size);
        stream->memcpy(dst, string(src + cksum), size, PMEM_NONTEMPORAL);
    } else {
        // TODO - popcount
    }
}

/////////////////////////////////////////////////////////////////////////////////////////////

// EXAMPLE showing how to build a log using this durable memcpy (with DSA probably) and a checksum
// ------------------------------------------ EXAMPLE --------------------------------------------
// - simple append only log with concurrent appends

struct cksum_entry {
    cksum_type cksum;
    size_t size;
    uint8_t data[];
}

struct stream {
    size_t size;
    entry entries[];
};

struct region_context
{
    size_t offset;
};

int create_memory_part_context(region_context **ctx, stream* log, durable_copy_config *cfg) {
    entry *e = log->entries;

    while (cfg->cksum(e->data, e->size) == e->cksum) {
        e += size;
    }

    *ctx = malloc(sizeof(region_context));

    // after offset, there might be some holes, and more data, for this example, we assume this data is just lost
    // we can use offset returned by append and save it somewhere on pmem to mark some critical places
    (*ctx)->offset = e - log->data;
}

size_t memory_part_append(region_context *ctx, void* src, size_t size) {
    auto cnt = __atomic_fetch_add(&ctx->offset, size);
    durable_memcpy(data + cnt, src, size, ctx->cfg);

    return cnt;
}

// THINK about adding RPMEM for such log?
// ADD a blog post about this?????

// ------------------------------------------ EXAMPLE --------------------------------------------

////////////////////////////////////////////////////////////////////////
enum pmemstream_span_type {
	PMEMSTREAM_SPAN_FREE = 00ULL << 62,
	PMEMSTREAM_SPAN_REGION = 11ULL << 62,
	PMEMSTREAM_SPAN_TX = 10ULL << 62,
	PMEMSTREAM_SPAN_ENTRY = 01ULL << 62,
};

#define PMEMSTREAM_SPAN_TYPE_MASK (11ULL << 62)
#define PMEMSTREAM_SPAN_EXTRA_MASK (~PMEMSTREAM_SPAN_TYPE_MASK)

struct pmemstream_span {
	uint64_t type_extra;
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
			uint64_t txid;
			uint64_t size;
		} region;
		struct {
			uint64_t size;
            uint64_t popcount;
		} entry_with_popcount;
		struct {
			uint64_t txid;
		} tx;
	};
};

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
	c->last_tx_span = NULL;

	struct pmemstream_span *region_span;
	region_span = pmemstream_get_span_for_offset(stream, region.offset);
	struct pmemstream_span_runtime region_sr =
		pmemstream_span_get_runtime(region_span);
	assert(region_sr.type == PMEMSTREAM_SPAN_REGION);

	while (c->offset < region_sr.region.size) {
		struct pmemstream_span *span =
			(struct pmemstream_span *)(region_sr.data + c->offset);
		struct pmemstream_span_runtime sr =
			pmemstream_span_get_runtime(span);
		if (sr.type == PMEMSTREAM_SPAN_TX) {
			c->last_tx_span = span;
		} else if (sr.type == PMEMSTREAM_SPAN_FREE) {
			break;
		}
		c->offset += sr.total_size;
	}

	*rcontext = c;
	return 0;
}
