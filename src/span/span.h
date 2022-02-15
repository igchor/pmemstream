// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2021-2022, Intel Corporation */

/* Internal Header */

#ifndef LIBPMEMSTREAM_SPAN_H
#define LIBPMEMSTREAM_SPAN_H

#include "libpmemstream.h"

#include <stdint.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Span is a contiguous sequence of bytes, which are located on persistent storage.
 *
 * Span is always 8-bytes aligned. Its first 8 bytes hold information about size and type of a span. Type can be one
 * of the following:
 * - region
 * - entry
 * - empty space
 */
enum span_type {
	SPAN_EMPTY = 0b00ULL << 62,
	SPAN_REGION = 0b11ULL << 62,
	SPAN_ENTRY = 0b10ULL << 62,
	SPAN_UNKNOWN = 0b01ULL << 62
};

#define SPAN_TYPE_MASK (11ULL << 62)
#define SPAN_EXTRA_MASK (~SPAN_TYPE_MASK)

/* Metadata common for all span types. */
struct span_runtime_base {
	size_t total_size;
    uint64_t offset;
	uint64_t data_offset;
};

typedef uint64_t span_bytes;
struct pmemstream_data_runtime;

/* Convert offset to pointer to span. offset must be 8-bytes aligned. */
const span_bytes *span_offset_to_span_ptr(const struct pmemstream_data_runtime *data, uint64_t offset);

uint64_t span_get_size(const struct pmemstream_data_runtime *data, uint64_t offset);
enum span_type span_get_type(const struct pmemstream_data_runtime *data, uint64_t offset);

#ifdef __cplusplus
} /* end extern "C" */
#endif
#endif /* LIBPMEMSTREAM_SPAN_H */
