// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2022, Intel Corporation */

#include "entry.h"

/* Internal helper for entry_create. */
static void entry_create_internal(struct pmemstream_data_runtime *data, uint64_t offset, size_t data_size,
				       size_t popcount, size_t flush_size)
{
	span_bytes *span = (span_bytes *)span_offset_to_span_ptr(data, offset);
	assert((data_size & SPAN_TYPE_MASK) == 0);

	// XXX - use variadic mempcy to store data and metadata at once
	span[0] = data_size | SPAN_ENTRY;
	span[1] = popcount;

	data->persist(span, flush_size);
}

/* Creates entry span at given offset.
 * It sets entry's metadata: type, size of the data and popcount.
 * It flushes metadata along with the data (of given 'data_size'), which are stored in the spans following metadata.
 */
void entry_create(struct pmemstream_data_runtime *data, uint64_t offset, size_t data_size, size_t popcount)
{
	entry_create_internal(data, offset, data_size, popcount, ENTRY_METADATA_SIZE + data_size);
}

/* Creates entry span at given offset.
 * It sets entry's metadata: type, size of the data and popcount.
 * It flushes only the metadata.
 */
void entry_create_no_flush_data(struct pmemstream_data_runtime *data, uint64_t offset, size_t data_size,
				     size_t popcount)
{
	entry_create_internal(data, offset, data_size, popcount, ENTRY_METADATA_SIZE);
}

struct entry_runtime entry_get_runtime(const struct pmemstream_data_runtime *data, uint64_t offset)
{
	const span_bytes *span = span_offset_to_span_ptr(data, offset);
	struct entry_runtime srt;

	srt.base.offset = offset;
	srt.base.data_offset = offset + ENTRY_METADATA_SIZE;
	srt.base.total_size = ALIGN_UP(srt.entry.size + ENTRY_METADATA_SIZE, sizeof(span_bytes));
	srt.size = span_get_size(data, offset);
	srt.popcount = span[1];

	return srt;
}
