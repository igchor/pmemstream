// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2022, Intel Corporation */

#include "empty.h"

struct empty_span_runtime empty_span_get_runtime(const struct pmemstream_data_runtime *data, uint64_t offset)
{
	const span_bytes *span = span_offset_to_span_ptr(data, offset);
	struct span_runtime srt;

	srt.base.offset = offset;
	srt.base.data_offset = offset + EMPTY_SPAN_METADATA_SIZE;
	srt.base.total_size = ALIGN_UP(srt.empty.size + EMPTY_SPAN_METADATA_SIZE, sizeof(span_bytes));
	srt.size = span_get_size(span);

	return srt;
}
