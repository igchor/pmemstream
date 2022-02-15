// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2022, Intel Corporation */

#include "span.h"

uint64_t span_get_size(const struct pmemstream_data_runtime *data, uint64_t offset)
{
    const span_bytes *span = span_offset_to_span_ptr(data, offset);
    return span[0] & SPAN_EXTRA_MASK;
}

enum span_type span_get_type(const struct pmemstream_data_runtime *data, uint64_t offset)
{
    const span_bytes *span = span_offset_to_span_ptr(data, offset);
    return span[0] & SPAN_TYPE_MASK;
}
