// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2021-2022, Intel Corporation */

/* Internal Header */

#ifndef LIBPMEMSTREAM_SPAN_ENTRY_H
#define LIBPMEMSTREAM_SPAN_ENTRY_H

#include <stdint.h>

#include "span.h"

#ifdef __cplusplus
extern "C" {
#endif

struct entry_runtime {
    struct span_runtime_base base;
    uint64_t size;
    uint64_t popcount;
};

#define ENTRY_METADATA_SIZE (sizeof(entry_runtime) - sizeof(struct span_runtime_base))

void entry_create(struct pmemstream_data_runtime *data, uint64_t offset, size_t data_size, size_t popcount);
void entry_create_no_flush_data(struct pmemstream_data_runtime *data, uint64_t offset, size_t data_size,
				     size_t popcount);

struct entry_runtime entry_get_runtime(const struct pmemstream_data_runtime *data, uint64_t offset);

#ifdef __cplusplus
} /* end extern "C" */
#endif
#endif /* LIBPMEMSTREAM_SPAN_ENTRY_H */
