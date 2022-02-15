// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2021-2022, Intel Corporation */

/* Internal Header */

#ifndef LIBPMEMSTREAM_SPAN_EMPTY_H
#define LIBPMEMSTREAM_SPAN_EMPTY_H

#include <stdint.h>

#include "span.h"

#ifdef __cplusplus
extern "C" {
#endif

struct empty_span_runtime {
    struct span_runtime_base base;
    uint64_t size;
};

#define SPAN_EMPTY_METADATA_SIZE (sizeof(empty_span_runtime) - sizeof(struct span_runtime_base))

void empty_span_create(struct pmemstream_data_runtime *data, uint64_t offset, size_t data_size);

struct empty_span_runtime empty_span_get_runtime(const struct pmemstream_data_runtime *data, uint64_t offset);

#ifdef __cplusplus
} /* end extern "C" */
#endif
#endif /* LIBPMEMSTREAM_SPAN_EMPTY_H */
