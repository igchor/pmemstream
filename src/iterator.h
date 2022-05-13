// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2021-2022, Intel Corporation */

/* Internal Header */

#ifndef LIBPMEMSTREAM_ITERATOR_H
#define LIBPMEMSTREAM_ITERATOR_H

#include "libpmemstream.h"

#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

struct pmemstream_entry_iterator {
	/* Should this iterator perf region recovery when last valid entry is found. */
	bool perform_recovery;
	struct pmemstream *const stream;
	const struct pmemstream_region region;
	struct pmemstream_region_runtime *const region_runtime;
	uint64_t offset;
};

struct pmemstream_region_iterator {
	struct pmemstream *const stream;
	struct pmemstream_region region;
};

/* Initializes pmemstream_entry_iterator pointed to by 'iterator'. */
int entry_iterator_initialize(struct pmemstream_entry_iterator *iterator, struct pmemstream *stream,
			      struct pmemstream_region region, bool perform_recovery);

#ifdef __cplusplus
} /* end extern "C" */
#endif
#endif /* LIBPMEMSTREAM_ITERATOR_H */
