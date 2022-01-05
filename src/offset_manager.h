// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2021, Intel Corporation */

/* Internal Header */

#ifndef LIBPMEMSTREAM_OFFSET_MANAGER_H
#define LIBPMEMSTREAM_OFFSET_MANAGER_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

struct offset_manager_producer {
	uint64_t granted_offset;

	/* avoid false sharing by padding the variable */
	// XXX: calculate 7 from CACHELINE_SIZE
	uint64_t padding[7];
};

struct offset_manager {
	uint64_t num_producers;
	uint64_t produce_offset;
	uint64_t padding_produce_offset[6];

	uint64_t consume_offset;
	uint64_t padding_consume_offset[7];

	struct offset_manager_producer producers[];
};

/* XXX: add support for dynamic producer registration? */
struct offset_manager *offset_manager_new(size_t num_producers);
void offset_manager_destroy(struct offset_manager *manager);

uint64_t offset_manager_acquire(struct offset_manager *manager, uint64_t producer_id, size_t size);
void offset_manager_produce(struct offset_manager *manager, uint64_t producer_id);

uint64_t offset_manager_consume(struct offset_manager *manager, size_t *ready_offset);

void offset_manager_reset(struct offset_manager *manager, uint64_t offset);

#ifdef __cplusplus
} /* end extern "C" */
#endif
#endif /* LIBPMEMSTREAM_OFFSET_MANAGER_H */
