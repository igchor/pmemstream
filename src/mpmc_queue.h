// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2021, Intel Corporation */

/* Internal Header */

#ifndef LIBPMEMSTREAM_MPMC_QUEUE_H
#define LIBPMEMSTREAM_MPMC_QUEUE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

struct mpmc_queue_producer {
	uint64_t granted_offset;

	/* avoid false sharing by padding the variable */
	// XXX: calculate 7 from CACHELINE_SIZE
	uint64_t padding[7];
};

/* mpmc_queue: a lock-free, unbounded, multi-producer, multi-consumer queue. */
struct mpmc_queue {
	uint64_t num_producers;
	uint64_t produce_offset;
	uint64_t padding_produce_offset[6];

	uint64_t consume_offset;
	uint64_t padding_consume_offset[7];

	struct mpmc_queue_producer producers[];
};

/* XXX: add support for dynamic producer registration? */
struct mpmc_queue *mpmc_queue_new(size_t num_producers);
void mpmc_queue_destroy(struct mpmc_queue *manager);

uint64_t mpmc_queue_acquire(struct mpmc_queue *manager, uint64_t producer_id, size_t size);
void mpmc_queue_produce(struct mpmc_queue *manager, uint64_t producer_id);

uint64_t mpmc_queue_consume(struct mpmc_queue *manager, size_t *ready_offset);

void mpmc_queue_reset(struct mpmc_queue *manager, uint64_t offset);

#ifdef __cplusplus
} /* end extern "C" */
#endif
#endif /* LIBPMEMSTREAM_MPMC_QUEUE_H */
