// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2021, Intel Corporation */

#include "mpmc_queue.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

#define mpmc_queue_OFFSET_MAX ((uint64_t)-1)

/* XXX: add support for dynamic producer registration? */
struct mpmc_queue *mpmc_queue_new(size_t num_producers)
{
	struct mpmc_queue *manager = malloc(sizeof(*manager) + num_producers * sizeof(struct mpmc_queue_producer));
	if (!manager) {
		return NULL;
	}

	manager->num_producers = num_producers;
	mpmc_queue_reset(manager, 0);

	return manager;
}

void mpmc_queue_destroy(struct mpmc_queue *manager)
{
	free(manager);
}

uint64_t mpmc_queue_acquire(struct mpmc_queue *manager, uint64_t producer_id, size_t size)
{
	struct mpmc_queue_producer *producer = &manager->producers[producer_id];
	uint64_t grant_offset = __atomic_load_n(&manager->produce_offset, __ATOMIC_RELAXED);
	bool success = false;

	do {
		__atomic_store_n(&producer->granted_offset, grant_offset, __ATOMIC_RELAXED);
		const bool weak = true;
		success = __atomic_compare_exchange_n(&manager->produce_offset, &grant_offset, grant_offset + size,
						      weak, __ATOMIC_RELAXED, __ATOMIC_RELAXED);
	} while (!success);

	return grant_offset;
}

void mpmc_queue_produce(struct mpmc_queue *manager, uint64_t producer_id)
{
	struct mpmc_queue_producer *producer = &manager->producers[producer_id];
	__atomic_store_n(&producer->granted_offset, mpmc_queue_OFFSET_MAX, __ATOMIC_RELAXED);
}

uint64_t mpmc_queue_consume(struct mpmc_queue *manager, size_t *ready_offset)
{
	/* produce_offset must be loaded before checking granted_offsets. */
	uint64_t produce_offset = __atomic_load_n(&manager->produce_offset, __ATOMIC_RELAXED);
	/* We can only consume offsets up to min_granted_offset. */
	uint64_t min_granted_offset = mpmc_queue_OFFSET_MAX;
	for (unsigned i = 0; i < manager->num_producers; i++) {
		uint64_t granted_offset = __atomic_load_n(&manager->producers[i].granted_offset, __ATOMIC_RELAXED);
		if (granted_offset < min_granted_offset) {
			min_granted_offset = granted_offset;
		}
	}

	/* All producers have commited. */
	if (min_granted_offset == mpmc_queue_OFFSET_MAX) {
		min_granted_offset = produce_offset;
	}

	uint64_t consume_offset = __atomic_load_n(&manager->consume_offset, __ATOMIC_RELAXED);
	if (consume_offset < min_granted_offset) {
		bool weak = false;
		bool success =
			__atomic_compare_exchange_n(&manager->consume_offset, &consume_offset, min_granted_offset, weak,
						    __ATOMIC_RELAXED, __ATOMIC_RELAXED);
		if (success) {
			*ready_offset = consume_offset;
			return min_granted_offset - consume_offset;
		}
	}

	*ready_offset = consume_offset;
	return 0;
}

void mpmc_queue_reset(struct mpmc_queue *manager, uint64_t offset)
{
	manager->produce_offset = offset;
	manager->consume_offset = offset;

	for (unsigned i = 0; i < manager->num_producers; i++) {
		manager->producers[i].granted_offset = mpmc_queue_OFFSET_MAX;
	}
}
