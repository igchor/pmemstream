// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2021, Intel Corporation */

#include "offset_manager.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

#define OFFSET_MANAGER_OFFSET_MAX ((uint64_t)-1)

/* XXX: add support for dynamic producer registration? */
struct offset_manager *offset_manager_new(size_t num_producers)
{
	struct offset_manager *manager =
		malloc(sizeof(*manager) + num_producers * sizeof(struct offset_manager_producer));
	if (!manager) {
		return NULL;
	}

	manager->num_producers = num_producers;
	offset_manager_reset(manager, 0);

	return manager;
}

void offset_manager_destroy(struct offset_manager *manager)
{
	free(manager);
}

uint64_t offset_manager_acquire(struct offset_manager *manager, uint64_t producer_id, size_t size)
{
	struct offset_manager_producer *producer = &manager->producers[producer_id];
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

void offset_manager_produce(struct offset_manager *manager, uint64_t producer_id)
{
	struct offset_manager_producer *producer = &manager->producers[producer_id];
	__atomic_store_n(&producer->granted_offset, OFFSET_MANAGER_OFFSET_MAX, __ATOMIC_RELAXED);
}

uint64_t offset_manager_consume(struct offset_manager *manager, size_t *ready_offset)
{
	/* produce_offset must be loaded before checking granted_offsets. */
	uint64_t produce_offset = __atomic_load_n(&manager->produce_offset, __ATOMIC_RELAXED);
	/* We can only consume offsets up to min_granted_offset. */
	uint64_t min_granted_offset = OFFSET_MANAGER_OFFSET_MAX;
	for (unsigned i = 0; i < manager->num_producers; i++) {
		uint64_t granted_offset = __atomic_load_n(&manager->producers[i].granted_offset, __ATOMIC_RELAXED);
		if (granted_offset < min_granted_offset) {
			min_granted_offset = granted_offset;
		}
	}

	/* All producers have commited. */
	if (min_granted_offset == OFFSET_MANAGER_OFFSET_MAX) {
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

void offset_manager_reset(struct offset_manager *manager, uint64_t offset)
{
	manager->produce_offset = offset;
	manager->consume_offset = offset;

	for (unsigned i = 0; i < manager->num_producers; i++) {
		manager->producers[i].granted_offset = OFFSET_MANAGER_OFFSET_MAX;
	}
}
