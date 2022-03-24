#include <stdalign.h>
#include <stdint.h>
#include <stdlib.h>

struct data;

struct descriptor {
	struct data *head;
};

#define NUM_SLOTS 7

#define SLOT_FREE UINT64_MAX

struct data {
	alignas(64) uint64_t slots[NUM_SLOTS];
	struct data *next;
};

struct descriptor *make()
{
	struct descriptor *desc = (struct descriptor *)malloc(sizeof(*desc));
	if (!desc) {
		return NULL;
	}

	desc->head = (struct data *)aligned_alloc(alignof(*desc->head), sizeof(*desc->head));
	if (!desc->head) {
		free(desc);
		return NULL;
	}

	desc->head->next = desc->head;
	for (size_t i = 0; i < NUM_SLOTS; i++) {
		desc->head->slots[i] = SLOT_FREE;
	}

	return desc;
}

struct data *insert_new_node(struct descriptor *desc)
{
	struct data **insert_pos = &desc->head->next;
	struct data *old_next = *insert_pos;
	*insert_pos = (struct data *)aligned_alloc(alignof(struct data), sizeof(struct data));
	if (!(*insert_pos)) {
		return NULL;
	}

	(*insert_pos)->next = old_next;
	desc->head = (*insert_pos);

	return *insert_pos;
}

uint64_t *acquire(struct descriptor *desc)
{
	struct data *node = desc->head;
	do {
		for (size_t slot_index = 0; slot_index < NUM_SLOTS; slot_index++) {
			if (node->slots[slot_index] == SLOT_FREE) {
				return &node->slots[slot_index];
			}
		}
		node = node->next;
	} while (node != desc->head);

	node = insert_new_node(desc);
	if (!node) {
		return NULL;
	}

	return &node->slots[0];
}

void release(uint64_t *slot)
{
	__atomic_store_n(slot, SLOT_FREE, __ATOMIC_RELAXED);
}
