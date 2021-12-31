// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2021, Intel Corporation */

#include "spsc_queue.h"
#include "common/util.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>

struct spsc_queue *spsc_queue_new(size_t size)
{
	if (size == 0) {
		return NULL;
	}

	assert(size % 64 == 0);
	struct spsc_queue *s = aligned_alloc(64, sizeof(*s));
	if (!s) {
		return NULL;
	}

	s->data = aligned_alloc(64, size);
	if (!s->data) {
		free(s);
		return NULL;
	}

	s->size = size;
	s->read_pos = 0;
	s->write_pos = 0;

	return s;
}

void spsc_queue_destroy(struct spsc_queue *s)
{
	free(s->data);
	free(s);
}

static size_t spsc_queue_get_free(uint64_t read_pos, uint64_t write_pos, size_t size)
{
	size_t ret;

	if (read_pos == write_pos) {
		ret = size;
	} else if (read_pos > write_pos) {
		ret = read_pos - write_pos;
	} else {
		ret = size - (write_pos - read_pos);
	}

	/*
	 * One slot is always reserved to distinguish between empty and full case.
	 * if read_pos == write_pos, queue is empty (free_space = s->size - 8)
	 * if read_pos == write_pos + 8, queue is full (free_space = 0)
	 */
	return ret - 8;
}

static size_t spsc_queue_get_available(uint64_t read_pos, uint64_t write_pos, size_t size)
{
	if (read_pos == write_pos) {
		return 0;
	} else if (read_pos > write_pos) {
		return size - (read_pos - write_pos);
	} else {
		return write_pos - read_pos;
	}
}

int spsc_queue_try_enqueue(struct spsc_queue *s, struct spsc_queue_src_descriptor *descriptors, size_t num_descriptors)
{
	size_t size = 0;
	for (size_t i = 0; i < num_descriptors; i++) {
		size += descriptors[i].size;
	}

	/* read_pos can be concurrently modified by try_dequeue hence the load. */
	uint64_t read_pos = __atomic_load_n(&s->read_pos, __ATOMIC_ACQUIRE);
	uint64_t write_pos = s->write_pos;

	assert(read_pos % 8 == 0);
	assert(write_pos % 8 == 0);

	size_t free_space = spsc_queue_get_free(read_pos, write_pos, s->size);
	if (free_space < size + sizeof(uint64_t) /* For entry size */) {
		return -1;
	}

	/* There must always be at least 8 bytes at the end .*/
	assert(write_pos <= s->size - sizeof(size));

	/* Save size of the entry. */
	*((uint64_t *)(s->data + write_pos)) = size;
	write_pos += sizeof(size);

	if (size == 0) {
		return 0;
	}

	for (size_t i = 0; i < num_descriptors; i++) {
		size_t remaining_to_wrap = s->size - write_pos;
		size_t first_copy = remaining_to_wrap < descriptors[i].size ? remaining_to_wrap : descriptors[i].size;
		memcpy(s->data + write_pos, descriptors[i].data, first_copy);

		if (descriptors[i].size - first_copy) {
			/* Wrapround. */
			size_t remaining = descriptors[i].size - first_copy;
			memcpy(s->data, descriptors[i].data + first_copy, remaining);
			write_pos = remaining;
		} else {
			/* No wrapround. */
			write_pos += first_copy;
		}
	}

	write_pos = ALIGN_UP(write_pos, 8ULL);
	if (write_pos >= s->size) {
		write_pos = 0;
	}

	__atomic_store_n(&s->write_pos, write_pos, __ATOMIC_RELEASE);

	return 0;
}

int spsc_queue_try_dequeue_start(struct spsc_queue *s, struct spsc_queue_src_descriptor *descriptor1,
				 struct spsc_queue_src_descriptor *descriptor2)
{
	/* write_pos can be concurrently modified by try_enqueue hence the load. */
	uint64_t read_pos = s->read_pos;
	uint64_t write_pos = __atomic_load_n(&s->write_pos, __ATOMIC_ACQUIRE);

	assert(read_pos % 8 == 0);
	assert(write_pos % 8 == 0);

	size_t available_bytes = spsc_queue_get_available(read_pos, write_pos, s->size);
	if (available_bytes == 0) {
		return -1;
	}

	uint64_t size = *((uint64_t *)(s->data + read_pos));
	read_pos += sizeof(uint64_t);

	assert(available_bytes >= size + sizeof(uint64_t));

	if (size > s->size - read_pos) {
		/* Wrapround. */
		descriptor1->size = s->size - read_pos;
		descriptor1->data = s->data + read_pos;

		descriptor2->size = size - descriptor1->size;
		descriptor2->data = s->data;
	} else {
		/* No wrapround. */
		descriptor1->size = size;
		descriptor1->data = s->data + read_pos;

		descriptor2->size = 0;
		descriptor2->data = NULL;
	}

	return 0;
}

void spsc_queue_dequeue_finish(struct spsc_queue *s, size_t size)
{
	size += sizeof(uint64_t);

	if (ALIGN_UP(s->read_pos + size, 8ULL) >= s->size) {
		/* Wraparound. */
		__atomic_store_n(&s->read_pos, ALIGN_UP(s->read_pos + size - s->size, 8ULL), __ATOMIC_RELEASE);
	} else {
		/* No wrapround. */
		__atomic_store_n(&s->read_pos, ALIGN_UP(s->read_pos + size, 8ULL), __ATOMIC_RELEASE);
	}
}

// XXX: peek function for non-consuming readers? Need to implement optimistic concurrency control there.
