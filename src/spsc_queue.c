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
	assert(ret > 8);
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

size_t ringbuf_acquire(struct spsc_queue *s, size_t size)
{
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
	assert(s->write_pos <= s->size - sizeof(size));

	return write_pos;
}

void spsc_queue_try_produce(struct spsc_queue *s, size_t size)
{
	if (size == 0) {
		return;
	}

	size_t write_pos = ALIGN_UP(s->write_pos, sizeof(uint64_t));
	if (write_pos >= s->size) {
		write_pos = 0;
	}

	__atomic_store_n(&s->write_pos, write_pos, __ATOMIC_RELEASE);
}

int spsc_queue_try_consume(struct spsc_queue *s, size_t *user_offset, size_t *user_size)
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

	*user_offset = read_pos;
	*user_size = available_bytes;

	return 0;
}

void spsc_queue_release(struct spsc_queue *s, size_t size)
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
