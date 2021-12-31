
// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2021, Intel Corporation */

#include <stdint.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

/* avoid false sharing by padding the variable */
#define CACHELINE_PADDING(type, name)                                                                                  \
	union {                                                                                                        \
		type name;                                                                                             \
		uint64_t name##_padding[8];                                                                            \
	}

struct spsc_queue_src_descriptor {
	size_t size;
	const uint8_t *data;
};

struct spsc_queue { // XXX: CACHELINE_ALIGNED
	CACHELINE_PADDING(uint64_t, read_pos);
	CACHELINE_PADDING(uint64_t, write_pos);

	size_t size;
	uint8_t *data; // XXX: CACHELINE_ALIGNED
};

struct spsc_queue *spsc_queue_new(size_t size);
void spsc_queue_destroy(struct spsc_queue *s);
int spsc_queue_try_enqueue(struct spsc_queue *s, struct spsc_queue_src_descriptor *descriptors, size_t num_descriptors);
int spsc_queue_try_dequeue_start(struct spsc_queue *s, struct spsc_queue_src_descriptor *descriptor1,
				 struct spsc_queue_src_descriptor *descriptor2);
void spsc_queue_dequeue_finish(struct spsc_queue *s, size_t size);

#ifdef __cplusplus
} /* end extern "C" */
#endif
