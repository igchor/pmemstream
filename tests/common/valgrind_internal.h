// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2022, Intel Corporation */

/* XXX: remove this once valgrind headers are part of this project. */
#ifndef LIBPMEMSTREAM_TEST_VALGRIND_INTERNAL_H
#define LIBPMEMSTREAM_TEST_VALGRIND_INTERNAL_H

#ifdef __cplusplus
extern "C" {
#endif

extern unsigned On_valgrind;
extern unsigned On_pmemcheck;
extern unsigned On_memcheck;
extern unsigned On_helgrind;
extern unsigned On_drd;

void set_valgrind_internals();

#ifdef __cplusplus
}
#endif

#endif /* LIBPMEMSTREAM_TEST_VALGRIND_INTERNAL_H */
