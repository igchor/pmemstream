// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2022, Intel Corporation */

#include "valgrind_internal.h"

#include <stdlib.h>

unsigned On_valgrind = 0;
unsigned On_pmemcheck = 0;
unsigned On_memcheck = 0;
unsigned On_helgrind = 0;
unsigned On_drd = 0;

void set_valgrind_internals()
{
	if (getenv("LIBPMEMSTREAM_TRACER_PMEMCHECK")) {
		On_pmemcheck = 1;
		On_valgrind = 1;
	} else if (getenv("LIBPMEMSTREAM_TRACER_MEMCHECK")) {
		On_memcheck = 1;
		On_valgrind = 1;
	} else if (getenv("LIBPMEMSTREAM_TRACER_HELGRIND")) {
		On_helgrind = 1;
		On_valgrind = 1;
	} else if (getenv("LIBPMEMSTREAM_TRACER_DRD")) {
		On_drd = 1;
		On_valgrind = 1;
	}
}
