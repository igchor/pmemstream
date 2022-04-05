// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2022, Intel Corporation */

#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <stdexcept>
#include <iostream>

static inline void UT_FATAL(const char *format, ...)
{
	va_list args_list;
	va_start(args_list, format);
	vprintf(format, args_list);
	va_end(args_list); 

	throw std::runtime_error("XXX");
}

int main(int argc, char *argv[])
{
	try {
		UT_FATAL("%d%d%d%d%d%d%d",0,0,0,0,0,0,0);
	} catch(std::runtime_error &e) {
		std::cout << e.what() << std::endl;
	}

	return 0;
}

// compile with:
// clang++  -O2 -g -DNDEBUG -lunwind test.cpp
