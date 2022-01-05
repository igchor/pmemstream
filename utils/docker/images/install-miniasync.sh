#!/usr/bin/env bash
# SPDX-License-Identifier: BSD-3-Clause
# Copyright 2022, Intel Corporation

#
# install-miniasync.sh
#		- installs linminiasync
#

set -e

if [ "${SKIP_MINIASYNC_BUILD}" ]; then
	echo "Variable 'SKIP_MINIASYNC_BUILD' is set; skipping building libminiasync"
	exit
fi

PREFIX="/usr"

# 22.12.2021: Merge pull request #22 from kswiecicki/dml-mover
MINIASYNC_VERSION="f8935072bc797e8244f1b17427c7aa796dbfd4b8"
echo "MINIASYNC_VERSION: ${MINIASYNC_VERSION}"

build_dir=$(mktemp -d -t miniasync-XXX)

git clone https://github.com/pmem/miniasync --shallow-since=2021-12-01 ${build_dir}

pushd ${build_dir}
git checkout ${MINIASYNC_VERSION}

mkdir build
pushd build

# turn off all redundant components
cmake .. -DCMAKE_INSTALL_PREFIX=${PREFIX}

make -j$(nproc) install

popd
popd
rm -r ${build_dir}
