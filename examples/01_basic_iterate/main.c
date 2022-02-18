
struct pmemstream_append_ret {
	int error;
	struct future_commited commited;
	struct future_persisted persisted;
};

struct pmemstream_append_ret pmemstream_append(struct pmemstream *stream, struct pmemstream_region region,
		      struct pmemstream_region_runtime *region_runtime, const void *data, size_t size);

uint64_t pmemstream_sync(struct pmemstream *stream, uint64_t offset);


int main(int argc, char *argv[])
{
	// BG THREAD
	std::thread bg([&]{
		while (true) {
			// Accept offset to which data should be persisted (or MAX_OFFSET for persisting as much as possible)
			// Can have blocking semantic similar to io_uring_wait_cqe or non-blocking like io_uring_peek_cqe
			uint64_t persisted_offset = pmemstream_sync(stream, MAX_OFFSET);
			user_provided_callback(persisted_offset);

			sleep(...);
		}
	});

	// USER THREAD
	struct pmemstream_append_ret rets[3];
	
	ret[0] = pmemstream_append(stream, region, NULL, data, size);
	ret[1] = pmemstream_append(stream, region, NULL, data, size);
	ret[2] = pmemstream_append(stream, region, NULL, data, size);

	size_t completed = 0;
	while (completed != 3) {
		for (int i = 0; i < 3; i++) {
			// future_poll can just read the status of operation (which happens in background) and only update DRAM metadata
			// or it can help with making progress (help with persisting). This can be configurable.
			if (future_poll(ret[i].persisted) == COMPLETED) {
				completed++;
			}
		}

		// APPLICATION CODE
	}
}

// Extra info: chaining operatations for hardware accelerators
// When accelerator (e,g, DSA) is used we need to chain operations which are going to be executed on
// accelerator with the ones which need to be executed on CPU. E.g. append for DSA will return future which
// looks like this:
// FUTURE {
//		DSA_MEMCPY;
// 		PMEMSTREAM_INTERNAL_COMMIT;
//		PMEMSTREAM_INTERNAL_MAKE_PERSISTENT;
// }
// future_poll() for DSA_MEMCPY reads status flag to check when operation completes, after that PMEMSTREAM_INTERNAL_COMMIT is started
// future_poll() for PMEMSTREAM_INTERNAL_COMMIT actually exectues the function on CPU (mark the append as commited)
// future_poll() for PMEMSTREAM_INTERNAL_MAKE_PERSISTENT can either wait for data to become persistent (rely on BG thread calling pmemstream_sync)
//					 or it can also call pmemstream_sync itself).


////////////// TIMESTAMPS ////////////////////////////////////////

struct pmemstream_append_ret {
	...
	uint64_t timestamp;
};

{
	ret[0] = pmemstream_append(stream, region1, NULL, data, size);
	ret[1] = pmemstream_append(stream, region2, NULL, data, size);
	ret[2] = pmemstream_append(stream, region3, NULL, data, size);

	...

	auto max_timestamp = std::max(ret[0].timestamp, ret[1].timestamp, ret[2].timestamp);
	if (pmemstream_persisted_offset(stream) >= max_timestamp) {
		// all operations completed
	}
}
