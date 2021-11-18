#include <stdint.h>
#include <stddef.h>

int util_is_zeroed(const void *addr, size_t len);
int util_checksum(void *addr, size_t len, uint64_t *csump, int insert, size_t skip_off);
uint64_t util_checksum_seq(const void *addr, size_t len, uint64_t csum);
