#include <util.h>

#include <endian.h>

/*
 * util_is_zeroed -- check if given memory range is all zero
 */
int
util_is_zeroed(const void *addr, size_t len)
{
	const char *a = addr;

	if (len == 0)
		return 1;

	if (a[0] == 0 && memcmp(a, a + 1, len - 1) == 0)
		return 1;

	return 0;
}

/*
 * util_checksum_compute -- compute Fletcher64-like checksum
 *
 * csump points to where the checksum lives, so that location
 * is treated as zeros while calculating the checksum. The
 * checksummed data is assumed to be in little endian order.
 */
static uint64_t
util_checksum_compute(void *addr, size_t len, uint64_t *csump, size_t skip_off)
{
	if (len % 4 != 0)
		abort();

	uint32_t *p32 = addr;
	uint32_t *p32end = (uint32_t *)((char *)addr + len);
	uint32_t *skip;
	uint32_t lo32 = 0;
	uint32_t hi32 = 0;

	if (skip_off)
		skip = (uint32_t *)((char *)addr + skip_off);
	else
		skip = (uint32_t *)((char *)addr + len);

	while (p32 < p32end)
		if (p32 == (uint32_t *)csump || p32 >= skip) {
			/* lo32 += 0; treat first 32-bits as zero */
			p32++;
			hi32 += lo32;
			/* lo32 += 0; treat second 32-bits as zero */
			p32++;
			hi32 += lo32;
		} else {
			lo32 += le32toh(*p32);
			++p32;
			hi32 += lo32;
		}

	return (uint64_t)hi32 << 32 | lo32;
}

/*
 * util_checksum -- compute Fletcher64-like checksum
 *
 * csump points to where the checksum lives, so that location
 * is treated as zeros while calculating the checksum.
 * If insert is true, the calculated checksum is inserted into
 * the range at *csump.  Otherwise the calculated checksum is
 * checked against *csump and the result returned (true means
 * the range checksummed correctly).
 */
int
util_checksum(void *addr, size_t len, uint64_t *csump,
		int insert, size_t skip_off)
{
	uint64_t csum = util_checksum_compute(addr, len, csump, skip_off);

	if (insert) {
		*csump = htole64(csum);
		return 1;
	}

	return *csump == htole64(csum);
}

/*
 * util_checksum_seq -- compute sequential Fletcher64-like checksum
 *
 * Merges checksum from the old buffer with checksum for current buffer.
 */
uint64_t
util_checksum_seq(const void *addr, size_t len, uint64_t csum)
{
	if (len % 4 != 0)
		abort();
	const uint32_t *p32 = addr;
	const uint32_t *p32end = (const uint32_t *)((const char *)addr + len);
	uint32_t lo32 = (uint32_t)csum;
	uint32_t hi32 = (uint32_t)(csum >> 32);
	while (p32 < p32end) {
		lo32 += le32toh(*p32);
		++p32;
		hi32 += lo32;
	}
	return (uint64_t)hi32 << 32 | lo32;
}
