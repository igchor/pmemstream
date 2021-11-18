#include <stdint.h>
#include <stddef.h>

typedef uint32_t checksum32_fn_type(void *data, size_t len, void* arg);
typedef uint64_t checksum64_fn_type(void *data, size_t len, void* arg);

enum consistency_provider {
    NONE,
    CRC,
    POPCOUNT,
    CUSTOM32,
    CUSTOM64
};

struct consistency_descriptor {
    enum consistency_provider cp;

    union {
        void* arg;

        struct {
            checksum32_fn_type *checksum32_fn;
            void *arg;
        } custom32_data;

        struct {
            checksum64_fn_type *checksum64_fn;
            void *arg;
        } custom64_data;
    };
};

typedef void memcpy_fn(void *dst, const void *src, size_t len, unsigned flags);

int consistent_memcpy(void *dst, void *src, size_t size, consistency_descriptor dsc, memcpy_fn* memcpy_f, unsigned memcpy_flags) {
    if (dsc.cp == NONE) {
        return memcpy_f(dst, src, size, memcpy_flags);
    } else if (dsc.cp == CRC && IS_DSA_ENABLED???) {

    }
}

bool check_consistency(void *data, size_t size, consistency_descriptor dsc) {
    // can use DSA
}
