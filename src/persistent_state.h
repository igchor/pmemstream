#include <stdint.h>

struct shadow_state {
    /* Persistent and up-to-date value. */
    uint64_t *persistent;

    /* Runtime view - might be stale. */
    uint64_t runtime;
};

void shadow_state_initialize(struct shadow_state *state, uint64_t *persistent_value)
{
    state->persistent = persistent_value;
    state->runtime = *persistent_value;
}

void shadow_state_store(struct shadow_state *state, struct memops *memops, uint64_t value, int memory_order)
{
    __atomic_store_n(state->persistent, value, __ATOMIC_RELAXED);
    memops->persist(state->persistent, sizeof(value));
    __atomic_store_n(&state->runtime, value, memory_order);
}

uint64_t shadow_state_load(struct shadow_state *state, int memory_order)
{
    return __atomic_load_n(&state->runtime, memory_order);
}
