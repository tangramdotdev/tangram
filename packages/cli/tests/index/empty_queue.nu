use ../lib/test.nu *

# The indexer handles an empty queue gracefully.

let local = server spawn
timeout 1s tg index
