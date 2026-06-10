use ../../test.nu *

# The indexer handles an empty queue gracefully.

let server = spawn
timeout 1s tg clean
