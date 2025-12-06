use ../../test.nu *

# Test that the indexer handles an empty queue gracefully.

let server = spawn
run timeout 1s tg clean


