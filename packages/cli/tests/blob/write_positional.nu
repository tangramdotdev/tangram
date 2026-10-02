use ../lib/test.nu *

# Writing a blob from a positional argument creates the same blob as writing the same bytes from standard input.

let local = server spawn

let positional = tg write --no-tokens "hello" | referent node
let piped = "hello" | tg write --no-tokens | referent node
assert equal $positional $piped "the positional and piped writes should create the same blob"
