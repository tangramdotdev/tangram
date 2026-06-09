use ../../test.nu *

# Writing a blob from a positional argument creates the same blob as writing the same bytes from standard input.

let server = spawn

let positional = tg write "hello" | str trim
let piped = "hello" | tg write | str trim
assert equal $positional $piped "the positional and piped writes should create the same blob"
