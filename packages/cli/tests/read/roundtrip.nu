use ../lib/test.nu *

# Writing a blob and reading it back returns the original contents.

let local = server spawn

let blob = "hello, world!\n" | tg write --no-tokens | referent node

let contents = tg read $blob
assert equal $contents "hello, world!" "the read contents should match the written contents"
