use ../../test.nu *

# Writing a blob and reading it back returns the original contents.

let server = spawn

let blob = "hello, world!\n" | tg write | str trim

let contents = tg read $blob
assert equal $contents "hello, world!" "the read contents should match the written contents"
