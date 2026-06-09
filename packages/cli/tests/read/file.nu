use ../../test.nu *

# Reading a file artifact returns its contents.

let server = spawn

let file_id = tg put 'tg.file("file contents")' | str trim

let contents = tg read $file_id
assert equal $contents "file contents" "the read contents should match the file contents"
