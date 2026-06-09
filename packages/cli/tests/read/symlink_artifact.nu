use ../../test.nu *

# Reading a symlink with an artifact target resolves to the target file's contents.

let server = spawn

let link = tg put 'tg.symlink({ "artifact": tg.file("via symlink") })' | str trim

let contents = tg read $link
assert equal $contents "via symlink" "the read contents should match the target file contents"
