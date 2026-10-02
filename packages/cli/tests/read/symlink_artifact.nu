use ../lib/test.nu *

# Reading a symlink with an artifact target resolves to the target file's contents.

let local = server spawn

let link = tg put --no-tokens 'tg.symlink({ "artifact": tg.file("via symlink") })' | referent node

let contents = tg read $link
assert equal $contents "via symlink" "the read contents should match the target file contents"
