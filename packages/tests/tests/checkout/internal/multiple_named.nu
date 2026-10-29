use ../../../lib/test.nu *

# An internal checkout materializes multiple tags with a shared ancestor.

let local = server spawn
let first = artifact 'first'
let second = artifact 'second'

tg tag -p foo/first $first
tg tag -p foo/second $second

let paths = tg checkout foo/first foo/second | lines
assert equal (open $paths.0) 'first'
assert equal (open $paths.1) 'second'
