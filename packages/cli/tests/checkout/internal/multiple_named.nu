use ../../../test.nu *

# An internal checkout materializes multiple tags with a shared ancestor.

let server = server spawn
let first = artifact 'first'
let second = artifact 'second'
let store = $server.directory | path join store

tg tag -p foo/first $first
tg tag -p foo/second $second

let paths = tg checkout foo/first foo/second | lines
assert equal $paths [($store | path join foo/first) ($store | path join foo/second)]
assert equal (open $paths.0) 'first'
assert equal (open $paths.1) 'second'
assert (($store | path join foo) | path exists) "expected the shared ancestor directory"
