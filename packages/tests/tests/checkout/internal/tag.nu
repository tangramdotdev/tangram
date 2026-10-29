use ../../../util/test.nu *

# An internal checkout of a tag follows the tag when it is replaced or deleted.

let local = server spawn

let first = artifact 'first'
tg tag dep $first

let path = tg checkout dep | str trim
assert equal (open $path) 'first' "expected the tag path to contain the first artifact"

let second = artifact 'second'
tg tag --force dep $second
# Tag writes return before the indexer invalidates checkout entries.
tg index

let path = tg checkout dep | str trim
assert equal (open $path) 'second' "expected the tag path to contain the second artifact"

tg tag delete dep | ignore
tg index
failure (tg checkout dep | complete) "expected checking out a deleted tag to fail"
