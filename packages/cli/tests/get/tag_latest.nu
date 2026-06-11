use ../../test.nu *

# Resolving a tag pattern with the resolve option selects the most recent matching version.

let server = spawn

let one = tg put 'tg.file("one")' | str trim
tg tag put a/1.0.0 $one
let two = tg put 'tg.file("two")' | str trim
tg tag put a/1.1.0 $two

let output = tg get --resolve a | complete
success $output
let expected = tg get $two | str trim
assert (($output.stdout | str trim) == $expected) "the tag should resolve to the latest version"
