use ../../test.nu *

# Following a group selects the most recent matching version.

let server = server spawn

let one = tg put 'tg.file("one")' | str trim
tg tag put -p a/1.0.0 $one
let two = tg put 'tg.file("two")' | str trim
tg tag put -p a/1.1.0 $two

let output = tg get "a?follow=true" | complete
success $output
let expected = tg get $two | str trim
assert (($output.stdout | str trim) == $expected) "the tag should resolve to the latest version"
