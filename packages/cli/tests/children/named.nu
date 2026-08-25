use ../../test.nu *

# Named nodes expose their direct graph children.

let server = server spawn

let artifact = artifact 'contents'
let target = tg checkin $artifact | str trim
tg tag -p foo/bar $target
tg tag -p foo/baz $target

let bar = tg get foo/bar | from json | get id
let baz = tg get foo/baz | from json | get id
let children = tg children foo | from json
assert equal $children [$bar $baz] "the group children should be ordered by ID"

let children = tg children foo/bar | from json
assert equal $children [$target] "the tag child should be its target"
