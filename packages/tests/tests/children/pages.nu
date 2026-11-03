use ../../util/test.nu *

# Children of an object and of a group page from a cursor without skipping or repeating a child.

let local = server spawn

# Page through the children of a directory.
let directory = tg put --no-tokens 'tg.directory({ "a.txt": tg.file("a"), "b.txt": tg.file("b"), "c.txt": tg.file("c") })' | referent node
tg index
let all = tg children --all $directory | from json
let first = tg children --verbose --limit 2 $directory | from json
assert equal ($first.data | length) 2
let second = tg children --verbose --limit 2 --cursor $first.cursor $directory | from json
assert equal ($second.cursor? | default null) null "the last page should not have a cursor"
assert equal ($first.data | append $second.data | get node) $all "the pages should list each child once"

# Page through the children of a group, and keep paging correctly after a child before the cursor is deleted.
tg group create team
for name in [a b c d] {
	let file = tg put --no-tokens $"tg.file\(\"($name)\"\)" | referent node
	tg tag put $"team/($name)" $file
}
let first = tg children --verbose --limit 2 team | from json
assert equal ($first.data | length) 2
tg tag delete team/a
let second = tg children --verbose --limit 2 --cursor $first.cursor team | from json
assert equal ($second.data | length) 2 "deleting an earlier child should not skip a later one"
assert equal ($second.cursor? | default null) null "the last page should not have a cursor"
