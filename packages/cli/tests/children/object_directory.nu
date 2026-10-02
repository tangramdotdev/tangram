use ../lib/test.nu *

# A directory's children are the files of its entries.

let local = server spawn

let dir_id = tg put --no-tokens 'tg.directory({ "a.txt": tg.file("aaa"), "b.txt": tg.file("bbb") })' | referent node
let a_id = tg put --no-tokens 'tg.file("aaa")' | referent node
let b_id = tg put --no-tokens 'tg.file("bbb")' | referent node

let children = tg object children $dir_id | from json
assert equal ($children | sort) ([$a_id, $b_id] | sort) "the children should be the entry files"
