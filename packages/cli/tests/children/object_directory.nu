use ../../test.nu *

# A directory's children are the files of its entries.

let server = spawn

let dir_id = tg put 'tg.directory({ "a.txt": tg.file("aaa"), "b.txt": tg.file("bbb") })' | str trim
let a_id = tg put 'tg.file("aaa")' | str trim
let b_id = tg put 'tg.file("bbb")' | str trim

let children = tg object children $dir_id | from json
assert equal ($children | sort) ([$a_id, $b_id] | sort) "the children should be the entry files"
