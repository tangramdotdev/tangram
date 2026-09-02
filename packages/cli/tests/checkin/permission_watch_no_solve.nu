use ../../test.nu *
use ../lib/checkin.nu checkin-output

# A watched no-solve update reuses permission summaries for unchanged cyclic children.

let server = server spawn
let directory = artifact {
	a.tg.ts: 'import "./b.tg.ts";'
	b.tg.ts: 'import "./a.tg.ts";'
	value: 'first'
}
let first = checkin-output $server $directory --no-solve --watch
assert equal $first.permissions [object_subtree] "the initial checkin should have subtree permission"

'second' | save --force ($directory | path join value)
tg --url $server.url watch touch $directory ($directory | path join value)

let second = checkin-output $server $directory --no-solve --watch
assert equal $second.permissions [object_subtree] "the update should preserve subtree permission"
