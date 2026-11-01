use ../../util/test.nu *

# tg ps lists processes in pages that continue from a cursor without skipping or repeating a process.

let local = server spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose --name alice | from json

# Run three processes to completion.
let processes = 0..2 | each { |index|
	let path = artifact {
		tangram.ts: $'export default () => ($index);',
	}
	let process = tg --token $alice.token spawn --no-tokens $path | referent node
	tg --token $alice.token wait $process | ignore
	$process
}
tg index

# List the finished processes two at a time.
let first = tg --token $alice.token ps --verbose --status finished --limit 2 | from json
assert equal ($first.data | length) 2
assert ($first.cursor? != null) "the first page should have a cursor"
let second = tg --token $alice.token ps --verbose --status finished --limit 2 --cursor $first.cursor | from json
assert equal ($second.cursor? | default null) null "the second page should not have a cursor"
let listed = $first.data | append $second.data | get node
assert equal ($listed | sort) ($processes | sort)
