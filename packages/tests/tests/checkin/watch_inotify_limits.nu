use ../../util/test.nu *

# Watched checkins reflect edits even when the server cannot create an inotify instance or register a watch.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

for limit in [instances watches] {
	let local = server spawn --inotify-limit $limit
	let path = artifact { tangram.ts: 'export default () => "one";' }
	let first = tg checkin --no-tokens $path --watch | referent node

	# Edit the package and compare the watched checkin with a fresh read from disk.
	'export default () => "two";' | save --force ($path | path join tangram.ts)
	let plain = tg checkin --no-tokens $path --no-watch | referent node
	assert ($plain != $first) "the edit should change the checked in package"
	let watched = tg checkin --no-tokens $path --watch | referent node
	assert equal $watched $plain $"the watched checkin should reflect the edit with no inotify ($limit) available"
}
