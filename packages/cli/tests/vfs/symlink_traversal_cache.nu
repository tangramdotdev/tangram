use ../lib/test.nu *

# A warmed artifact path traverses a symlink without contacting the FUSE server again.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let transports = if (fuse_io_uring_available) { [read_write io_uring] } else { [read_write] }
for io in $transports {
	let local = server spawn --config { vfs: { io: $io, kind: fuse, passthrough: disabled } }
	let source = artifact { link: (symlink 'target'), target: 'contents' }
	let id = tg checkin $source | referent node
	let path = $local.directory | path join store $id link
	success (^stat -L $path | complete)

	# Pause the server so a second request cannot be mistaken for a cache hit.
	let pid = open ($local.directory | path join lock) | into int
	let parent = job id
	kill --signal 19 $pid
	job spawn {
		^stat -L $path | complete | job send $parent
	} | ignore
	let output = try { job recv --timeout 5sec } catch { null }
	kill --signal 18 $pid
	if $output == null {
		job recv --timeout 10sec | ignore
	}
	server stop $local
	assert ($output != null) 'the warmed symlink traversal contacted the paused FUSE server'
	success $output
}
