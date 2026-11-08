use ../../util/test.nu *

# Watched checkins can update their own locks and resume watching after success or failure.

let local = server spawn

for kind in [directory attr file] {
	let path = if $kind == directory {
		artifact { tangram.ts: 'import a from "a/^1";' }
	} else {
		let directory = artifact { foo.tg.ts: 'import a from "a/^1";' }
		$directory | path join foo.tg.ts
	}
	let source = if $kind == directory { $path | path join tangram.ts } else { $path }
	let lock = if $kind == directory { 'auto' } else { $kind }

	for version in 0..3 {
		let dependency = artifact { tangram.ts: $'// a 1.($version).0' }
		tg tag -p $'a/1.($version).0' $dependency
		let id = tg checkin --no-tokens --watch --no-checkout-pointers $'--lock=($lock)' $path --update a | referent node
		let object = tg object get --blobs --depth=inf --no-tokens $id
		assert ($object | str contains $'// a 1.($version).0') 'the watched update should resolve the new dependency'
		let unchanged = tg checkin --no-tokens --watch --no-checkout-pointers $'--lock=($lock)' $path | referent node
		assert equal $unchanged $id
	}

	# A failed locked update must resume the existing watch.
	let dependency = artifact { tangram.ts: '// a 1.4.0' }
	tg tag -p a/1.4.0 $dependency
	let output = tg checkin --no-tokens --watch --no-checkout-pointers $'--lock=($lock)' --locked $path --update a | complete
	failure $output
	assert ($output.stderr | str contains 'the lock is out of date')

	# Observe a subsequent source edit through native filesystem notifications.
	'import a from "a/^1"; // after failure' | save --force $source
	wait_until {
		let id = tg checkin --no-tokens --watch --no-checkout-pointers $'--lock=($lock)' $path | referent node
		let id = if $kind == directory { $'($id)?get=tangram.ts' } else { $id }
		tg read $id | str contains '// after failure'
	} 'the watch should detect source edits after a failed checkin' --interval 200ms

	let id = tg checkin --no-tokens --watch --no-checkout-pointers $'--lock=($lock)' $path --update a | referent node
	let object = tg object get --blobs --depth=inf --no-tokens $id
	assert ($object | str contains '// a 1.4.0') 'the watch should remain usable after a failed checkin'

	# Clear the tags before exercising the next lock representation.
	for version in 0..4 {
		tg tag delete $'a/1.($version).0'
	}
}
