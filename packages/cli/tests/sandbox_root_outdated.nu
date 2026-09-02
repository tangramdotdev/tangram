use ../test.nu *

# A server atomically rebuilds out-of-date container roots.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let directory = mktemp --directory
let container = $directory | path join 'container'
let root = $directory | path join 'container' 'root'

let server = server spawn --directory $directory

let path = artifact {
	tangram.ts: '
		export default () => {
			console.log("Hello, World!");
		};
	'
}

let root_modified = ls --directory $root | first | get modified
let output = tg run --sandbox $path
assert equal $output 'Hello, World!'
assert not ($root | path join '.tangram-version' | path exists)
let image = $directory | path join 'vm' 'image.squashfs'
let snapshot_preserved = $directory | path join 'vm' 'snapshot' 'preserved'
let vm = $image | path exists
if $vm {
	let current_root_modified = ls --directory $root | first | get modified
	assert equal $current_root_modified $root_modified 'expected VM image creation to leave the root unchanged'
	touch $snapshot_preserved
}

# A current root is reused.
server stop $server
^touch --date 'now + 1 hour' $root
let preserved = $root | path join 'tmp' 'preserved'
touch $preserved
let server = server start $server
assert ($preserved | path exists) 'expected the current root to be reused'

# A failed rebuild leaves the installed root untouched.
server stop $server
^touch --date '1970-01-01 UTC' $root
^chmod 0555 $container
let error = try {
	server start $server | ignore
	null
} catch { |error| $error }
^chmod 0755 $container
assert ($error != null) 'expected the rebuild to fail'
assert ($preserved | path exists) 'expected the failed rebuild to preserve the installed root'

# A subsequent successful start replaces the invalid root.
let server = server start $server
assert not ($preserved | path exists) 'expected the invalid root to be replaced'
if $vm {
	^touch --reference $root $image
	let root_modified = ls --directory $root | first | get modified
	let image_modified = ls --directory $image | first | get modified
	assert equal $image_modified $root_modified 'expected equal mtimes before rebuilding the VM image'
}
let root_modified = ls --directory $root | first | get modified
let output = tg run --sandbox $path
assert equal $output 'Hello, World!'
assert not ($root | path join '.tangram-version' | path exists)
if $vm {
	let current_root_modified = ls --directory $root | first | get modified
	let image_modified = ls --directory $image | first | get modified
	assert equal $current_root_modified $root_modified 'expected VM image creation to leave the root unchanged'
	assert ($image_modified >= $root_modified) 'expected the VM image to be rebuilt'
	assert not ($snapshot_preserved | path exists) 'expected the stale VM snapshot to be replaced'
}
