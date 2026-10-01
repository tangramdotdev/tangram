use ../../lib/test.nu *

# Normalize artifact timestamps without following symlinks, including dangling links.
let local = server spawn --config { vfs: false }
let target = artifact 'external target'
let target_times = ls -lD $target | first | select accessed modified
let target_json = $target | to json --raw
let artifacts = [
	'tg.file("hello")'
	('tg.symlink({ "path": ' + $target_json + ' })')
	'tg.symlink({ "path": "missing" })'
	'tg.directory({ "file": "hello", "link": tg.symlink({ "path": "missing" }) })'
]

for artifact in $artifacts {
	let id = tg put $artifact | str trim
	let path = tg checkout $id | str trim
	let paths = if ($id | str starts-with 'dir_') {
		[$path, ($path | path join file), ($path | path join link)]
	} else {
		[$path]
	}
	for path in $paths {
		let metadata = ls -lD $path | first
		assert equal $metadata.accessed 1970-01-01T00:00:00Z
		assert equal $metadata.modified 1970-01-01T00:00:00Z
	}
}

assert equal (ls -lD $target | first | select accessed modified) $target_times
