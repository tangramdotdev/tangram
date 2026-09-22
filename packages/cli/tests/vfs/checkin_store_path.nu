use ../lib/test.nu *
use ../lib/checkin.nu checkin-output
use ../lib/vfs.nu

# Checkin resolves a VFS store subpath without materializing its physical checkout.

vfs skip_unless_supported

let server_path = mktemp --directory
let server = server spawn --directory $server_path --config { vfs: true }
vfs assert_mounted $server_path
let directory = tg put 'tg.directory({ "bin": tg.directory({ "program": tg.file("contents") }) })' | str trim
let file = tg put 'tg.file("contents")' | str trim
for name in [$directory $'($directory).tg.ts'] {
	let path = vfs root $server_path $name | path join bin program
	let output = checkin-output $server $path
	assert equal $output.artifact $file
	let params = $'http://localhost/($output.reference)' | url parse | get params
	assert equal ($params | where key == id | first | get value) $directory
	assert equal ($params | where key == path | first | get value) 'bin/program'
	assert equal ($params | where key starts-with 'tokens[local]' | length) 2
}
assert (not ($server.checkout_directory | path join $directory bin | path exists))

# A module suffix is a store alias for the same artifact.
let path = vfs root $server_path $'($file).tg.ts'
let output = checkin-output $server $path
assert equal $output.artifact $file
let params = $'http://localhost/($output.reference)' | url parse | get params
assert ($params | where key in [id path] | is-empty)
assert equal ($params | where key starts-with 'tokens[local]' | length) 1
