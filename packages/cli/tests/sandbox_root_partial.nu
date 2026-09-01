use ../test.nu *

# A server atomically rebuilds incomplete and out-of-date container roots.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let directory = mktemp --directory
let container = $directory | path join 'container'
let root = $directory | path join 'container' 'root'
mkdir $root
'partial' | save ($root | path join 'partial')

let server = server spawn --directory $directory

let path = artifact {
	tangram.ts: '
		export default () => {
			console.log("Hello, World!");
		};
	'
}

let output = tg run --sandbox $path
assert equal $output 'Hello, World!'

# A failed rebuild leaves the installed root untouched.
server stop $server
let version = $root | path join '.tangram-version'
open --raw $version | from json | update tangram 'out of date' | to json | save --force $version
let preserved = $root | path join 'preserved'
touch $preserved
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
let output = tg run --sandbox $path
assert equal $output 'Hello, World!'
