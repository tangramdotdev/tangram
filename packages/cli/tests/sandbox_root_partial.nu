use ../test.nu *

# A server rebuilds a partial container root so that sandboxed processes can start.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let directory = mktemp --directory
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
