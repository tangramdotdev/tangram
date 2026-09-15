use ../../test.nu *
use ../lib/checkin.nu checkin-output

# A store subpath backed by a graph pointer returns a stored artifact with its own usable token.

let server = server spawn --config {
	authentication: { users: { providers: { insecure: true } } }
	remotes: {}
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let module = artifact {
	tangram.ts: '
		export default async () => {
			const graph = await tg.graph({ nodes: [
				{ kind: "directory", entries: { program: 1 } },
				{ kind: "file", contents: "contents", dependencies: { self: 1 } },
			] });
			return tg.directory({ graph, index: 0, kind: "directory" });
		};
	'
}
let directory = tg --token $alice.token build $module | str trim
let directory_id = $directory | split row "?" | first
tg --token $alice.token index
let root = tg --token $alice.token checkout $directory | str trim
let path = $root | path join program
let output = checkin-output $server $path --token $alice.token
let params = $'http://localhost/($output.reference)' | url parse | get params
assert equal ($params | where key == id | first | get value) $directory_id
assert equal ($params | where key == path | first | get value) program
assert equal ($params | where key starts-with 'tokens[local]' | length) 2
let contents = tg --token $bob.token read $output.reference
assert equal $contents contents
