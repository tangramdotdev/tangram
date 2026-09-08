use ../../test.nu *

# The descendant search must authorize public directory children when the ancestor search cannot reach the grant.

let server = server spawn --config {
	authentication: { users: { providers: { insecure: true } } }
	authorization: {
		initial: { ancestor: { max_edges: 0 } }
		final: { ancestor: { max_edges: 0 } }
	}
}

let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json

let directory = tg --token $alice.token put 'tg.directory({ "hello.txt": tg.file("hello") })' | str trim
tg --token $alice.token index
let child = tg --token $alice.token children $directory | from json | get 0
tg --token $alice.token grant public object_subtree $directory | ignore
tg --token $alice.token index

let output = tg --token $bob.token get $child | complete
success $output "bob should read a node inside a publicly granted directory"

let config = mktemp
{} | to json | save -f $config
let output = with-env { TANGRAM_CONFIG: $config } { tg get $child | complete }
success $output "an anonymous client should read a node inside a publicly granted directory"
