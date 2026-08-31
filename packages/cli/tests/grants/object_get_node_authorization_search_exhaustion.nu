use ../../test.nu *

# Getting an object succeeds when its required node permission is authorized but its optional subtree permission exhausts the authorization search.

let server = server spawn --config {
	authentication: { users: { providers: { insecure: true } } }
	authorization: {
		final: {
			descendant: { max_depth: 0, max_edges: 0, max_nodes: 0 }
			subtree: { max_objects: 0 }
		}
	}
}

let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json

let directory = tg --token $alice.token put 'tg.directory({ "child": tg.directory({}) })' | str trim
tg --token $alice.token index
tg --token $alice.token grant $bob.user.id object_node $directory | ignore
tg --token $alice.token index

let output = tg --token $bob.token object get --availability --bytes --metadata $directory | complete
success $output "Bob should read the directory node even when its optional subtree authorization is indeterminate."

let metadata = tg --token $bob.token metadata $directory | complete
success $metadata "Bob should read the directory metadata even when its optional subtree authorization is indeterminate."

let availability = tg --token $bob.token availability $directory | complete
success $availability "Bob should read the directory availability even when its optional subtree authorization is indeterminate."
