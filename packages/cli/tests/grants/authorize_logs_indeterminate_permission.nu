use ../../test.nu *

# A requested permission whose search exhausts is reported as absent, so the authorization engine must record that it is indeterminate rather than denied.

let server = server spawn --config {
	authentication: { users: { providers: { insecure: true } } }
	authorization: {
		final: {
			descendant: { max_depth: 0, max_edges: 0, max_nodes: 0 }
			subtree: { max_objects: 0 }
		}
	}
	tracing: {
		filter: 'tangram=info,tangram_index::authorize=debug'
		stderr_format: 'json'
	}
}

let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json

let directory = tg --token $alice.token put 'tg.directory({ "child": tg.directory({}) })' | str trim
tg --token $alice.token index
tg --token $alice.token grant $bob.user.id object_node $directory | ignore
tg --token $alice.token index

# Bob's read succeeds with only the node permission, and the subtree permission it drops is indeterminate.
let output = tg --token $bob.token object get --availability --bytes --metadata $directory | complete
success $output "Bob should read the directory node."

let events = open $server.log
	| lines
	| where ($it | str starts-with '{')
	| each { from json }
	| where $it.fields.message? == 'authorize permission indeterminate'
	| where $it.fields.resource? == $directory
assert (($events | length) > 0) 'expected the engine to record an indeterminate permission'
assert equal ($events | last | get fields.authorized) 'object_node' 'expected only the node permission to be authorized'
assert equal ($events | last | get fields.indeterminate) 'object_subtree' 'expected the subtree permission to be indeterminate'
