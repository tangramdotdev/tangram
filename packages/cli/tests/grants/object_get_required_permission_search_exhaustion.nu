use ../lib/test.nu *

# Exhausting a required permission search must not report that an existing object is absent.
let local = server spawn --config {
	authentication: { users: { providers: { insecure: true } } }
	verification: {
		permissions: {
			initial: {
				ancestor: { max_edges: 0, max_nodes: 0 }
				descendant: { max_edges: 0, max_nodes: 0 }
			}
			final: {
				ancestor: { max_edges: 0, max_nodes: 0 }
				descendant: { max_edges: 0, max_nodes: 0 }
			}
		}
	}
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let object = tg --token $alice.token put 'tg.blob("existing object")' | str trim
tg --token $alice.token index
let output = tg --token $bob.token object get --bytes $object | complete
failure $output 'a required permission search without a proof should fail'
assert ($output.stderr | str contains 'authorization search exhausted') 'the failure should report exhaustion instead of absence'
server stop $local
