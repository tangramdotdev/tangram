use ../../test.nu *

# Getting an object with many parents must succeed when a grant above one of them proves it, because the ancestor search expands a page of parents before it reads the next page.

let searches = {
	ancestor: { max_edges: 3, page_size: 1 }
	descendant: { max_depth: 0, max_edges: 0, max_nodes: 0 }
}
let server = server spawn --config {
	authentication: { users: { providers: { insecure: true } } }
	authorization: { final: $searches, initial: $searches }
}

let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json

# Alice creates eight distinct directories that all contain the same file, all under one grandparent.
let entries = 0..<8 | each { |index|
	let n = $index | into string
	['"p' $n '": tg.directory({ "hub": tg.file("hub"), "pad": tg.file("' $n '") })'] | str join
} | str join ', '
let grandparent = tg --token $alice.token put (['tg.directory({ ' $entries ' })'] | str join) | str trim
let hub = tg --token $alice.token put 'tg.file("hub")' | str trim
tg --token $alice.token index
tg --token $alice.token grant $bob.user.id object_subtree $grandparent | ignore
tg --token $alice.token index

# Bob's grant is two hops above the file, so the ancestor search must climb rather than enumerate every parent.
let output = tg --token $bob.token get $hub | complete
success $output "Bob should read the file through the grant on its grandparent."
