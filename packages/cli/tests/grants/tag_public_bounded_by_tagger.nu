use ../../test.nu *

# Publishing is bounded by the tagger's access: a user with only node access to a directory who tags it --public confers the public that node only, so the directory's private child stays masked.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json
let eve = tg login --verbose eve | from json

# Alice creates a directory with a private child file.
let dir = tg --token $alice.token put 'tg.directory({ "secret.txt": tg.file("childsecret") })' | str trim
let child = tg --token $alice.token children $dir | from json | get 0

# Alice grants Bob only node access to the directory, not its subtree.
tg --token $alice.token grant $bob.user.id object_node $dir | ignore

# Bob reads the directory node but not its child, since node does not confer children.
let bob_dir = tg --token $bob.token get $dir | complete
success $bob_dir "Bob should read the directory node he was granted."
let bob_child = tg --token $bob.token get $child | complete
failure $bob_child "Bob should not read the child the node grant does not confer."

# Bob publishes the directory with a public tag; the recorded permission is bounded by his node-only access.
tg --token $bob.token tag put bobtag $dir --public
tg --token $bob.token index

# Eve, a third party, resolves the public tag and reads the published directory node.
let eve_tag = tg --token $eve.token tag get bobtag | complete
success $eve_tag "Eve should resolve the public tag."
let eve_dir = tg --token $eve.token get $dir | complete
success $eve_dir "Eve should read the published directory node."

# But Eve must not read the child: Bob could not publish a subtree he could not read.
let eve_child = tg --token $eve.token get $child | complete
failure $eve_child "a public tag must not confer more than the tagger could read; the private child stays masked."
