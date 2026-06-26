use ../../test.nu *

# A successful lazy push mints a sync grant, so the same principal's next push skips its whole visible subtree and transfers nothing.

let remote = spawn --cloud --name remote --config { authentication: { providers: { insecure: true } } }

let alice = tg --url $remote.url login --verbose alice | from json
let bob = tg --url $remote.url login --verbose bob | from json

let alice_local = spawn --name alice-local --config {
	remotes: { default: { url: $remote.url, token: $alice.token } },
}
let bob_local = spawn --name bob-local --config {
	remotes: { default: { url: $remote.url, token: $bob.token } },
}

# Alice stores a private file and blob on the remote and grants Bob the file subtree.
let directory = tg --url $alice_local.url put 'tg.directory({ "hello.txt": tg.file("hello") })' | str trim
tg --url $alice_local.url index
let file = tg --url $alice_local.url children $directory | from json | get 0
tg --url $alice_local.url push --lazy $file
tg --url $remote.url index
tg --url $alice_local.url get --bytes $directory | tg --url $bob_local.url put --bytes --kind dir
tg --url $remote.url --token $alice.token grant $bob.user.id object_subtree $file | ignore

# Bob's first push transfers the directory and mints a sync grant over the subtree he relied on.
tg --url $bob_local.url push --lazy $directory
tg --url $remote.url index

# Bob's second push skips the whole subtree through his sync grant and transfers nothing.
let output = tg --url $bob_local.url --no-quiet push --lazy $directory | complete
success $output "Bob should push the directory again."
snapshot ($output.stderr | lines | where {|l| $l =~ '(transferred|skipped) \d+ processes'} | sort | str join "\n") '
	info skipped 0 processes, 3 objects, 109 B
	info transferred 0 processes, 0 objects, 0 B
'
