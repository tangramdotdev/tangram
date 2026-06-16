use ../../test.nu *

# A lazy push can rely on objects the remote has already stored only when the pushing principal is authorized for them.

let remote = spawn --cloud --name remote --config { authentication: true }

let alice = tg --url $remote.url login --verbose alice | from json
let bob = tg --url $remote.url login --verbose bob | from json

let alice_local = spawn --name alice-local --config {
	remotes: { default: { url: $remote.url, token: $alice.token } },
}
let bob_local = spawn --name bob-local --config {
	remotes: { default: { url: $remote.url, token: $bob.token } },
}

let directory = tg --url $alice_local.url put 'tg.directory({ "hello.txt": tg.file("hello") })' | str trim
tg --url $alice_local.url index
let file = tg --url $alice_local.url children $directory | from json | get 0
let blob = tg --url $alice_local.url children $file | from json | get 0

let output = tg --url $alice_local.url --no-quiet push --lazy $file | complete
success $output "Alice should be able to push the file and blob."
assert ($output.stderr | str contains "transferred 0 processes, 2 objects") "Alice should transfer the file and blob."

tg --url $remote.url index

tg --url $alice_local.url get --bytes $directory | tg --url $bob_local.url put --bytes --kind dir

let output = tg --url $bob_local.url --no-quiet push --lazy $directory | complete
failure $output "Bob should not be able to lazy-push a directory that depends on Alice's private file."

tg --url $remote.url --token $alice.token grant $bob.user.id object_subtree $file | ignore

let output = tg --url $bob_local.url --no-quiet push --lazy $directory | complete
success $output "Bob should be able to lazy-push the directory after Alice grants access to the file subtree."
assert ($output.stderr | str contains "skipped 0 processes, 2 objects") "Bob should skip the file and blob after Alice's grant."
assert ($output.stderr | str contains "transferred 0 processes, 1 objects") "Bob should only transfer the directory."

tg --url $remote.url index

let remote_object = tg --url $remote.url --token $bob.token get $directory --blobs --depth=inf --pretty
let local_object = tg --url $alice_local.url get $directory --blobs --depth=inf --pretty
assert equal $remote_object $local_object "The remote directory should resolve through the file and blob."

let output = tg --url $bob_local.url --no-quiet push --lazy $directory | complete
success $output "Bob should be able to push the directory again."
assert ($output.stderr | str contains "skipped 0 processes, 3 objects") "The second push should touch and skip the visible directory subtree through Bob's sync grant."
assert ($output.stderr | str contains "transferred 0 processes, 0 objects") "The second push should not transfer any objects."

let public_source = spawn --name public-source --config {
	remotes: { default: { url: $remote.url } },
}
let public_directory_source = spawn --name public-directory-source --config {
	remotes: { default: { url: $remote.url } },
}

let public_directory = tg --url $public_source.url put 'tg.directory({ "public.txt": tg.file("public") })' | str trim
tg --url $public_source.url index
let public_file = tg --url $public_source.url children $public_directory | from json | get 0

let output = tg --url $public_source.url --no-quiet push --lazy $public_file | complete
success $output "An anonymous lazy push should succeed."
assert ($output.stderr | str contains "transferred 0 processes, 2 objects") "The anonymous push should transfer the public file and blob."

tg --url $remote.url index

tg --url $public_source.url get --bytes $public_directory | tg --url $public_directory_source.url put --bytes --kind dir

let output = tg --url $public_directory_source.url --no-quiet push --lazy $public_directory | complete
success $output "A later anonymous lazy push should be able to rely on the public file subtree."
assert ($output.stderr | str contains "skipped 0 processes, 2 objects") "The later anonymous push should skip the public file and blob."
assert ($output.stderr | str contains "transferred 0 processes, 1 objects") "The later anonymous push should only transfer the directory."
