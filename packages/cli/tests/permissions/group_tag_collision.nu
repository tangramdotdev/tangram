use ../../test.nu *
use ../lib/permissions.nu *

# A group cannot be created where a tag exists, and a tag cannot be created where a group exists.

let server = spawn --config { authentication: true }
tg user login alice
let alice = current_token
let path = artifact 'test'
let id = tg --token $alice checkin $path

tg --token $alice tag --public top-level $id
let output = tg --token $alice group create top-level | complete
failure $output "A group should not be created with the same specifier as a tag."
assert ($output.stderr | str contains "already exists") "The error should mention that a node already exists with the specifier."

tg --token $alice group create project
let output = tg --token $alice tag project $id | complete
failure $output "A tag should not be created with the same specifier as a group."
assert ($output.stderr | str contains "already exists") "The error should mention that a node already exists with the specifier."
