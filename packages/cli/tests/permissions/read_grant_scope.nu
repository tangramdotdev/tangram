use ../../test.nu *
use ../lib/permissions.nu *

# A group read grant allows getting the group and its tags, but does not allow putting or deleting tags, inspecting grants, or viewing another user's grants.

let server = spawn --config { authentication: true }
let users = login_users
let alice = $users.alice
let carol = $users.carol
let path = artifact 'test'
let id = tg --token $alice checkin $path

tg --token $alice group create alice/project
tg --token $alice tag put alice/project/pkg $id

let output = tg --token $carol group get alice/project | complete
assert_unauthorized $output "Carol should not be able to get a group without read permission."

let output = tg --token $carol tag get alice/project/pkg | complete
failure $output "Carol should not be able to get a tag without read permission."
assert ($output.stderr | str contains "failed to find the tag") "The tag should not be visible without read permission."

tg --token $alice grant $users.carol_id read alice/project
tg --token $carol group get alice/project
tg --token $carol tag get alice/project/pkg

let output = tg --token $carol grants list alice/project | complete
assert_unauthorized $output "Read permission should not allow Carol to inspect group grants."

let carol_grants = tg --token $carol user grants carol | from json
assert (($carol_grants | length) > 0) "Carol should be able to inspect her own grants."

let bob_grants = tg --token $carol user grants bob | from json
assert equal ($bob_grants | length) 0 "Carol should not see Bob's grants without admin permission."

let output = tg --token $carol tag put alice/project/carol $id | complete
assert_unauthorized $output "Read permission should not allow Carol to put a tag."

let output = tg --token $carol tag delete alice/project/pkg | complete
assert_unauthorized $output "Read permission should not allow Carol to delete a tag."
