use ../../test.nu *
use ../lib/permissions.nu *

# A grant on a specific tag applies only to that tag: a write grant allows getting and putting the tag but not its siblings or inspecting grants, and an admin grant allows managing the tag's grants but not its siblings'.

let server = spawn --config { authentication: true }
let users = login_users
let alice = $users.alice
let bob = $users.bob
let carol = $users.carol
let path = artifact 'test'
let id = tg --token $alice checkin $path

tg --token $alice group create tag-private
tg --token $alice tag tag-private/pkg $id
tg --token $alice tag tag-private/sibling $id

tg --token $alice grant $users.bob_id write tag-private/pkg
tg --token $bob tag get tag-private/pkg
tg --token $bob tag tag-private/pkg $id

let output = tg --token $bob tag tag-private/sibling $id | complete
assert_unauthorized $output "A tag grant should not grant write access to sibling tags."

let output = tg --token $bob grants list tag-private/pkg | complete
assert_unauthorized $output "A write tag grant should not allow Bob to inspect tag grants."

tg --token $alice grant $users.bob_id admin tag-private/pkg
tg --token $bob grant $users.carol_id read tag-private/pkg
tg --token $carol tag get tag-private/pkg
tg --token $bob revoke $users.carol_id read tag-private/pkg

let output = tg --token $bob grant $users.carol_id read tag-private/sibling | complete
assert_unauthorized $output "A tag admin grant should not allow Bob to manage sibling tag grants."
