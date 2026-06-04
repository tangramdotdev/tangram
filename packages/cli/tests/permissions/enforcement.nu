use ../../test.nu *

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

def assert_unauthorized [output: record, message: string] {
	failure $output $message
	assert ($output.stderr | str contains "unauthorized") "The error should mention that the request is unauthorized."
}

def anonymous_config [] {
	let path = mktemp
	{} | to json | save -f $path
	$path
}

def invalid_token_config [] {
	let path = mktemp
	{ token: invalid } | to json | save -f $path
	$path
}

let alice_user = tg user login alice | from json
let alice = current_token
let alice_user_id = $alice_user.id
let bob_user = tg user login bob | from json
let bob = current_token
let bob_user_id = $bob_user.id
let carol_user = tg user login carol | from json
let carol = current_token
let carol_user_id = $carol_user.id
let all_group = tg --token $alice group get all | from json
let all_group_id = $all_group.id

tg --token $alice group create project
tg --token $alice group get project
tg --token $alice grants list project
tg --token $alice group create project/pkg

tg --token $alice group create location-flags
tg --token $alice grants create $bob_user_id read location-flags
tg --token $alice grant $carol_user_id write location-flags
tg --token $alice grants list location-flags
tg --token $alice user grants bob
tg --token $alice user grants carol location-flags
tg --token $alice grants delete $bob_user_id read location-flags
tg --token $alice revoke $carol_user_id write location-flags

tg --token $alice group create nested/claimed
tg --token $alice group get nested
tg --token $alice group get nested/claimed

let output = tg --token $bob group create nested/bob | complete
assert_unauthorized $output "Bob should not be able to create a child group in Alice's auto-created group."

let output = tg --token $bob group create project/bob | complete
assert_unauthorized $output "Bob should not be able to create a child group without write permission on Alice's claimed group."

let config = anonymous_config
let output = with-env { TANGRAM_CONFIG: $config } { tg group create anonymous | complete }
assert_unauthorized $output "An anonymous user should not be able to claim a top-level group."

let config = anonymous_config
let output = with-env { TANGRAM_CONFIG: $config } { tg group create anonymous/nested | complete }
assert_unauthorized $output "An anonymous user should not be able to claim a nested group."

let path = artifact 'test'
let id = tg --token $alice checkin $path

tg --token $alice group create tag-location-flags
tg --token $alice tag tag-location-flags/pkg $id
tg --token $alice grants create $bob_user_id read tag-location-flags/pkg
tg --token $alice grant $carol_user_id write tag-location-flags/pkg
tg --token $alice grants list tag-location-flags/pkg
tg --token $alice grants delete $bob_user_id read tag-location-flags/pkg
tg --token $alice revoke $carol_user_id write tag-location-flags/pkg

let config = anonymous_config
let output = with-env { TANGRAM_CONFIG: $config } { tg group get project | complete }
assert_unauthorized $output "An anonymous user should not be able to get a private claimed group."

tg --token $alice group create public
tg --token $alice grants create $all_group_id read public
let config = anonymous_config
let output = with-env { TANGRAM_CONFIG: $config } { tg group get public | complete }
success $output "An anonymous user should be able to get a public group."

tg --token $alice group create tag-public
tg --token $alice tag --public tag-public/pkg $id
let config = anonymous_config
let output = with-env { TANGRAM_CONFIG: $config } { tg tag get tag-public/pkg | complete }
success $output "An anonymous user should be able to get a public tag."

tg --token $alice tag --public top-level $id
let config = anonymous_config
let output = with-env { TANGRAM_CONFIG: $config } { tg tag get top-level | complete }
success $output "An anonymous user should be able to get a top-level public tag."

let output = tg --token $alice group create top-level | complete
failure $output "A group should not be created with the same specifier as a tag."
assert ($output.stderr | str contains "already exists") "The error should mention that a node already exists with the specifier."

let output = tg --token $alice tag project $id | complete
failure $output "A tag should not be created with the same specifier as a group."
assert ($output.stderr | str contains "already exists") "The error should mention that a node already exists with the specifier."

tg --token $alice tag auto-created/pkg $id
tg --token $alice group get auto-created
tg --token $alice grants list auto-created

let output = tg --token $bob tag auto-created/sibling $id | complete
assert_unauthorized $output "Bob should not be able to put a tag in Alice's auto-created group."

let config = anonymous_config
let output = with-env { TANGRAM_CONFIG: $config } { tg tag anonymous/pkg $id | complete }
assert_unauthorized $output "An anonymous user should not be able to create a group by putting a tag."

tg --token $alice group create tag-private
tg --token $alice tag tag-private/pkg $id
tg --token $alice tag tag-private/sibling $id
let config = anonymous_config
let output = with-env { TANGRAM_CONFIG: $config } { tg tag get tag-private/pkg | complete }
failure $output "An anonymous user should not be able to get a private tag."
assert ($output.stderr | str contains "failed to find the tag") "The private tag should not be visible without an all read grant."

tg --token $alice grant $bob_user_id write tag-private/pkg
tg --token $bob tag get tag-private/pkg
tg --token $bob tag tag-private/pkg $id

let output = tg --token $bob tag tag-private/sibling $id | complete
assert_unauthorized $output "A tag grant should not grant write access to sibling tags."

let output = tg --token $bob grants list tag-private/pkg | complete
assert_unauthorized $output "A write tag grant should not allow Bob to inspect tag grants."

tg --token $alice grants create $bob_user_id admin tag-private/pkg
tg --token $bob grants create $carol_user_id read tag-private/pkg
tg --token $carol tag get tag-private/pkg
tg --token $bob revoke $carol_user_id read tag-private/pkg

let output = tg --token $bob grants create $carol_user_id read tag-private/sibling | complete
assert_unauthorized $output "A tag admin grant should not allow Bob to manage sibling tag grants."

tg --token $alice group create exact-public
tg --token $alice tag --public exact-public/pkg $id
tg --token $alice tag exact-public/sibling $id
let config = anonymous_config
let output = with-env { TANGRAM_CONFIG: $config } { tg list --no-groups --recursive exact-public | complete }
success $output "An anonymous user should be able to list exact public tags."
assert ($output.stdout | str contains "exact-public/pkg") "The public tag should be visible without a token."
assert (not ($output.stdout | str contains "exact-public/sibling")) "A public tag should not expose sibling tags."

tg --token $alice tag apple/secretproject/0 $id
tg --token $alice tag apple/macos/code/26 $id
tg --token $alice tag apple/macos/code/27 $id
tg --token $alice tag apple/macos/builds/26 $id
tg --token $alice tag apple/macos/builds/27 $id
tg --token $alice grants create $all_group_id read apple/macos/builds/26

let config = anonymous_config
let output = with-env { TANGRAM_CONFIG: $config } { tg ls apple | complete }
success $output "An anonymous user should be able to list a parent with a readable descendant."
assert ($output.stdout | str contains "apple/macos") "The readable descendant should expose the macos path segment."
assert (not ($output.stdout | str contains "apple/secretproject")) "A private sibling path segment should not be visible."

let output = with-env { TANGRAM_CONFIG: $config } { tg ls apple/macos | complete }
success $output "An anonymous user should be able to list an intermediate path with a readable descendant."
assert ($output.stdout | str contains "apple/macos/builds") "The readable descendant should expose the builds path segment."
assert (not ($output.stdout | str contains "apple/macos/code")) "A private sibling path segment should not be visible."

let output = with-env { TANGRAM_CONFIG: $config } { tg ls apple/macos/builds | complete }
success $output "An anonymous user should be able to list the readable release parent."
assert ($output.stdout | str contains "apple/macos/builds/26") "The released build should be visible."
assert (not ($output.stdout | str contains "apple/macos/builds/27")) "The unreleased build should not be visible."

tg --token $alice grants delete $all_group_id read apple/macos/builds/26
let output = with-env { TANGRAM_CONFIG: $config } { tg ls apple | complete }
success $output "An anonymous user should be able to list a private parent after the grant is revoked."
assert (not ($output.stdout | str contains "apple/macos")) "The group specifier should stop being visible without readable descendants."

tg --token $alice group create alice/project
tg --token $alice group get alice/project

let output = tg --token $bob group create alice/project/bob | complete
assert_unauthorized $output "Bob should not be able to create a child group without write permission."

let output = tg --token $bob tag put alice/project/pkg $id | complete
assert_unauthorized $output "Bob should not be able to put a tag without write permission."

tg --token $alice grant $bob_user_id write alice/project
tg --token $bob group get alice/project
let bob_artifact_id = tg --token $bob checkin $path
tg --token $bob tag put alice/project/pkg $bob_artifact_id

let output = tg --token $bob grants list alice/project | complete
assert_unauthorized $output "Write permission should not allow Bob to inspect group grants."

let bob_grants = tg --token $bob user grants bob | from json
assert (($bob_grants | length) > 0) "Bob should be able to inspect his own grants."

let output = tg --token $bob user grants carol alice/project | complete
assert_unauthorized $output "Write permission should not allow Bob to inspect Carol's grants."

let output = tg --token $carol group get alice/project | complete
assert_unauthorized $output "Carol should not be able to get a group without read permission."

let output = tg --token $carol tag get alice/project/pkg | complete
failure $output "Carol should not be able to get a tag without read permission."
assert ($output.stderr | str contains "failed to find the tag") "The tag should not be visible without read permission."

let output = tg --token invalid list --no-groups --recursive alice/project | complete
success $output "An invalid token should be treated as anonymous for readable entries."
assert (not ($output.stdout | str contains "alice/project/pkg")) "The private tag should not be visible with an invalid token."

let output = tg --token invalid grants list alice/project | complete
assert_unauthorized $output "An invalid token should not be able to inspect group grants."

let config = invalid_token_config
let output = with-env { TANGRAM_CONFIG: $config } { tg health | complete }
success $output "A bad token in the config should not prevent anonymous-capable requests."

let config = anonymous_config
let output = with-env { TANGRAM_CONFIG: $config } { tg list --no-groups --recursive alice/project | complete }
success $output "An anonymous user should be able to list readable entries."
assert (not ($output.stdout | str contains "alice/project/pkg")) "The private tag should not be visible without an all read grant."

tg --token $alice grants create $all_group_id read alice/project
let config = anonymous_config
let output = with-env { TANGRAM_CONFIG: $config } { tg list --no-groups --recursive alice/project | complete }
success $output "An anonymous user should be able to list all entries."
assert ($output.stdout | str contains "alice/project/pkg") "The all-readable tag should be visible without a token."

tg --token $alice revoke $all_group_id read alice/project
let config = anonymous_config
let output = with-env { TANGRAM_CONFIG: $config } { tg list --no-groups --recursive alice/project | complete }
success $output "An anonymous user should be able to list readable entries after revocation."
assert (not ($output.stdout | str contains "alice/project/pkg")) "The tag should stop being visible after the all read grant is revoked."

let output = tg --token $bob grants create $carol_user_id read alice/project | complete
assert_unauthorized $output "Write permission should not allow Bob to manage grants."

tg --token $alice grants create $carol_user_id read alice/project
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

tg --token $alice grants create $bob_user_id admin alice/project
tg --token $bob grants list alice/project
tg --token $bob user grants carol alice/project
tg --token $bob grants create $carol_user_id write alice/project
let carol_id = tg --token $carol checkin $path
tg --token $carol tag put alice/project/carol $carol_id
tg --token $carol tag delete alice/project/carol
