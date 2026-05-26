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

tg user login alice
let alice = current_token
tg user login bob
let bob = current_token
tg user login carol
let carol = current_token

tg --token $alice namespace create project
tg --token $alice namespace get project
tg --token $alice namespace grants list project
tg --token $alice namespace grants list project --local
tg --token $alice namespace create project/pkg

tg --token $alice namespace create location-flags
tg --token $alice namespace grants add location-flags --local --read --user bob
tg --token $alice grant --local --namespace location-flags --write --user carol
tg --token $alice namespace grants list location-flags --local
tg --token $alice user grants bob --local
tg --token $alice user grants carol location-flags --local
tg --token $alice namespace grants delete location-flags --local --read --user bob
tg --token $alice revoke --local --namespace location-flags --write --user carol

tg --token $alice namespace create nested/claimed
tg --token $alice namespace get nested
tg --token $alice namespace get nested/claimed

let output = tg --token $bob namespace create nested/bob | complete
assert_unauthorized $output "Bob should not be able to create a child namespace in Alice's auto-created namespace."

let output = tg --token $bob namespace create project/bob | complete
assert_unauthorized $output "Bob should not be able to create a child namespace without write permission on Alice's claimed namespace."

let config = anonymous_config
let output = with-env { TANGRAM_CONFIG: $config } { tg namespace create anonymous | complete }
assert_unauthorized $output "An anonymous user should not be able to claim a top-level namespace."

let config = anonymous_config
let output = with-env { TANGRAM_CONFIG: $config } { tg namespace create anonymous/nested | complete }
assert_unauthorized $output "An anonymous user should not be able to claim a nested namespace."

let path = artifact 'test'
let id = tg --token $alice checkin $path

tg --token $alice namespace create tag-location-flags
tg --token $alice tag tag-location-flags/pkg $id
tg --token $alice tag grants add tag-location-flags/pkg --local --read --user bob
tg --token $alice grant --local --tag tag-location-flags/pkg --write --user carol
tg --token $alice tag grants list tag-location-flags/pkg --local
tg --token $alice tag grants delete tag-location-flags/pkg --local --read --user bob
tg --token $alice revoke --local --tag tag-location-flags/pkg --write --user carol

let config = anonymous_config
let output = with-env { TANGRAM_CONFIG: $config } { tg namespace get project | complete }
assert_unauthorized $output "An anonymous user should not be able to get a private claimed namespace."

tg --token $alice namespace create --public public
let config = anonymous_config
let output = with-env { TANGRAM_CONFIG: $config } { tg namespace get public | complete }
success $output "An anonymous user should be able to get a public namespace."

tg --token $alice namespace create tag-public
tg --token $alice tag --public tag-public/pkg $id
let config = anonymous_config
let output = with-env { TANGRAM_CONFIG: $config } { tg tag get tag-public/pkg | complete }
success $output "An anonymous user should be able to get a public tag."

tg --token $alice tag --public top-level $id
let config = anonymous_config
let output = with-env { TANGRAM_CONFIG: $config } { tg tag get top-level | complete }
success $output "An anonymous user should be able to get a top-level public tag."

let output = tg --token $alice namespace create top-level | complete
failure $output "A namespace should not be created at the same path as a tag."
assert ($output.stderr | str contains "a tag exists at the namespace path") "The error should mention that a tag exists at the namespace path."

let output = tg --token $alice tag project $id | complete
failure $output "A tag should not be created at the same path as a namespace."
assert ($output.stderr | str contains "a namespace exists at the tag path") "The error should mention that a namespace exists at the tag path."

tg --token $alice tag auto-created/pkg $id
tg --token $alice namespace get auto-created
tg --token $alice namespace grants list auto-created

let output = tg --token $bob tag auto-created/sibling $id | complete
assert_unauthorized $output "Bob should not be able to put a tag in Alice's auto-created namespace."

let config = anonymous_config
let output = with-env { TANGRAM_CONFIG: $config } { tg tag anonymous/pkg $id | complete }
assert_unauthorized $output "An anonymous user should not be able to create a namespace by putting a tag."

tg --token $alice namespace create tag-private
tg --token $alice tag tag-private/pkg $id
tg --token $alice tag tag-private/sibling $id
let config = anonymous_config
let output = with-env { TANGRAM_CONFIG: $config } { tg tag get tag-private/pkg | complete }
failure $output "An anonymous user should not be able to get a tag in a private namespace."
assert ($output.stderr | str contains "no tag was found") "The private tag should not be visible without all read."

tg --token $alice grant --tag tag-private/pkg --write --user bob
tg --token $bob tag get tag-private/pkg
tg --token $bob tag tag-private/pkg $id

let output = tg --token $bob tag tag-private/sibling $id | complete
assert_unauthorized $output "A tag grant should not grant write access to sibling tags."

let output = tg --token $bob tag grants list tag-private/pkg | complete
assert_unauthorized $output "A write tag grant should not allow Bob to inspect tag grants."

tg --token $alice tag grants add tag-private/pkg --admin --user bob
tg --token $bob tag grants add tag-private/pkg --read --user carol
tg --token $carol tag get tag-private/pkg
tg --token $bob revoke --tag tag-private/pkg --read --user carol

let output = tg --token $bob tag grants add tag-private/sibling --read --user carol | complete
assert_unauthorized $output "A tag admin grant should not allow Bob to manage sibling tag grants."

tg --token $alice namespace create exact-public
tg --token $alice tag --public exact-public/pkg $id
tg --token $alice tag exact-public/sibling $id
let config = anonymous_config
let output = with-env { TANGRAM_CONFIG: $config } { tg list --no-namespaces --recursive exact-public | complete }
success $output "An anonymous user should be able to list exact public tags."
assert ($output.stdout | str contains "exact-public/pkg") "The public tag should be visible without a token."
assert (not ($output.stdout | str contains "exact-public/sibling")) "A public tag should not expose sibling tags."

tg --token $alice tag apple/secretproject/0 $id
tg --token $alice tag apple/macos/code/26 $id
tg --token $alice tag apple/macos/code/27 $id
tg --token $alice tag apple/macos/builds/26 $id
tg --token $alice tag apple/macos/builds/27 $id
tg --token $alice tag grants add apple/macos/builds/26 --read --all

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

tg --token $alice tag grants delete apple/macos/builds/26 --read --all
let output = with-env { TANGRAM_CONFIG: $config } { tg ls apple | complete }
success $output "An anonymous user should be able to list a private parent after the grant is revoked."
assert (not ($output.stdout | str contains "apple/macos")) "The namespace should stop being visible without readable descendants."

tg --token $alice namespace create alice/project
tg --token $alice namespace get alice/project

let output = tg --token $bob namespace create alice/project/bob | complete
assert_unauthorized $output "Bob should not be able to create a child namespace without write permission."

let output = tg --token $bob tag put alice/project/pkg $id | complete
assert_unauthorized $output "Bob should not be able to put a tag without write permission."

tg --token $alice grant --namespace alice/project --write --user bob
tg --token $bob namespace get alice/project
let bob_id = tg --token $bob checkin $path
tg --token $bob tag put alice/project/pkg $bob_id

let output = tg --token $bob namespace grants list alice/project | complete
assert_unauthorized $output "Write permission should not allow Bob to inspect namespace grants."

let bob_grants = tg --token $bob user grants bob | from json
assert (($bob_grants | length) > 0) "Bob should be able to inspect his own grants."

let output = tg --token $bob user grants carol alice/project | complete
assert_unauthorized $output "Write permission should not allow Bob to inspect Carol's grants."

let output = tg --token $carol namespace get alice/project | complete
assert_unauthorized $output "Carol should not be able to get a namespace without read permission."

let output = tg --token $carol tag get alice/project/pkg | complete
failure $output "Carol should not be able to get a tag without read permission."
assert ($output.stderr | str contains "no tag was found") "The tag should not be visible without read permission."

let output = tg --token invalid list --no-namespaces --recursive alice/project | complete
success $output "An invalid token should be treated as anonymous for readable entries."
assert (not ($output.stdout | str contains "alice/project/pkg")) "The private tag should not be visible with an invalid token."

let output = tg --token invalid namespace grants list alice/project | complete
assert_unauthorized $output "An invalid token should not be able to inspect namespace grants."

let config = invalid_token_config
let output = with-env { TANGRAM_CONFIG: $config } { tg health | complete }
success $output "A bad token in the config should not prevent anonymous-capable requests."

let config = anonymous_config
let output = with-env { TANGRAM_CONFIG: $config } { tg list --no-namespaces --recursive alice/project | complete }
success $output "An anonymous user should be able to list readable entries."
assert (not ($output.stdout | str contains "alice/project/pkg")) "The private tag should not be visible without all read."

tg --token $alice namespace grants add alice/project --read --all
let config = anonymous_config
let output = with-env { TANGRAM_CONFIG: $config } { tg list --no-namespaces --recursive alice/project | complete }
success $output "An anonymous user should be able to list all entries."
assert ($output.stdout | str contains "alice/project/pkg") "The all tag should be visible without a token."

tg --token $alice revoke --namespace alice/project --read --all
let config = anonymous_config
let output = with-env { TANGRAM_CONFIG: $config } { tg list --no-namespaces --recursive alice/project | complete }
success $output "An anonymous user should be able to list readable entries after revocation."
assert (not ($output.stdout | str contains "alice/project/pkg")) "The tag should stop being visible after all read is revoked."

let output = tg --token $bob namespace grants add alice/project --read --user carol | complete
assert_unauthorized $output "Write permission should not allow Bob to manage grants."

tg --token $alice namespace grants add alice/project --read --user carol
tg --token $carol namespace get alice/project
tg --token $carol tag get alice/project/pkg

let output = tg --token $carol namespace grants list alice/project | complete
assert_unauthorized $output "Read permission should not allow Carol to inspect namespace grants."

let carol_grants = tg --token $carol user grants carol | from json
assert (($carol_grants | length) > 0) "Carol should be able to inspect her own grants."

let bob_grants = tg --token $carol user grants bob | from json
assert equal ($bob_grants | length) 0 "Carol should not see Bob's grants without admin permission."

let output = tg --token $carol tag put alice/project/carol $id | complete
assert_unauthorized $output "Read permission should not allow Carol to put a tag."

let output = tg --token $carol tag delete alice/project/pkg | complete
assert_unauthorized $output "Read permission should not allow Carol to delete a tag."

tg --token $alice namespace grants add alice/project --admin --user bob
tg --token $bob namespace grants list alice/project
tg --token $bob user grants carol alice/project
tg --token $bob namespace grants add alice/project --write --user carol
let carol_id = tg --token $carol checkin $path
tg --token $carol tag put alice/project/carol $carol_id
tg --token $carol tag delete alice/project/carol
