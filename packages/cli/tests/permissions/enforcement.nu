use ../../test.nu *

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

def assert_forbidden [output: record, message: string] {
	failure $output $message
	assert ($output.stderr | str contains "forbidden") "The error should mention that the request is forbidden."
}

def anonymous_config [] {
	let path = mktemp
	{} | to json | save -f $path
	$path
}

tg user login alice@example.com --handle alice
let alice = current_token
tg user login bob@example.com --handle bob
let bob = current_token
tg user login carol@example.com --handle carol
let carol = current_token

let path = artifact 'test'
let id = tg --token $alice checkin $path

tg --token $alice namespace create alice/project
tg --token $alice namespace get alice/project

let output = tg --token $bob namespace create alice/project/bob | complete
assert_forbidden $output "Bob should not be able to create a child namespace without write permission."

let output = tg --token $bob tag put alice/project/pkg $id | complete
assert_forbidden $output "Bob should not be able to put a tag without write permission."

tg --token $alice namespace grants add alice/project write --user bob
tg --token $bob namespace get alice/project
tg --token $bob tag put alice/project/pkg $id

let output = tg --token $bob namespace grants list alice/project | complete
assert_forbidden $output "Write permission should not allow Bob to inspect namespace grants."

let bob_grants = tg --token $bob user grants bob | from json
assert (($bob_grants | length) > 0) "Bob should be able to inspect his own grants."

let output = tg --token $bob user permissions carol alice/project | complete
assert_forbidden $output "Write permission should not allow Bob to inspect Carol's permissions."

let output = tg --token $carol namespace get alice/project | complete
assert_forbidden $output "Carol should not be able to get a namespace without read permission."

let output = tg --token $carol tag get alice/project/pkg | complete
failure $output "Carol should not be able to get a tag without read permission."
assert ($output.stderr | str contains "no tag was found") "The tag should not be visible without read permission."

let output = tg --token invalid list --no-namespaces --recursive alice/project | complete
failure $output "An invalid token should not be able to list private entries."
assert ($output.stderr | str contains "failed to authorize") "The error should mention authentication."

let config = anonymous_config
let output = with-env { TANGRAM_CONFIG: $config } { tg list --no-namespaces --recursive alice/project | complete }
success $output "An anonymous user should be able to list readable entries."
assert (not ($output.stdout | str contains "alice/project/pkg")) "The private tag should not be visible without public read."

tg --token $alice namespace grants add alice/project read --public
let config = anonymous_config
let output = with-env { TANGRAM_CONFIG: $config } { tg list --no-namespaces --recursive alice/project | complete }
success $output "An anonymous user should be able to list public entries."
assert ($output.stdout | str contains "alice/project/pkg") "The public tag should be visible without a token."

tg --token $alice namespace grants delete alice/project read --public
let config = anonymous_config
let output = with-env { TANGRAM_CONFIG: $config } { tg list --no-namespaces --recursive alice/project | complete }
success $output "An anonymous user should be able to list readable entries after revocation."
assert (not ($output.stdout | str contains "alice/project/pkg")) "The tag should stop being visible after public read is revoked."

let output = tg --token $bob namespace grants add alice/project read --user carol | complete
assert_forbidden $output "Write permission should not allow Bob to manage grants."

tg --token $alice namespace grants add alice/project read --user carol
tg --token $carol namespace get alice/project
tg --token $carol tag get alice/project/pkg

let output = tg --token $carol namespace grants list alice/project | complete
assert_forbidden $output "Read permission should not allow Carol to inspect namespace grants."

let carol_grants = tg --token $carol user grants carol | from json
assert (($carol_grants | length) > 0) "Carol should be able to inspect her own grants."

let bob_grants = tg --token $carol user grants bob | from json
assert equal ($bob_grants | length) 0 "Carol should not see Bob's grants without admin permission."

let output = tg --token $carol tag put alice/project/carol $id | complete
assert_forbidden $output "Read permission should not allow Carol to put a tag."

let output = tg --token $carol tag delete alice/project/pkg | complete
assert_forbidden $output "Read permission should not allow Carol to delete a tag."

tg --token $alice namespace grants add alice/project admin --user bob
tg --token $bob namespace grants list alice/project
tg --token $bob user permissions carol alice/project
tg --token $bob namespace grants add alice/project write --user carol
tg --token $carol tag put alice/project/carol $id
tg --token $carol tag delete alice/project/carol
