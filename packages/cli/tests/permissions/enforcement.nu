use ../../test.nu *

let server = spawn --config { authorization: true }

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

tg --token $alice namespace grant alice/project write --user bob
tg --token $bob namespace get alice/project
tg --token $bob tag put alice/project/pkg $id

let output = tg --token $carol namespace get alice/project | complete
assert_forbidden $output "Carol should not be able to get a namespace without read permission."

let output = tg --token $carol tag get alice/project/pkg | complete
failure $output "Carol should not be able to get a tag without read permission."
assert ($output.stderr | str contains "no tag was found") "The tag should not be visible without read permission."

let output = tg --token invalid list --no-namespaces --recursive alice/project | complete
failure $output "An invalid token should not be able to list private entries."
assert ($output.stderr | str contains "failed to authorize") "The error should mention authorization."

let config = anonymous_config
let output = with-env { TANGRAM_CONFIG: $config } { tg list --no-namespaces --recursive alice/project | complete }
success $output "An anonymous user should be able to list readable entries."
assert (not ($output.stdout | str contains "alice/project/pkg")) "The private tag should not be visible without public read."

tg --token $alice namespace grant alice/project read --public
let config = anonymous_config
let output = with-env { TANGRAM_CONFIG: $config } { tg list --no-namespaces --recursive alice/project | complete }
success $output "An anonymous user should be able to list public entries."
assert ($output.stdout | str contains "alice/project/pkg") "The public tag should be visible without a token."

tg --token $alice namespace revoke alice/project read --public
let config = anonymous_config
let output = with-env { TANGRAM_CONFIG: $config } { tg list --no-namespaces --recursive alice/project | complete }
success $output "An anonymous user should be able to list readable entries after revocation."
assert (not ($output.stdout | str contains "alice/project/pkg")) "The tag should stop being visible after public read is revoked."

let output = tg --token $bob namespace grant alice/project read --user carol | complete
assert_forbidden $output "Write permission should not allow Bob to manage grants."

tg --token $alice namespace grant alice/project read --user carol
tg --token $carol namespace get alice/project
tg --token $carol tag get alice/project/pkg

let output = tg --token $carol tag put alice/project/carol $id | complete
assert_forbidden $output "Read permission should not allow Carol to put a tag."

let output = tg --token $carol tag delete alice/project/pkg | complete
assert_forbidden $output "Read permission should not allow Carol to delete a tag."

tg --token $alice namespace grant alice/project admin --user bob
tg --token $bob namespace grant alice/project write --user carol
tg --token $carol tag put alice/project/carol $id
tg --token $carol tag delete alice/project/carol
