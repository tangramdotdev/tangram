use ../../test.nu *
use ../lib/permissions.nu *

# An anonymous user cannot get a private group or a tag that lives in a private group.

let server = spawn --config { authentication: true }
tg user login alice
let alice = current_token
let path = artifact 'test'
let id = tg --token $alice checkin $path

tg --token $alice group create project
let config = anonymous_config
let output = with-env { TANGRAM_CONFIG: $config } { tg group get project | complete }
assert_unauthorized $output "An anonymous user should not be able to get a private claimed group."

tg --token $alice group create tag-private
tg --token $alice tag tag-private/pkg $id
let config = anonymous_config
let output = with-env { TANGRAM_CONFIG: $config } { tg tag get tag-private/pkg | complete }
failure $output "An anonymous user should not be able to get a tag in a private group."
assert ($output.stderr | str contains "failed to find the tag") "The private tag should not be visible without a public read grant."
