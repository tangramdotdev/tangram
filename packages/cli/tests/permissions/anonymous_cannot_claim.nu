use ../../test.nu *
use ../lib/permissions.nu *

# An anonymous user cannot claim a group, whether top-level, nested, or implicitly by putting a tag.

let server = spawn --config { authentication: true }
tg user login alice
let alice = current_token
let path = artifact 'test'
let id = tg --token $alice checkin $path

let config = anonymous_config
let output = with-env { TANGRAM_CONFIG: $config } { tg group create anonymous | complete }
assert_unauthorized $output "An anonymous user should not be able to claim a top-level group."

let config = anonymous_config
let output = with-env { TANGRAM_CONFIG: $config } { tg group create anonymous/nested | complete }
assert_unauthorized $output "An anonymous user should not be able to claim a nested group."

let config = anonymous_config
let output = with-env { TANGRAM_CONFIG: $config } { tg tag anonymous/pkg $id | complete }
assert_unauthorized $output "An anonymous user should not be able to create a group by putting a tag."
