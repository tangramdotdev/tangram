use ../../test.nu *
use ../lib/permissions.nu *

# An anonymous user can get a public group and public tags, whether the tag is nested under a group or at the top level.

let server = spawn --config { authentication: true }
tg user login alice
let alice = current_token
let path = artifact 'test'
let id = tg --token $alice checkin $path

tg --token $alice grant public read public
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
