use ../../test.nu *
use ../lib/permissions.nu *

# Granting public read on a group makes its private tags visible to anonymous listing, and revoking it hides them again.

let server = spawn --config { authentication: true }
tg user login alice
let alice = current_token
let path = artifact 'test'
let id = tg --token $alice checkin $path

tg --token $alice group create alice/project
tg --token $alice tag put alice/project/pkg $id

let config = anonymous_config
let output = with-env { TANGRAM_CONFIG: $config } { tg list --no-groups --recursive alice/project | complete }
success $output "An anonymous user should be able to list readable entries."
assert (not ($output.stdout | str contains "alice/project/pkg")) "The private tag should not be visible without a public read grant."

tg --token $alice grant public read alice/project
let config = anonymous_config
let output = with-env { TANGRAM_CONFIG: $config } { tg list --no-groups --recursive alice/project | complete }
success $output "An anonymous user should be able to list all entries."
assert ($output.stdout | str contains "alice/project/pkg") "The public tag should be visible without a token."

tg --token $alice revoke public read alice/project
let config = anonymous_config
let output = with-env { TANGRAM_CONFIG: $config } { tg list --no-groups --recursive alice/project | complete }
success $output "An anonymous user should be able to list readable entries after revocation."
assert (not ($output.stdout | str contains "alice/project/pkg")) "The tag should stop being visible after the public read grant is revoked."
