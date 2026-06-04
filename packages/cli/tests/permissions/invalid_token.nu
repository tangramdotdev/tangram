use ../../test.nu *
use ../lib/permissions.nu *

# An invalid token is treated as anonymous for readable entries and cannot inspect grants, and a bad token in the configuration does not block anonymous-capable requests.

let server = spawn --config { authentication: true }
tg user login alice
let alice = current_token
let path = artifact 'test'
let id = tg --token $alice checkin $path

tg --token $alice group create alice/project
tg --token $alice tag put alice/project/pkg $id

let output = tg --token invalid list --no-groups --recursive alice/project | complete
success $output "An invalid token should be treated as anonymous for readable entries."
assert (not ($output.stdout | str contains "alice/project/pkg")) "The private tag should not be visible with an invalid token."

let output = tg --token invalid grants list alice/project | complete
assert_unauthorized $output "An invalid token should not be able to inspect group grants."

let config = invalid_token_config
let output = with-env { TANGRAM_CONFIG: $config } { tg health | complete }
success $output "A bad token in the config should not prevent anonymous-capable requests."
