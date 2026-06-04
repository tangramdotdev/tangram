use ../../test.nu *
use ../lib/permissions.nu *

# Listing exposes an exact public tag without exposing its private siblings.

let server = spawn --config { authentication: true }
tg user login alice
let alice = current_token
let path = artifact 'test'
let id = tg --token $alice checkin $path

tg --token $alice group create exact-public
tg --token $alice tag --public exact-public/pkg $id
tg --token $alice tag exact-public/sibling $id
let config = anonymous_config
let output = with-env { TANGRAM_CONFIG: $config } { tg list --no-groups --recursive exact-public | complete }
success $output "An anonymous user should be able to list exact public tags."
assert ($output.stdout | str contains "exact-public/pkg") "The public tag should be visible without a token."
assert (not ($output.stdout | str contains "exact-public/sibling")) "A public tag should not expose sibling tags."
