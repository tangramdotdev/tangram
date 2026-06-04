use ../../test.nu *
use ../lib/permissions.nu *

# Read and write grants on a group can be created, listed, and deleted, using both the grants subcommands and the grant and revoke aliases.

let server = spawn --config { authentication: true }
let users = login_users
let alice = $users.alice

tg --token $alice group create location-flags
tg --token $alice grant $users.bob_id read location-flags
tg --token $alice grant $users.carol_id write location-flags
tg --token $alice grants list location-flags
tg --token $alice user grants bob
tg --token $alice user grants carol
tg --token $alice grants delete $users.bob_id read location-flags
tg --token $alice revoke $users.carol_id write location-flags
