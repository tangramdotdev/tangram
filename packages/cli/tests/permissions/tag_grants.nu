use ../../test.nu *
use ../lib/permissions.nu *

# Read and write grants on a tag can be created, listed, and deleted, using both the grants subcommands and the grant and revoke aliases.

let server = spawn --config { authentication: true }
let users = login_users
let alice = $users.alice
let path = artifact 'test'
let id = tg --token $alice checkin $path

tg --token $alice group create tag-location-flags
tg --token $alice tag tag-location-flags/pkg $id
tg --token $alice grant $users.bob_id read tag-location-flags/pkg
tg --token $alice grant $users.carol_id write tag-location-flags/pkg
tg --token $alice grants list tag-location-flags/pkg
tg --token $alice grants delete $users.bob_id read tag-location-flags/pkg
tg --token $alice revoke $users.carol_id write tag-location-flags/pkg
