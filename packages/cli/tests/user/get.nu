use ../../test.nu *

# A user can get their own record by id and by specifier.

let server = server spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose --name alice | from json

let output = with-env { TANGRAM_QUIET: "false" } { tg --token $alice.token user get $alice.user.id | complete }
success $output
let by_id = $output.stdout | from json
assert ($by_id.id == $alice.user.id) "getting a user by id should return that user"
assert (($by_id | get --optional location) == null) "getting a user by id should not print a location to stdout"
assert (($by_id | get --optional tokens) == null) "getting a user by id should not print tokens to stdout"
assert equal ($output.stderr | lines | length) 2 "getting a user by id should print location and token info"

let output = with-env { TANGRAM_QUIET: "false" } { tg --token $alice.token user get alice | complete }
success $output
let by_specifier = $output.stdout | from json
assert ($by_specifier.id == $alice.user.id) "getting a user by specifier should return that user"
assert (($by_specifier | get --optional location) == null) "getting a user by specifier should not print a location to stdout"
assert (($by_specifier | get --optional tokens) == null) "getting a user by specifier should not print tokens to stdout"
assert equal ($output.stderr | lines | length) 2 "getting a user by specifier should print location and token info"
