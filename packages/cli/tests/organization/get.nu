use ../../test.nu *

# An organization can be retrieved by its id and by its specifier.

let server = server spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose --name alice | from json

let organization = tg --token $alice.token organization create acme | from json

let output = with-env { TANGRAM_QUIET: "false" } { tg --token $alice.token organization get $organization.id | complete }
success $output
let by_id = $output.stdout | from json
assert ($by_id.id == $organization.id) "getting an organization by id should return that organization"
assert (($by_id | get --optional location) == null) "getting an organization by id should not print a location to stdout"
assert (($by_id | get --optional tokens) == null) "getting an organization by id should not print tokens to stdout"
assert equal ($output.stderr | lines | length) 2 "getting an organization by id should print location and token info"

let output = with-env { TANGRAM_QUIET: "false" } { tg --token $alice.token organization get acme | complete }
success $output
let by_specifier = $output.stdout | from json
assert ($by_specifier.id == $organization.id) "getting an organization by specifier should return that organization"
assert (($by_specifier | get --optional location) == null) "getting an organization by specifier should not print a location to stdout"
assert (($by_specifier | get --optional tokens) == null) "getting an organization by specifier should not print tokens to stdout"
assert equal ($output.stderr | lines | length) 2 "getting an organization by specifier should print location and token info"
