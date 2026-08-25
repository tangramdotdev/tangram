use ../../test.nu *

# A group can be retrieved by its id and by its specifier.

skip_if_no_tokens

let server = server spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose --name alice | from json

let group = tg --token $alice.token group create project | from json

let output = with-env { TANGRAM_QUIET: "false" } { tg --token $alice.token group get $group.id | complete }
success $output
let by_id = $output.stdout | from json
assert ($by_id.id == $group.id) "getting a group by id should return that group"
assert (($by_id | get --optional location) == null) "getting a group by id should not print a location to stdout"
assert (($by_id | get --optional tokens) == null) "getting a group by id should not print tokens to stdout"
assert equal ($output.stderr | lines | length) 2 "getting a group by id should print location and token info"

let output = with-env { TANGRAM_QUIET: "false" } { tg --token $alice.token group get project | complete }
success $output
let by_specifier = $output.stdout | from json
assert ($by_specifier.id == $group.id) "getting a group by specifier should return that group"
assert (($by_specifier | get --optional location) == null) "getting a group by specifier should not print a location to stdout"
assert (($by_specifier | get --optional tokens) == null) "getting a group by specifier should not print tokens to stdout"
assert equal ($output.stderr | lines | length) 2 "getting a group by specifier should print location and token info"

let output = with-env { TANGRAM_QUIET: "false" } { tg --token $alice.token get $group.id | complete }
success $output
let generic = $output.stdout | from json
assert ($generic.id == $group.id) "generic get should return the group"
assert (($generic | get --optional location) == null) "generic get should not print a location to stdout"
assert (($generic | get --optional tokens) == null) "generic get should not print tokens to stdout"
assert equal ($output.stderr | lines | length) 1 "generic get should print the resolved referent as info"
assert ($output.stderr | str contains "location=local") "the resolved referent should include its location"
assert ($output.stderr | str contains "tokens") "the resolved referent should include its tokens"
