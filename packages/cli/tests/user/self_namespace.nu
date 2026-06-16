use ../../test.nu *

# A user inherently has permission over their own namespace, so they can tag and create groups directly under their username.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

let alice_user = tg user login alice | from json
let alice = current_token

let path = artifact 'hello'
let id = tg --token $alice checkin $path

# Alice can publish a tag under her own username namespace.
tg --token $alice tag alice/foo $id
let tag = tg --token $alice tag get alice/foo | from json
assert ($tag.specifier == "alice/foo") "the tag should live under alice's namespace"
assert ($tag.parent == $alice_user.id) "the tag's parent should be alice's own user node"

# Alice can also create a group under her own namespace.
tg --token $alice group create alice/proj
