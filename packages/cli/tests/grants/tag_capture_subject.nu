use ../lib/test.nu *

# Tags capture permissions internally, but cannot receive user-created grants.

let root_token = random chars
let local = server spawn --config {
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } }
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let target = tg --token $alice.token put --no-tokens 'tg.file("capture")' | referent node
tg --token $alice.token tag put private $target
tg --token $root_token index
let tag = tg --token $alice.token tag get private | from json

# Granting read permission on a tag remains supported.
failure (tg --token $bob.token tag get private | complete) "Bob must not read the private tag before receiving permission."
tg --token $alice.token grant $bob.user.id tag_read $tag.id
tg --token $root_token index
success (tg --token $bob.token tag get private | complete) "a grant on a tag must still authorize reading it."
let grants = tg --token $alice.token grants list --resource $tag.id | from json
assert ($grants | any {|grant| $grant.subject == $bob.user.id }) "the grant on the tag must be stored."

# Even root cannot create a grant with the tag as its recipient.
let denied = tg --token $root_token grant $tag.id object_node $target | complete
failure $denied "a tag must not receive a user-created grant."
assert ($denied.stderr | str contains 'a tag cannot be a grant subject') "the recipient guard must reject the grant."
tg --token $root_token index

# These lists read primary grant records, rather than captured index permissions.
let received = tg --token $root_token grants list --subject $tag.id | from json
assert equal ($received | length) 0
let grants = tg --token $root_token grants list --resource $target | from json
assert (not ($grants | any {|grant| $grant.subject == $tag.id })) "the rejected grant must leave no primary record."
