use ../../test.nu *

# A user can create, list, use, and delete API tokens.

let server = server spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose --name alice | from json
let created = tg --token $alice.token user token create | from json
assert ($created.data.id | str starts-with "tok_") "a token should have a token ID"
assert (($created.token | str length) > 0) "the token secret should be returned on creation"

let me = tg --token $created.token user whoami | from json
assert equal $me.id $alice.user.id

let tokens = tg --token $alice.token user token list | from json
assert ($created.data.id in $tokens.id) "the created token should be listed"
assert not ("token" in ($tokens | columns)) "token secrets should not be listed"

tg --token $alice.token user token delete $created.data.id
let tokens = tg --token $alice.token user token list | from json
assert not ($created.data.id in $tokens.id) "the deleted token should not be listed"
failure (tg --token $created.token user whoami | complete) "the deleted token should be revoked"
