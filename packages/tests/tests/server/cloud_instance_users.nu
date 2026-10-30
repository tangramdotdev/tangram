use ../../util/test.nu *

# Cloud instances create and retain independent users with the same name.

let config = { authentication: { users: { providers: { insecure: true } } } }
let first = server spawn --cloud --name first --config $config
let first_alice = tg --url $first.url login --verbose --name alice | from json

let second = server spawn --cloud --name second --config $config
let second_alice = tg --url $second.url login --verbose --name alice | from json
assert ($first_alice.user.id != $second_alice.user.id) 'independent instances should create distinct users'

let first_user = tg --url $first.url --token $first_alice.token user get alice | from json
let second_user = tg --url $second.url --token $second_alice.token user get alice | from json
assert equal $first_user.id $first_alice.user.id
assert equal $second_user.id $second_alice.user.id
