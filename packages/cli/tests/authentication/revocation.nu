use ../lib/test.nu *
use ../lib/stripe.nu *

skip_if_no_cloud

# Authentication invalidations reach other servers and regions in the same instance.
let stripe = spawn_stripe
let root = random chars
let secret = 'whsec_mock'
let common = {
	advanced: { checkpoints: true },
	authentication: { cache: { ttl: 60 }, root: { token: $root }, users: { providers: { insecure: true } } },
	billing: { stripe: { secret_key: 'sk_test_mock', url: $stripe.url, webhook_secret: $secret } },
	database: { index_queue: { wakeup_interval: 0.01 } },
}
let instance = instance --cloud --primary-region a --regions [{ name: a }, { name: b }] --config $common
let a = server spawn --instance $instance --region a --name a --url (instance region url $instance a)
let b = server spawn --instance $instance --region b --name b --url (instance region url $instance b)
let alice = tg --url $a.url login --verbose --email alice@example.com --name alice | from json
with-env { BROWSER: 'false' } { tg --url $a.url --token $alice.token user billing manage }
let ready = { data: { object: { id: cus_mock } }, id: evt_ready, type: customer.updated }
assert equal (send_stripe_webhook $a $secret $ready) 200

let token = tg --url $a.url --token $alice.token user token create | from json
success (tg --url $b.url --token $token.token remote list | complete)
let invalidated = checkpoint watch authentication.cache.invalidated --params { kind: token } --token $root --url $b.url
tg --url $a.url --token $alice.token user token delete $token.id
checkpoint pass $invalidated | ignore
checkpoint unwatch $invalidated
failure (tg --url $b.url --token $token.token remote list | complete) 'the other region retained the deleted user token'

success (tg --url $b.url --token $alice.token remote list | complete)
let invalidated = checkpoint watch authentication.cache.invalidated --params { kind: token } --token $root --url $b.url
tg --url $a.url --token $alice.token logout
checkpoint pass $invalidated | ignore
checkpoint unwatch $invalidated
failure (tg --url $b.url --token $alice.token remote list | complete) 'the other region retained the logged-out session'

let runner = tg --url $a.url --token $root runner create | from json
let token = tg --url $a.url --token $root runner token create $runner.data.id | from json
success (tg --url $b.url --token $token.token remote list | complete)
let invalidated = checkpoint watch authentication.cache.invalidated --params { kind: token } --token $root --url $b.url
tg --url $a.url --token $root runner token delete $runner.data.id $token.id
checkpoint pass $invalidated | ignore
checkpoint unwatch $invalidated
failure (tg --url $b.url --token $token.token remote list | complete) 'the other region retained the deleted runner token'

let token = tg --url $a.url --token $root runner token create $runner.data.id | from json
success (tg --url $b.url --token $token.token remote list | complete)
let invalidated = checkpoint watch authentication.cache.invalidated --params { id: $runner.data.id, kind: principal } --token $root --url $b.url
tg --url $a.url --token $root runner delete $runner.data.id
checkpoint pass $invalidated | ignore
checkpoint unwatch $invalidated
failure (tg --url $b.url --token $token.token remote list | complete) 'the other region retained the deleted runner principal'

stop_stripe $stripe
