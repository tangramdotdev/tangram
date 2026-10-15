use ../lib/test.nu *
use ../lib/stripe.nu *

skip_if_no_cloud

# Authentication invalidations reach other servers and regions in the same instance.
let stripe = spawn_stripe
let root = random chars
let secret = 'whsec_mock'
let common = {
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
tg --url $a.url --token $alice.token user token delete $token.id
wait_until { (tg --url $b.url --token $token.token remote list | complete).exit_code != 0 } 'the other region retained the deleted user token' --timeout 5sec

success (tg --url $b.url --token $alice.token remote list | complete)
tg --url $a.url --token $alice.token logout
wait_until { (tg --url $b.url --token $alice.token remote list | complete).exit_code != 0 } 'the other region retained the logged-out session' --timeout 5sec

let runner = tg --url $a.url --token $root runner create | from json
let token = tg --url $a.url --token $root runner token create $runner.data.id | from json
success (tg --url $b.url --token $token.token remote list | complete)
tg --url $a.url --token $root runner token delete $runner.data.id $token.id
wait_until { (tg --url $b.url --token $token.token remote list | complete).exit_code != 0 } 'the other region retained the deleted runner token' --timeout 5sec

let token = tg --url $a.url --token $root runner token create $runner.data.id | from json
success (tg --url $b.url --token $token.token remote list | complete)
tg --url $a.url --token $root runner delete $runner.data.id
wait_until { (tg --url $b.url --token $token.token remote list | complete).exit_code != 0 } 'the other region retained the deleted runner principal' --timeout 5sec

stop_stripe $stripe
