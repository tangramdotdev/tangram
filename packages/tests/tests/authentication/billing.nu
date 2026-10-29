use ../../util/test.nu *
use ../../util/stripe.nu *

skip_test --cloud

# Billing-unready users recheck the database, and readiness revocation invalidates a warm cache.
let stripe = spawn_stripe
let root = random chars
let secret = 'whsec_mock'
let common = {
	advanced: { checkpoints: true },
	authentication: { cache: { ttl: 60 }, root: { token: $root }, users: { providers: { insecure: true } } },
	billing: { stripe: { secret_key: 'sk_test_mock', url: $stripe.url, webhook_secret: $secret } },
	database: { index_queue: { wakeup_interval: 0.01 } },
	roles: [api indexer runner scheduler],
}
let instance = instance --cloud --primary-region a --regions [{ name: a }, { name: b }] --config $common
let a = server spawn --instance $instance --region a --name a --url (instance region url $instance a)
let b = server spawn --instance $instance --region b --name b --url (instance region url $instance b)
let alice = tg --url $a.url login --verbose --email alice@example.com --name alice | from json
with-env { BROWSER: 'false' } { tg --url $a.url --token $alice.token user billing manage }
tg --url $b.url --token $alice.token index
failure (tg --url $b.url --token $alice.token sandbox create --no-tokens --no-network | complete)
let invalidated = checkpoint watch authentication.cache.invalidated --params { id: $alice.user.id, kind: principal } --token $root --url $b.url
let ready = { data: { object: { id: cus_mock } }, id: evt_ready, type: customer.updated }
assert equal (send_stripe_webhook $a $secret $ready) 200
checkpoint pass $invalidated 0 | ignore
let sandbox = tg --url $b.url --token $alice.token sandbox create --no-tokens --no-network | referent node
tg --url $b.url --token $alice.token sandbox destroy $sandbox

{ id: cus_mock, invoice_settings: { default_payment_method: null } } | to json | save --force $stripe.customer_path
let unready = { data: { object: { id: cus_mock } }, id: evt_unready, type: customer.updated }
assert equal (send_stripe_webhook $a $secret $unready) 200
checkpoint pass $invalidated 1 | ignore
checkpoint unwatch $invalidated
let output = tg --url $b.url --token $alice.token sandbox create --no-tokens --no-network | complete
failure $output 'the other region retained cached billing readiness'
assert ($output.stderr | str contains 'billing is not ready') 'the other region retained cached billing readiness'
stop_stripe $stripe
