use ../lib/test.nu *
use ../lib/stripe.nu *

skip_if_no_cloud

# Billing-unready users recheck the database, and readiness revocation invalidates a warm cache.
let stripe = spawn_stripe
let secret = 'whsec_mock'
let common = {
	authentication: { cache: { ttl: 60 }, users: { providers: { insecure: true } } },
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
let ready = { data: { object: { id: cus_mock } }, id: evt_ready, type: customer.updated }
assert equal (send_stripe_webhook $a $secret $ready) 200
let sandbox = tg --url $b.url --token $alice.token sandbox create --no-tokens --no-network | referent node
tg --url $b.url --token $alice.token sandbox destroy $sandbox

{ id: cus_mock, invoice_settings: { default_payment_method: null } } | to json | save --force $stripe.customer_path
let unready = { data: { object: { id: cus_mock } }, id: evt_unready, type: customer.updated }
assert equal (send_stripe_webhook $a $secret $unready) 200
wait_until {
	let result = tg --url $b.url --token $alice.token sandbox create --no-tokens --no-network | complete
	if $result.exit_code == 0 {
		tg --url $b.url --token $alice.token sandbox destroy ($result.stdout | referent node)
		false
	} else {
		$result.stderr | str contains 'billing is not ready'
	}
} 'the other region retained cached billing readiness' --timeout 5sec
stop_stripe $stripe
