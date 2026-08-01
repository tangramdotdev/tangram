use ../../test.nu *
use ../lib/stripe.nu *

# Stripe webhooks project payment method state locally for sandbox billing checks.

let webhook_secret = 'whsec_mock'
let stripe = spawn_stripe
let server = spawn --config {
	authentication: { users: { providers: { insecure: true } } },
	billing: { stripe: { secret_key: 'sk_test_mock', url: $stripe.url, webhook_secret: $webhook_secret } },
}

let alice = tg login --email alice@example.com --verbose alice | from json

let unconfigured = tg --token $alice.token sandbox create --no-network | complete
failure $unconfigured "a user without a Stripe customer should not create a sandbox"
assert ($unconfigured.stderr | str contains 'tg user billing manage') "the error should explain how to configure billing"

with-env { BROWSER: 'false' } {
	tg --token $alice.token user billing manage
}

let incomplete = tg --token $alice.token sandbox create --no-network | complete
failure $incomplete "a user without a default payment method should not create a sandbox"
assert ($incomplete.stderr | str contains 'billing is not ready') "the error should explain that billing is not ready"

let event = {
	created: 1,
	data: { object: { id: 'cus_mock' } },
	id: 'evt_user_ready',
	type: 'customer.updated',
}
assert equal (send_invalid_stripe_webhook $server $event) 400 "an invalid webhook signature should be rejected"
assert equal (send_stripe_webhook $server $webhook_secret $event) 200 "a valid webhook should be accepted"
assert equal (send_stripe_webhook $server $webhook_secret $event) 200 "a duplicate webhook should be accepted"

let requests = stripe_requests $stripe
assert equal ($requests | length) 3 "the duplicate webhook should not retrieve the customer twice"
assert equal $requests.2.method 'GET' "the webhook should retrieve the latest customer state"
assert equal $requests.2.path '/v1/customers/cus_mock' "the webhook should retrieve the affected customer"

let created = tg --token $alice.token sandbox create --no-network | complete
success $created "a user with a default payment method should create a sandbox"
tg --token $alice.token sandbox destroy ($created.stdout | str trim)

stop_stripe $stripe
