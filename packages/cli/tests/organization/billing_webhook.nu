use ../../test.nu *
use ../lib/stripe.nu *

# Organization-owned sandboxes use the organization's projected Stripe state.

let webhook_secret = 'whsec_mock'
let stripe = spawn_stripe
let server = spawn --config {
	authentication: { users: { providers: { insecure: true } } },
	billing: { stripe: { secret_key: 'sk_test_mock', url: $stripe.url, webhook_secret: $webhook_secret } },
}

let alice = tg login --verbose --name alice | from json
tg --token $alice.token organization create acme
tg --token $alice.token group create acme/team

let unconfigured = tg --token $alice.token sandbox create --organization acme --no-network | complete
failure $unconfigured "an organization without a Stripe customer should not own a sandbox"
assert ($unconfigured.stderr | str contains 'tg organization billing manage') "the error should explain how to configure organization billing"

with-env { BROWSER: 'false' } {
	tg --token $alice.token organization billing manage acme
}

let event = {
	created: 1,
	data: { object: { id: 'cus_mock' } },
	id: 'evt_organization_ready',
	type: 'customer.updated',
}
assert equal (send_stripe_webhook $server $webhook_secret $event) 200 "a valid webhook should be accepted"

let created = tg --token $alice.token sandbox create --group acme/team --no-network | complete
success $created "a group should inherit its organization's billing status"
tg --token $alice.token sandbox destroy ($created.stdout | str trim)

stop_stripe $stripe
