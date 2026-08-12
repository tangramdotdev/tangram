use ../../test.nu *
use ../lib/stripe.nu *

# Managing organization billing requires admin and reuses the Stripe customer.

let stripe = spawn_stripe
let server = spawn --config {
	authentication: { users: { providers: { insecure: true } } },
	billing: { stripe: { secret_key: 'sk_test_mock', url: $stripe.url, webhook_secret: 'whsec_mock' } },
}

let alice = tg login --verbose --name alice | from json
let eve = tg login --verbose --name eve | from json
let organization = tg --token $alice.token organization create acme | from json

let unauthorized = with-env { BROWSER: 'false' } {
	tg --token $eve.token organization billing manage acme | complete
}
failure $unauthorized "a non-admin user should not be able to manage organization billing"
assert equal ((stripe_requests $stripe) | length) 0 "an unauthorized request should not contact Stripe"

let first = with-env { BROWSER: 'false', TANGRAM_QUIET: false } {
	tg --token $alice.token organization billing manage acme | complete
}
assert equal $first.exit_code 0 "managing organization billing should succeed"
assert ($first.stderr | str contains 'https://example.invalid/stripe-portal') "the portal URL should be printed"

let second = with-env { BROWSER: 'false', TANGRAM_QUIET: false } {
	tg --token $alice.token organization billing manage acme | complete
}
assert equal $second.exit_code 0 "managing organization billing again should succeed"

let requests = stripe_requests $stripe
assert equal ($requests | length) 3 "one customer and two portal sessions should be created"
assert equal $requests.0.path '/v1/customers' "the first request should create a customer"
assert ($requests.0.authorization | str starts-with 'Basic ') "the Stripe secret key should authenticate the request"
assert equal $requests.0.idempotencyKey $'tangram-organization-($organization.id)' "the customer request should be idempotent"
assert ($requests.0.body | str contains $'metadata%5Btangram_organization_id%5D=($organization.id)') "the Stripe customer should identify the Tangram organization"
assert equal $requests.1.path '/v1/billing_portal/sessions' "the second request should create a portal session"
assert ($requests.1.body | str contains 'customer=cus_mock') "the portal session should use the Stripe customer"
assert equal $requests.2.path '/v1/billing_portal/sessions' "the stored customer should be reused"

stop_stripe $stripe
