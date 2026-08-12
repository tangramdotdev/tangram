use ../../test.nu *
use ../lib/stripe.nu *

# Managing user billing creates one Stripe customer and reuses it for subsequent portal sessions.

let stripe = spawn_stripe
let server = spawn --config {
	authentication: { users: { providers: { insecure: true } } },
	billing: { stripe: { secret_key: 'sk_test_mock', url: $stripe.url, webhook_secret: 'whsec_mock' } },
}

let alice = tg login --email alice@example.com --verbose --name alice | from json

let first = with-env { BROWSER: 'false', TANGRAM_QUIET: false } {
	tg --token $alice.token user billing manage | complete
}
assert equal $first.exit_code 0 "managing user billing should succeed"
assert ($first.stderr | str contains 'https://example.invalid/stripe-portal') "the portal URL should be printed"

let second = with-env { BROWSER: 'false', TANGRAM_QUIET: false } {
	tg --token $alice.token user billing manage | complete
}
assert equal $second.exit_code 0 "managing user billing again should succeed"

let requests = stripe_requests $stripe
assert equal ($requests | length) 3 "one customer and two portal sessions should be created"
assert equal $requests.0.path '/v1/customers' "the first request should create a customer"
assert ($requests.0.authorization | str starts-with 'Basic ') "the Stripe secret key should authenticate the request"
assert equal $requests.0.idempotencyKey $'tangram-user-($alice.user.id)' "the customer request should be idempotent"
assert ($requests.0.body | str contains 'email=alice%40example.com') "the Stripe customer should include the user email"
assert ($requests.0.body | str contains $'metadata%5Btangram_user_id%5D=($alice.user.id)') "the Stripe customer should identify the Tangram user"
assert equal $requests.1.path '/v1/billing_portal/sessions' "the second request should create a portal session"
assert ($requests.1.body | str contains 'customer=cus_mock') "the portal session should use the Stripe customer"
assert ($requests.1.body | str contains 'flow_data%5Btype%5D=payment_method_update') "the portal session should update the payment method"
assert equal $requests.2.path '/v1/billing_portal/sessions' "the stored customer should be reused"

stop_stripe $stripe
