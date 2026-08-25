use ../../test.nu *
use ../lib/stripe.nu *

# Managing billing does not hold a database connection while Stripe responds.

let root_token = random chars
let stripe = spawn_stripe --customer-delay 2sec
let server = server spawn --config {
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } }
	billing: { stripe: { secret_key: 'sk_test_mock', url: $stripe.url, webhook_secret: 'whsec_mock' } }
	database: { kind: 'sqlite', pool: { max: 1 } }
}
let alice = tg login --verbose --name alice | from json
let manage = job spawn {
	let job_id = job id
	let output = with-env { BROWSER: 'false' } {
		tg --token $alice.token user billing manage | complete
	}
	$output | job send --tag $job_id 0
}

wait_until { (stripe_requests $stripe | length) == 1 } "the customer request should start"
let create = timeout 1s tg --token $root_token group create available | complete
assert equal $create.exit_code 0 "an unrelated database write should not wait for Stripe"

let output = job recv --tag $manage --timeout 5sec
assert equal $output.exit_code 0 "managing user billing should succeed"
stop_stripe $stripe
