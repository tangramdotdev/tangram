use ../lib/test.nu *
use ../lib/stripe.nu *

let root_token = random chars
let webhook_secret = 'whsec_mock'
let stripe = spawn_stripe
let local = server spawn --config {
	advanced: { checkpoints: true },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	billing: { stripe: { secret_key: 'sk_test_mock', url: $stripe.url, webhook_secret: $webhook_secret } },
}
let alice = tg login --verbose --name alice | from json
with-env { BROWSER: 'false' } {
	tg --token $alice.token user billing manage
}
let event = {
	data: { object: { id: 'cus_mock' } },
	id: 'evt_concurrent',
	type: 'customer.updated',
}
let watch = tg --token $root_token checkpoint watch billing.webhook.store | from json | get watch
let first = job spawn {
	let id = job id
	let status = send_stripe_webhook $local $webhook_secret $event
	$status | job send --tag $id 0
}
success (timeout 10s tg --token $root_token checkpoint wait billing.webhook.store $watch 0 | complete)

{ id: 'cus_mock', invoice_settings: { default_payment_method: null } } | to json | save --force $stripe.customer_path
let second = job spawn {
	let id = job id
	let status = send_stripe_webhook $local $webhook_secret $event
	$status | job send --tag $id 0
}
success (timeout 10s tg --token $root_token checkpoint wait billing.webhook.store $watch 1 | complete)

tg --token $root_token checkpoint continue billing.webhook.store $watch 1
assert equal (job recv --tag $second --timeout 10sec) 200

tg --token $root_token checkpoint continue billing.webhook.store $watch 0
assert equal (job recv --tag $first --timeout 10sec) 200
tg --token $root_token checkpoint unwatch billing.webhook.store $watch

let output = tg --token $alice.token sandbox create --no-tokens --no-network | complete
failure $output 'the delayed duplicate must not overwrite the committed billing state'
assert ($output.stderr | str contains 'billing is not ready')
stop_stripe $stripe
