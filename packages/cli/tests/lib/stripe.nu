use ../../test.nu *

const signature_source = r#'
import { createHmac } from "node:crypto";

const [secret, timestamp, payload] = process.argv.slice(1);
if (secret === undefined || timestamp === undefined || payload === undefined) {
	throw new Error("expected the secret, timestamp, and payload");
}
const signature = createHmac("sha256", secret)
	.update(`${timestamp}.${payload}`)
	.digest("hex");
process.stdout.write(`t=${timestamp},v1=${signature}`);
'#
const stripe_path = path self stripe.ts

export def spawn_stripe [--customer-delay: duration = 0sec] {
	let port_path = mktemp
	let requests_path = mktemp
	let customer_delay_ms = $customer_delay / 1ms | into int
	let job = job spawn -d stripe {
		bash -c $"
			PARENT_PID=\$PPID
			SELF_PID=\$\$
			\(
				while kill -0 \$PARENT_PID 2>/dev/null; do
					sleep 0.05
				done
				kill -TERM -\$SELF_PID 2>/dev/null || true
			\) &
			exec bun run \"($stripe_path)\" \"($port_path)\" \"($requests_path)\" \"($customer_delay_ms)\"
		"
	}
	wait_until {
		try { (open --raw $port_path | str trim | is-not-empty) } catch { false }
	} "the mock Stripe server did not start"
	let port = open --raw $port_path | str trim
	{
		job: $job,
		requests_path: $requests_path,
		url: $'http://127.0.0.1:($port)',
	}
}

export def stripe_requests [stripe: record] {
	open --raw $stripe.requests_path
	| lines
	| each { |line| $line | from json }
}

export def send_stripe_webhook [server: record, secret: string, event: record] {
	let body = $event | to json --raw
	let timestamp = date now | into int | $in / 1_000_000_000 | math floor
	let signature = ^bun --eval $signature_source $secret ($timestamp | into string) $body
	let socket = $server.url | str replace 'http+unix://' '' | url decode
	let status = (
		$body
		| into binary
		| http post
			--allow-errors
			--headers {
				'Content-Type': 'application/json',
				'Stripe-Signature': $signature,
			}
			--unix-socket $socket
			'http://localhost/webhooks/stripe'
		| metadata
		| get http_response.status
	)

	$status
}

export def send_invalid_stripe_webhook [server: record, event: record] {
	let body = $event | to json --raw
	let socket = $server.url | str replace 'http+unix://' '' | url decode
	let status = (
		$body
		| into binary
		| http post
			--allow-errors
			--headers {
				'Content-Type': 'application/json',
				'Stripe-Signature': 't=0,v1=invalid',
			}
			--unix-socket $socket
			'http://localhost/webhooks/stripe'
		| metadata
		| get http_response.status
	)

	$status
}

export def stop_stripe [stripe: record] {
	try { job kill $stripe.job }
}
