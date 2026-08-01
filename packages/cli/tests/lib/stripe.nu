use ../../test.nu *

const mock_path = path self mock_stripe.ts
const signature_path = path self stripe_signature.ts

export def spawn_stripe [] {
	let port_path = mktemp
	let requests_path = mktemp
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
			exec bun run \"($mock_path)\" \"($port_path)\" \"($requests_path)\"
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
	let signature = bun run $signature_path $secret $timestamp $body
	let socket = $server.url | str replace 'http+unix://' '' | url decode
	let output = (
		^curl
			--silent
			--show-error
			--output /dev/null
			--write-out '%{http_code}'
			--unix-socket $socket
			--request POST
			--header 'Content-Type: application/json'
			--header $'Stripe-Signature: ($signature)'
			--data-binary $body
			'http://localhost/billing/stripe/webhook'
		| complete
	)
	assert equal $output.exit_code 0 "sending the Stripe webhook should succeed"

	$output.stdout | into int
}

export def send_invalid_stripe_webhook [server: record, event: record] {
	let body = $event | to json --raw
	let socket = $server.url | str replace 'http+unix://' '' | url decode
	let output = (
		^curl
			--silent
			--show-error
			--output /dev/null
			--write-out '%{http_code}'
			--unix-socket $socket
			--request POST
			--header 'Content-Type: application/json'
			--header 'Stripe-Signature: t=0,v1=invalid'
			--data-binary $body
			'http://localhost/billing/stripe/webhook'
		| complete
	)
	assert equal $output.exit_code 0 "sending the invalid Stripe webhook should succeed"

	$output.stdout | into int
}

export def stop_stripe [stripe: record] {
	try { job kill $stripe.job }
}
