use ../../util/test.nu *

# A request that no runner could ever fit follows the scheduler's unplaceable timeout.

let path = artifact {
	tangram.ts: '
		export default function () {
			return "hello";
		}
	',
}

# A timeout of zero rejects the request, so the call fails.
let server = server spawn --name reject --config {
	roles: [api indexer scheduler],
	scheduler: { unplaceable_timeout: 0 },
}
let output = tg --url $server.url sandbox create --no-tokens | complete
failure $output "a request that no runner can fit should be rejected"
assert ($output.stderr | str contains "no runners available")

# A duration accepts the request, and then records the failure in the sandbox's error field.
let server = server spawn --name timeout --config {
	roles: [api indexer scheduler],
	scheduler: { unplaceable_timeout: 0.25 },
}
let sandbox = tg --url $server.url sandbox create --no-tokens | str trim
let output = tg --url $server.url sandbox wait $sandbox | complete
success $output "the sandbox should end after the timeout"
let data = tg --url $server.url sandbox get $sandbox | from json
assert equal $data.data.status 'destroyed'
assert ($data.data.error.message | str contains "no runners available")

# With a wait, the call reports the failure.
let output = tg --url $server.url sandbox create --no-tokens --wait | complete
failure $output "a waiting create should report the failure"
assert ($output.stderr | str contains "no runners available")

# A null timeout keeps the request queued for a runner that may arrive later.
let server = server spawn --name indefinite --config {
	roles: [api indexer scheduler],
	scheduler: { unplaceable_timeout: null },
}
let sandbox = tg --url $server.url sandbox create --no-tokens | str trim
sleep 500ms
let data = tg --url $server.url sandbox get $sandbox | from json
assert equal $data.data.status 'starting'
