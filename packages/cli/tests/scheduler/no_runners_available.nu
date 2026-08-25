use ../../test.nu *

# A sandbox that no registered runner can satisfy is retried for a grace period and then discarded, rather than waiting forever.

let scheduler = {
	create_sandbox_timeout: 0.25,
	max_create_sandbox_attempts: 2,
}

# A server without a runner has nothing to schedule on.
let server = server spawn --name server --config {
	roles: [cleaner http indexer scheduler],
	scheduler: $scheduler,
}
let output = tg --url $server.url sandbox create | complete
failure $output "creating a sandbox with no runners should fail"
assert ($output.stderr | str contains 'no runners available')

# A process whose sandbox cannot be scheduled is canceled with the same error.
let path = artifact {
	tangram.ts: '
		export default function () {
			return "hello";
		}
	',
}
let output = tg --url $server.url build $path | complete
failure $output "building with no runners should fail"
assert ($output.stderr | str contains 'no runners available')

# A runner whose host does not match the request can never satisfy the sandbox.
let runner = server spawn --name runner --config {
	runner: { cpus: 1 },
	scheduler: $scheduler,
}
let output = tg --url $runner.url sandbox create --host nonexistent | complete
failure $output "creating a sandbox for an unmatched host should fail"
assert ($output.stderr | str contains 'no runners available')
