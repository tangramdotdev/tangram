use ../lib/test.nu *

# A scheduler rejects sandbox requests when it has no compatible runners.

let scheduler = {
	create_sandbox_timeout: 0.25,
	max_create_sandbox_attempts: 2,
}

# A server without a runner has nothing to schedule on.
let local = server spawn --name local --config {
	roles: [api indexer scheduler],
	scheduler: $scheduler,
}
let output = timeout 2s tg --url $local.url sandbox create --no-tokens | complete
assert equal $output.exit_code 124 "creating a sandbox with no runners should wait for a scheduler with runners"

# A process also waits until a scheduler can accept its sandbox.
let path = artifact {
	tangram.ts: '
		export default function () {
			return "hello";
		}
	',
}
let output = timeout 2s tg --url $local.url build $path | complete
assert equal $output.exit_code 124 "building with no runners should wait for a scheduler with runners"

# A runner whose host does not match the request can never satisfy the sandbox.
let runner = server spawn --name runner --config {
	runner: { cpus: 1, memory: 1_073_741_824 },
	scheduler: $scheduler,
}
let output = timeout 2s tg --url $runner.url sandbox create --no-tokens --host nonexistent | complete
assert equal $output.exit_code 124 "creating a sandbox for an unmatched host should wait for a compatible runner"

# Requests that exceed a runner's total capacity also wait for a compatible runner.
# Explicit resource options require container or VM isolation on Linux.
if $nu.os-info.name == 'linux' {
	let output = timeout 2s tg --url $runner.url sandbox create --no-tokens --dedicated-cpu 2 | complete
	assert equal $output.exit_code 124 "creating a sandbox with too many CPUs should wait for a compatible runner"
	let output = timeout 2s tg --url $runner.url sandbox create --no-tokens --memory 2147483648 | complete
	assert equal $output.exit_code 124 "creating a sandbox with too much memory should wait for a compatible runner"
}
