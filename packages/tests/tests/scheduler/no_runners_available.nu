use ../../util/test.nu *

# A sandbox request that no runner can satisfy keeps waiting for a compatible runner instead of failing.

let scheduler = {
	create_sandbox_timeout: 0.25,
	enqueue_retry: { backoff: 0.01, jitter: 0, max_delay: 0.01 },
	max_create_sandbox_attempts: 2,
}

# Run a request, observe the scheduler decline it twice, and check that the request is still waiting.
def assert_waits [url: string, args: list<string>, message: string] {
	let declined = checkpoint watch scheduler.sandbox.declined --url $url
	let request = job spawn {
		let job_id = job id
		tg --url $url ...$args | complete | job send --tag $job_id 0
	}
	checkpoint pass $declined 0
	checkpoint pass $declined 1
	checkpoint unwatch $declined
	let output = try { job recv --tag $request --timeout 0sec } catch { null }
	assert equal $output null $message
	job kill $request
}

# A server without a runner has nothing to schedule on.
let local = server spawn --name local --config {
	advanced: { checkpoints: true },
	roles: [api indexer scheduler],
	scheduler: $scheduler,
}
assert_waits $local.url [sandbox create --no-tokens] "creating a sandbox with no runners should wait for a scheduler with runners"

# A process also waits until a scheduler can accept its sandbox.
let path = artifact {
	tangram.ts: '
		export default function () {
			return "hello";
		}
	',
}
assert_waits $local.url [build $path] "building with no runners should wait for a scheduler with runners"

# A runner whose host does not match the request can never satisfy the sandbox.
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	runner: { cpus: 1, overcommit: { cpu: 1 }, memory: 1_073_741_824 },
	scheduler: $scheduler,
}
assert_waits $runner.url [sandbox create --no-tokens --host nonexistent] "creating a sandbox for an unmatched host should wait for a compatible runner"

# Requests that exceed a runner's total capacity also wait for a compatible runner. Explicit resource options require container or VM isolation on Linux.
if $nu.os-info.name == 'linux' {
	assert_waits $runner.url [sandbox create --no-tokens --cpu-min 2] "creating a sandbox with too many CPUs should wait for a compatible runner"
	assert_waits $runner.url [sandbox create --no-tokens --memory 2147483648] "creating a sandbox with too much memory should wait for a compatible runner"
}
