use ../../test.nu *

# A sandbox releases its runner capacity after its process exits so that a
# subsequent build can start.
#
# Regression test for 23a72a86.

let server = server spawn --config {
	advanced: {
		checkpoints: true,
	},
	runner: {
		cpus: 1,
		memory: 1_073_741_824,
	},
}

def build_background [path: path] {
	job spawn {
		let job_id = job id
		let output = tg build $path | complete
		$output | job send --tag $job_id 0
	}
}

def receive_build [job: int] {
	let output = try { job recv --tag $job --timeout 10sec } catch { null }
	if $output == null {
		error make { msg: 'the build timed out' }
	}
	success $output
}

let first_path = artifact {
	tangram.ts: 'export default function () { return "first"; }'
}
let second_path = artifact {
	tangram.ts: 'export default function () { return "second"; }'
}

let watch = (
	tg checkpoint watch runner.sandbox.capacity.release
	| from json
	| get watch
)

# Hold the first sandbox immediately before it releases the runner's only CPU.
let first = build_background $first_path
tg checkpoint wait runner.sandbox.capacity.release $watch 0 | ignore
receive_build $first
tg checkpoint continue runner.sandbox.capacity.release $watch 0
tg checkpoint unwatch runner.sandbox.capacity.release $watch

# The released capacity must allow another build to start.
receive_build (build_background $second_path)
