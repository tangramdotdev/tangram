use ../../test.nu *

# Canceling a running build releases its sandbox's capacity so that a subsequent
# build can start.
#
# Regression test added in cd5bbb68.

let server = server spawn --config {
	advanced: {
		checkpoints: true,
	},
	runner: {
		cpus: 1,
		memory: 1_073_741_824,
	},
}

let long = artifact {
	tangram.ts: '
		export default async function () {
			await tg.run`sleep 60`.sandbox();
			return "done";
		}
	',
}

let watch = (
	tg checkpoint watch runner.sandbox.capacity.release
	| from json
	| get watch
)

let process = tg build --detach --verbose $long | from json
tg cancel $process.process $process.lease

# Wait until cancellation cleanup reaches the capacity release.
tg checkpoint wait runner.sandbox.capacity.release $watch 0 | ignore
tg checkpoint continue runner.sandbox.capacity.release $watch 0
tg checkpoint unwatch runner.sandbox.capacity.release $watch

let short = artifact {
	tangram.ts: '
		export default function () { return "hello"; }
	',
}

let output = timeout 10s tg build $short | complete
success $output "build should not hang after cancellation"
