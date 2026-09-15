use ../../../test.nu *

def assert_idle_error [source: string] {
	let path = artifact {
		tangram.ts: $source,
	}
	let output = tg run --sandbox $path | complete
	failure $output
	assert (
		$output.stderr
		| str contains "the JavaScript runtime became idle while the result promise was pending"
	) "an unresolved result promise should fail when the runtime becomes idle"
}

let server = server spawn

# An unawaited unresolved promise does not keep the process result pending.
let path = artifact {
	tangram.ts: '
		export default function () {
			void new Promise(() => {});
			return "ok";
		}
	',
}
let output = tg run --sandbox $path | from json
assert equal $output "ok"

# An unresolved result promise fails when the runtime has no work capable of settling it.
assert_idle_error '
	export default async function () {
		await new Promise(() => {});
	}
'

# A handled rejection does not replace the idle result error.
assert_idle_error '
	export default async function () {
		let promise = Promise.reject(new Error("handled"));
		promise.catch(() => {});
		await new Promise(() => {});
	}
'
