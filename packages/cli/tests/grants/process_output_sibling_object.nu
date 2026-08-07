use ../../test.nu *

# A child process cannot return a sibling's output when it received only the output's id as a string.

let server = spawn --config { advanced: { single_process: false } }

let path = artifact {
	tangram.ts: '
		export default async function () {
			const file = await tg.build(producer);
			return await tg.build(consumer, file.id);
		}
		export function producer() {
			return tg.file("hello");
		}
		export function consumer(id) {
			return tg.File.withId(id);
		}
	'
}

let build = tg build --detach --verbose $path | from json
let wait = tg wait $build.process | from json
assert equal $wait.exit 1 "the parent must fail when its child returns an unauthorized output."

# The consumer must fail while handling its finish request without storing the output.
let consumer = tg process children $build.process | from json | get 1.process
let wait = tg wait $consumer | from json
assert equal $wait.exit 1
assert equal $wait.error.message "failed to authorize the process output"
assert equal $wait.output? null
