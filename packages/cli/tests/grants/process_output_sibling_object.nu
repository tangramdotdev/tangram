use ../../test.nu *

# A child can name a sibling's output without gaining access to it.

let server = spawn --config {
	advanced: { single_process: false },
	authentication: { users: { providers: { insecure: true } } },
}
let alice = tg login --verbose --name alice | from json
let eve = tg login --verbose --name eve | from json

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

let build = tg --token $alice.token build --detach --verbose $path | from json
let wait = tg --token $alice.token wait $build.process | from json
assert equal $wait.exit 0 "the parent may return the output it received from the consumer."

# Give Eve access only to the consumer process.
let consumer = tg --token $alice.token process children $build.process | from json | get 1.process
tg --token $alice.token grant $eve.user.id process_parent $consumer
tg --token $alice.token index

# Eve can read the consumer's return value, but not the object it merely named.
let wait = tg --token $eve.token wait $consumer | from json
assert equal $wait.exit 0
let file = $wait.output.value | split row '?' | first
let leaked = tg --token $eve.token get $file | complete
failure $leaked "the consumer must not gain access to its sibling's output by naming its id."
