use ../../test.nu *

# The id of a command must not depend on the expiration of an authorization token embedded in one of its referents. The output of a process carries a token, so passing that output to another build embeds the token in the consuming command. A token expires on a wall clock, so editing an unrelated module and building again later must not change the id of the consuming command, which would defeat the process cache for a subtree that did not change.

let server = spawn

let path = artifact {
	tangram.ts: r#'
		import { consumer, producer } from "./build.tg.ts";
		export const unrelated = "one";
		export default async function () {
			const source = await tg.build(producer).named("producer");
			return await tg.build(consumer, { source }).named("consumer");
		}
	'#
	build.tg.ts: r#'
		export function producer() { return tg.directory({ "a.txt": "a" }); }
		export function consumer(arg) { return tg.file("consumed"); }
	'#
}

# Build the package, which creates the consumer command with a token for the producer's output.
let first = tg build --detach --verbose $path | from json
tg wait $first.process | complete
let first_consumer = tg process children $first.process | from json | get 1 | get process
let first_command = tg process get $first_consumer | from json | get command

# Wait so that a token minted now expires later than the token minted above.
sleep 2sec

# Edit a module that the consumer does not depend on, so that the default export re-runs and mints a fresh token while the consumer command stays the same.
'
	import { consumer, producer } from "./build.tg.ts";
	export const unrelated = "two";
	export default async function () {
		const source = await tg.build(producer).named("producer");
		return await tg.build(consumer, { source }).named("consumer");
	}
' | save --force $"($path)/tangram.ts"

let second = tg build --detach --verbose $path | from json
tg wait $second.process | complete
let second_consumer = tg process children $second.process | from json | get 1 | get process
let second_command = tg process get $second_consumer | from json | get command

assert ($first_command == $second_command) $"The consumer command id must not depend on the token expiration, but the first build created ($first_command) and the second created ($second_command)."
