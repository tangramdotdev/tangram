use ../../test.nu *

# A process_write grant permits writing piped stdin, while process_node does not.

let server = spawn --config { authentication: { providers: { insecure: true } } }
let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

let path = artifact {
	tangram.ts: '
		export default async () => {
			let process = await tg.spawn`read line; echo "got:$line"`.stdin("pipe").stdout("log").sandbox().network(true);
			console.log(process.id);
			await tg.sleep(30);
		};
	'
}
let parent = tg --token $alice.token run --network=true --detach --verbose $path | from json
wait_until { (tg --token $alice.token log $parent.process | str trim | str length) > 0 } "the parent should log the child process id"
let child = tg --token $alice.token log $parent.process | str trim

tg --token $alice.token grant $eve.user.id process_node $child
tg --token $alice.token index

let denied = "hello\n" | tg --token $eve.token process stdio write $child --stream stdin | complete
failure $denied "process_node must not permit writing piped stdin."

tg --token $alice.token grant $eve.user.id process_write $child
tg --token $alice.token index

let wrote = "hello\n" | tg --token $eve.token process stdio write $child --stream stdin | complete
success $wrote "process_write should permit writing piped stdin."
