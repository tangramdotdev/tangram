use ../../test.nu *

# A process_parent grant permits consuming piped stdout, while process_node does not.

let server = server spawn --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg login --verbose --name alice | from json
let eve = tg login --verbose --name eve | from json

let path = artifact {
	tangram.ts: '
		export default async () => {
			let process = await tg.spawn`printf secret; read line`.stdin("pipe").stdout("pipe").sandbox().network(true);
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

let denied = tg --token $eve.token process stdio read $child --stream stdout | complete
failure $denied "process_node must not permit consuming piped stdout."

tg --token $alice.token grant $eve.user.id process_parent $child
tg --token $alice.token index
"\n" | tg --token $alice.token process stdio write $child --stream stdin

let read = tg --token $eve.token process stdio read $child --stream stdout | complete
success $read "process_parent should permit consuming piped stdout."
