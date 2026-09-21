use ../../test.nu *

# Process node access exposes inline fields; command access covers each input subtree.

let server = server spawn --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg login --verbose --name alice | from json
let eve = tg login --verbose --name eve | from json
let path = artifact {
	tangram.ts: '
		export const consume = (_first: tg.File, _second: tg.File) => true;
		export default async () => {
			const first = await tg.file("first private input");
			const second = await tg.file("second private input");
			const process = await tg.spawn(consume, first, second).sandbox();
			const output = await process.wait();
			tg.assert(output.exit === 0);
			return { first: first.id, second: second.id, process: process.id };
		};
	'
}
let output = tg --token $alice.token build $path | from json
tg --token $alice.token index
tg --token $alice.token grant $eve.user.id process_node $output.process | ignore
let data = tg --token $eve.token get $output.process | from json
assert (($data.command.node | describe) | str starts-with record)
for input in [$output.first $output.second] {
	failure (tg --token $eve.token read $input | complete) "process node access must not confer input access"
}
tg --token $alice.token grant $eve.user.id process_node_command $output.process | ignore
assert equal (tg --token $eve.token read $output.first | str trim) "first private input"
assert equal (tg --token $eve.token read $output.second | str trim) "second private input"
