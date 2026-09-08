use ../../test.nu *

# A process that stores an object whose children name the same child twice must receive a token for its subtree, so checking that object out costs no more authorization work than for an object with distinct children.

let server = server spawn --config {
	tracing: { filter: 'tangram=info,tangram_index::authorize=debug', stderr_format: 'json' }
}

let path = artifact {
	tangram.ts: '
		export default async (mark: string) => {
			const shared = await tg.file(`shared ${mark}`);
			const other = await tg.file(`other ${mark}`);
			// One child under two names serializes to a single child, but to two referents.
			const output = mark === "duplicate"
				? await tg.directory({ a: shared, b: shared })
				: await tg.directory({ a: shared, b: other });
			await output.store();
			const reference = tg.Referent.toDataString(
				tg.Object.toReferent(output),
				id => id,
			);
			return tg.command({
				args: ["checkout", "--dependencies=false", "--path", tg.output, reference],
				env: { OUTPUT: output },
				executable: "tg",
				host: tg.host.current,
			});
		};
	'
}

let command = tg build $path -a distinct | str trim
let output = tg build $command | complete
success $output "a process should check out a directory with distinct children"
let distinct = $output.stdout | str trim

let command = tg build $path -a duplicate | str trim
let output = tg build $command | complete
success $output "a process should check out a directory whose children name the same child twice"
let duplicate = $output.stdout | str trim

let searches = open $server.log
	| lines
	| where ($it | str starts-with '{')
	| each { from json }
	| where ($in.fields.message? | default '') == 'authorize batch'
	| get fields.resource
let distinct = $searches | where $it == $distinct | length
let duplicate = $searches | where $it == $duplicate | length
print $'authorizing the checkout took ($distinct) searches with distinct children and ($duplicate) with a repeated child'
assert ($duplicate <= $distinct) 'a repeated child should not cost extra authorization searches'
