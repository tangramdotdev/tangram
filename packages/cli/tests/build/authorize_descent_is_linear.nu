use ../../test.nu *

# Each lookup retains its own subtree token, so descending does not search the authorization index for parents.

let server = server spawn --config {
	tracing: {
		filter: 'tangram=info,tangram_index::authorize::facts=debug'
		stderr_format: 'json'
	}
}

let path = artifact {
	tangram.ts: '
		const nest = async (depth: number) => {
			let directory = await tg.directory();
			for (let index = depth - 1; index >= 0; index--) {
				directory = await tg.directory({ [`d${index}`]: directory });
			}
			return directory;
		};

		export const descend = async (directory: tg.Directory, depth: number) => {
			const directories = [];
			for (let index = 0; index < depth; index++) {
				if (index > 0) directories.push(directory.id);
				directory = tg.Directory.expect(await directory.get(`d${index}`));
			}
			return directories;
		};

		export default async () => tg.build(descend, await nest(8), 8);
	'
}

let directories = tg build $path | from json
assert equal ($directories | length) 7
server stop $server
let reads = open $server.log
	| lines
	| where ($it | str starts-with '{')
	| each { |line| $line | from json }
	| where $it.fields.message? in ['check object parent for authorization', 'read object parents for authorization']
	| where { |event| $event.fields.object? in $directories }
	| length
assert equal $reads 0
