use ../../test.nu *

# Each lookup inherits the token returned for its parent, so it reads only the current directory's parents.

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
server stop $server
let reads = open $server.log
	| lines
	| where ($it | str starts-with '{')
	| each { |line| $line | from json }
	| where $it.fields.message? == 'read object parents for authorization'
	| where { |event| $event.fields.object? in $directories }
	| group-by { |event| $event.fields.object }
	| values
	| each { |events| $events | length }
	| sort
let expected = $directories | each { 1 }
assert equal $reads $expected
