use ../../test.nu *

# Authorizing a shared directory should not get more expensive as more processes reference it.

let server = server spawn --config {
	tracing: {
		filter: 'tangram=info,tangram_index::authorize=debug'
		stderr_format: 'json'
	}
}

let path = artifact {
	tangram.ts: '
		export const touch = async (directory: tg.Directory, i: number) => {
			await directory.entries;
			return i;
		};

		export default async () => {
			const directory = await tg.directory({ file: "contents" });
			for (let i = 0; i < 32; i++) {
				await tg.build(touch, directory, i);
			}
			return directory.id;
		};
	'
}

let id = tg build $path | from json
let reads = open $server.log
	| lines
	| where ($it | str starts-with '{')
	| each { from json }
	| where $it.fields.message? == 'authorize batch'
	| where $it.fields.args? == 1
	| where $it.fields.resource? == $id
	| get fields.reads

let first = $reads | first 8 | math avg
let last = $reads | last 8 | math avg
print $'authorization reads grew from an average of ($first) to ($last)'
assert ($last < ($first * 1.5)) 'authorizing the same directory got more expensive as more processes referenced it'
