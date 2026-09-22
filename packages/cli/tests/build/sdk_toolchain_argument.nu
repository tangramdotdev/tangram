use ../lib/test.nu *

# Reading a tool through an SDK symlink overlay must not prevent passing the overlay to another build.
# Reduced from std buildSdk: its bootstrap bin directory contained 85 distinct tool files.

let server = server spawn
let tools = 0..<85 | reduce --fold {} { |i, entries| $entries | insert $'tool($i)' $'tool($i)' }
let path = artifact {
	tools: $tools
	tangram.ts: '
		import tools from "./tools" with { type: "directory" };

		export async function toolchain() {
			return tg.directory(Object.fromEntries(
				Object.keys(await tools.entries).map((name) => [name, tg.symlink(tg`${tools}/${name}`)]),
			));
		}

		export async function child(directory: tg.Directory) {
			return Object.keys(await directory.entries).length;
		}

		export default async function () {
			const directory = await tg.build(toolchain);
			await directory.get("tool0");
			return tg.build(child, directory);
		}
	'
}

let output = tg build $path | complete
success $output
snapshot ($output.stdout | str trim) '85'
