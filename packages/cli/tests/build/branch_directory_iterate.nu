use ../lib/test.nu *

# Iterating a directory large enough to be split into branch nodes yields all of its entry names.

let local = server spawn

# Create a directory with 1,025 entries, one more than fit in a directory leaf.
let entries = 0..<1_025 | each { |i| { name: $'($i | into string | fill --alignment right --character "0" --width 4).txt', contents: $'File ($i)' } } | transpose --header-row --as-record
let path = artifact {
	tangram.ts: '
		import directory from "./data" with { type: "directory" };
		export default async function () {
			let entries: Array<string> = [];
			for await (let [name, _artifact] of directory) {
				entries.push(name);
			}
			return [entries.length, entries[0], entries[entries.length - 1]].join(",");
		}
	'
	data: $entries
}

let output = tg build $path
snapshot $output
