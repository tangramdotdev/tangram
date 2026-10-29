use ../../lib/test.nu *

# Directory.get retrieves a file by name from a directory large enough to be split into branch nodes.

let local = server spawn

# Create a directory with 1,025 entries, one more than fit in a directory leaf.
let entries = 0..<1_025 | each { |i| { name: $'($i | into string | fill --alignment right --character "0" --width 4).txt', contents: $'File ($i)' } } | transpose --header-row --as-record
let path = artifact {
	tangram.ts: '
		import directory from "./data" with { type: "directory" };
		export default async function () {
			let file = await directory.get("1024.txt");
			tg.File.assert(file);
			return file.text;
		}
	'
	data: $entries
}

let output = tg build $path
snapshot $output
