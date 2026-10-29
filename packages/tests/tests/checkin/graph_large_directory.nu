use ../../lib/test.nu *

# Checking in a cycle that contains a directory with more entries than fit in a directory leaf splits the directory into nodes of the graph, and the directory can still be checked out, iterated, and read.

let local = server spawn

# Create a package with 2,000 files that is in a cycle with a package that imports it as a directory.
let files = 0..<2_000 | each { |i| { name: $'file_($i | into string | fill --alignment right --character "0" --width 4).txt', contents: $'File ($i)' } } | transpose --header-row --as-record
let path = artifact {
	a: ($files | merge {
		tangram.ts: '
			import b from "b" with { source: "../b" };
			export default b;
		'
	})
	b: {
		tangram.ts: '
			import directory from "../a" with { type: "directory" };
			export default async function () {
				let count = 0;
				for await (let _ of directory) {
					count += 1;
				}
				let file = await directory.get("file_1999.txt");
				tg.File.assert(file);
				return `${count},${await file.text}`;
			}
		'
	}
}

# Check in the package, which requires splitting the directory, because a single node with 2,001 entries would have more than 1,024 children.
let id = tg checkin --no-tokens ($path | path join a) | referent node

# Check out the package.
let checkout = mktemp -d | path join checkout
tg checkout $id --path $checkout
assert equal (ls $checkout | length) 2_001 'the checkout must contain every entry'
assert equal (open ($checkout | path join file_1999.txt)) 'File 1999' 'the checkout must have the correct contents'

# Build the other package, which iterates and reads the directory.
let output = tg build ($path | path join b)
assert equal $output '"2001,File 1999"' 'the build must iterate and read the directory'
