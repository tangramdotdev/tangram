use ../../util/test.nu *

# Pulling a cycle whose graph is split into leaves and a branch pulls every leaf, and the pulled artifacts can be checked out and built.

let remote = server spawn --name remote
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url } },
}

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

# Check in the packages on the remote.
let a = tg --url $remote.url checkin --no-tokens ($path | path join a) | referent node
let b = tg --url $remote.url checkin --no-tokens ($path | path join b) | referent node
tg --url $remote.url index

# Pull the packages.
tg --url $local.url pull $a
tg --url $local.url pull $b

# Check out the directory from the local server.
let checkout = mktemp -d | path join checkout
tg --url $local.url checkout $a --path $checkout
assert equal (ls $checkout | length) 2_001 'the checkout must contain every entry'
assert equal (open ($checkout | path join file_1999.txt)) 'File 1999' 'the checkout must have the correct contents'

# Build the other package on the local server.
let output = tg --url $local.url build $b
assert equal $output '"2001,File 1999"' 'the build must iterate and read the directory'
