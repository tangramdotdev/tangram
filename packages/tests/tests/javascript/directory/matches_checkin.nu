use ../../../util/test.nu *

# tg.directory creates the same directory as checking in the same entries, splitting a directory with more entries than fit in a leaf into branches.

let local = server spawn

# Check in a directory with 1,025 files.
let entries = 0..<1_025 | each { |i| { name: ($i | into string), contents: ($i | into string) } } | transpose --header-row --as-record
let expected = tg checkin --no-tokens (artifact $entries) | referent node

# Create the same directory with tg.directory.
let path = artifact {
	tangram.ts: '
		export default async function () {
			let entries: Record<string, tg.File> = {};
			for (let i = 0; i < 1025; i++) {
				entries[String(i)] = await tg.file(String(i));
			}
			return tg.directory(entries);
		}
	'
}
let actual = tg build --no-tokens $path | referent node
assert equal $actual $expected 'tg.directory must create the same directory as checkin'
