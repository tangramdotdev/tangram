use ../../lib/test.nu *

# tg.file with more contents than fit in a blob leaf creates the same file as checking in the same contents, chunking the contents like checkin.

let local = server spawn

# Check in a file with about 290 KB of contents, which spans several blob leaves.
let path = artifact { data: (0..<50_000 | each { into string } | str join (char newline)) }
let expected = tg checkin --no-tokens ($path | path join data) | referent node

# Create the same file with tg.file.
let path = artifact {
	tangram.ts: '
		export default async function () {
			let lines = [];
			for (let i = 0; i < 50000; i++) {
				lines.push(String(i));
			}
			return tg.file(lines.join("\n"));
		}
	'
}
let actual = tg build --no-tokens $path | referent node
assert equal $actual $expected 'tg.file must create the same file as checkin'
