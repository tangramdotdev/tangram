use ../../util/test.nu *

# tg.file in Python with contents larger than the minimum blob leaf size but smaller than the maximum creates the same file as checking in the same contents, chunking the contents like checkin.

let local = server spawn

# Check in a file with about 100 KB of contents.
let path = artifact { data: (0..<20_000 | each { into string } | str join (char newline)) }
let expected = tg checkin --no-tokens ($path | path join data) | referent node

# Create the same file with tg.file.
let path = artifact {
	'test.tg.py': '
		async def default():
			return await tg.file("\n".join(str(i) for i in range(20000)))
	'
}
let actual = tg build --no-tokens ($path | path join test.tg.py) | referent node
assert equal $actual $expected 'tg.file must create the same file as checkin'
