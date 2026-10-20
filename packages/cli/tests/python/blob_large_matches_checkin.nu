use ../lib/test.nu *

# tg.file in Python with more contents than fit in a blob leaf creates the same file as checking in the same contents, chunking the contents like checkin.

let local = server spawn

# Check in a file with about 2 MB of contents.
let path = artifact { data: (0..<300_000 | each { into string } | str join (char newline)) }
let expected = tg checkin --no-tokens ($path | path join data) | referent node

# Create the same file with tg.file.
let path = artifact {
	'test.tg.py': '
		async def default():
			return await tg.file("\n".join(str(i) for i in range(300000)))
	'
}
let actual = tg build --no-tokens ($path | path join test.tg.py) | referent node
assert equal $actual $expected 'tg.file must create the same file as checkin'
