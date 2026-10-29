use ../../util/test.nu *

# tg.directory in Python creates the same directory as checking in the same entries, splitting a directory with more entries than fit in a leaf into branches.

let local = server spawn

# Check in a directory with 1,025 files.
let entries = 0..<1_025 | each { |i| { name: ($i | into string), contents: ($i | into string) } } | transpose --header-row --as-record
let expected = tg checkin --no-tokens (artifact $entries) | referent node

# Create the same directory with tg.directory.
let path = artifact {
	'test.tg.py': '
		async def default():
			entries = {str(i): await tg.file(str(i)) for i in range(1025)}
			return await tg.directory(entries)
	'
}
let actual = tg build --no-tokens ($path | path join test.tg.py) | referent node
assert equal $actual $expected 'tg.directory must create the same directory as checkin'
