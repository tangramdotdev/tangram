use ../../lib/test.nu *

# Putting a tg.directory value creates the same directory as checking in the same entries, splitting a directory with more entries than fit in a leaf into branches.

let local = server spawn

# Check in a directory with 1,025 files.
let entries = 0..<1_025 | each { |i| { name: ($i | into string), contents: ($i | into string) } } | transpose --header-row --as-record
let expected = tg checkin --no-tokens (artifact $entries) | referent node

# Put the same directory as a value.
let entries = 0..<1_025 | each { |i| $'"($i)":' + 'tg.file("' + ($i | into string) + '")' } | str join ','
let actual = tg put --no-tokens ('tg.directory({' + $entries + '})') | referent node
assert equal $actual $expected 'putting tg.directory must create the same directory as checkin'
