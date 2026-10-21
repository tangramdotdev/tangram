use ../../lib/test.nu *

# Checking out a directory with more entries than fit in a directory leaf, forcing it into a branch directory, writes all of the entries into the checkouts directory.

let local = server spawn

# Create a directory with 1,025 entries, one more than fit in a directory leaf.
let entries = 0..<1_025 | each { |i| { name: $'($i | into string | fill --alignment right --character "0" --width 4).txt', contents: $'File ($i)' } } | transpose --header-row --as-record
let path = artifact $entries

let id = tg checkin --no-tokens $path | referent node

# Check out the branch directory.
tg checkout $id

# Verify that all of the files are present in the checkouts directory and that the files in each leaf have the correct contents.
let checkout_path = $local.checkout_directory | path join $id
assert equal (ls $checkout_path | length) 1_025 'the checkout must contain every entry'
assert equal (open ($checkout_path | path join '0000.txt')) 'File 0' 'the first file must have the correct contents'
assert equal (open ($checkout_path | path join '1024.txt')) 'File 1024' 'the last file must have the correct contents'
