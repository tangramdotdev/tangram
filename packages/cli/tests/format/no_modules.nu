use ../../test.nu *

# Formatting a directory that contains no modules succeeds as a no-op.

let server = spawn

let dir = mktemp --directory
'just text' | save ($dir | path join readme.txt)

let output = tg format $dir | complete
success $output
assert equal (open ($dir | path join readme.txt)) "just text" "the file should be unchanged"
