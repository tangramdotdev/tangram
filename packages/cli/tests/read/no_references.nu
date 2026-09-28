use ../lib/test.nu *

# Reading with no references succeeds and outputs nothing.

let local = server spawn

let output = tg read | complete
success $output
assert equal $output.stdout "" "the output should be empty"
