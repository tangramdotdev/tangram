use ../../test.nu *

# Reading with no references succeeds and outputs nothing.

let server = spawn

let output = tg read | complete
success $output
assert equal $output.stdout "" "the output should be empty"
