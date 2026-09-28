use ../lib/test.nu *

# Reading an empty blob succeeds and outputs nothing.

let local = server spawn

let blob = "" | tg write | str trim

let output = tg read $blob | complete
success $output
assert equal $output.stdout "" "the empty blob should read as empty output"
