use ../../test.nu *

# Reading multiple references concatenates their contents in argument order.

let server = spawn

let one = "one " | tg write | str trim
let two = "two" | tg write | str trim

let contents = tg read $one $two
assert equal $contents "one two" "the contents should be concatenated in order"
