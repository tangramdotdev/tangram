use ../../test.nu *

# tg id converts an id string to its binary form and back without requiring a server.

let id = "fil_01sa3pyv7baf50x2ymmvy7p41zqnmmv8gp1fq5z3mq60ps8vcfxa30"

# An id argument is converted to its binary form.
let binary = tg id $id | into binary
assert equal ($binary | bytes length) 36 "the binary form should be 36 bytes"

# Binary input on stdin is converted back to the id string.
let back = $binary | tg id
assert equal $back $id "the roundtrip should return the original id"

# String input on stdin behaves the same as an argument.
let from_stdin = $id | tg id | into binary
assert equal $from_stdin $binary "stdin input should match the argument form"
