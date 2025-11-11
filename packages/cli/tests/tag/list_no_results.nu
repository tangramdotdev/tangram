use std assert
use ../../test.nu *

let server = spawn

let pattern = "test"
let output = tg tag list $pattern

assert (snapshot -n output $output)
