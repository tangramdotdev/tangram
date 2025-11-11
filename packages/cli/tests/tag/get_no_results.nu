use std assert
use ../../test.nu *

let server = spawn

let pattern = "test"
let output = tg tag get $pattern | complete

assert ($output.exit_code != 0) 'should fail when tag does not exist'
