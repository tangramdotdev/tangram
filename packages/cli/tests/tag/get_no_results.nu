use ../../test.nu *

let server = spawn

let pattern = "test"
let output = tg tag get $pattern | complete

failure $output "The command should fail when the tag does not exist."
