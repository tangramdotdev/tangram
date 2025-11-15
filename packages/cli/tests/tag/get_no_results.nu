use ../../test.nu *

let server = spawn

let pattern = "test"
let output = tg tag get $pattern | complete
failure $output "the tag get should fail when the tag does not exist"
