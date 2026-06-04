use ../../test.nu *

# tg tag get fails when no tag matches the requested pattern.

let server = spawn

let pattern = "test"
let output = tg tag get $pattern | complete
failure $output "the tag get should fail when the tag does not exist"
