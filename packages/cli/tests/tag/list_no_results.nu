use ../../test.nu *

# tg list produces empty output when no tag matches the requested pattern.

let server = server spawn

let pattern = "test"
let output = tg match --no-groups $pattern

snapshot --name output $output
