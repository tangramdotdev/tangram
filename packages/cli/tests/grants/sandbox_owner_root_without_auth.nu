use ../../test.nu *

# With authentication disabled, the root principal may create a sandbox owned by root.

let server = spawn

let sandbox = tg sandbox create --owner root --no-network | str trim
let data = tg sandbox get $sandbox | from json
assert equal $data.owner "root" "a root principal should create a root-owned sandbox when authentication is disabled"
