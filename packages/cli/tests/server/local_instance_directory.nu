use ../../test.nu *

# A local instance owns the directory used by its server.

let instance = instance
let first = server spawn --instance $instance --name first
assert equal $first.directory $instance.directory
server stop $first

let second = server spawn --instance $instance --name second
assert equal $second.directory $instance.directory
