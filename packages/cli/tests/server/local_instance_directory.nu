use ../lib/test.nu *

# A local instance owns the directory used by its server.

let instance = instance
let local_first = server spawn --instance $instance --name local-first
assert equal $local_first.directory $instance.directory
server stop $local_first

let local_second = server spawn --instance $instance --name local-second
assert equal $local_second.directory $instance.directory
