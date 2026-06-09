use ../../test.nu *

# The fields flag restricts the health to the requested comma separated fields.

let server = spawn

let health = tg health --fields processes,version | from json
assert equal ($health | columns) [processes version] "only the requested fields should be returned"

let health = tg health --fields version | from json
assert equal ($health | columns) [version] "a single requested field should be returned alone"
