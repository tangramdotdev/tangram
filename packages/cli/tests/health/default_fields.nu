use ../../test.nu *

# The default health reports the database, diagnostics, processes, and version fields.

let server = spawn

let health = tg health | from json
assert equal ($health | columns) [database diagnostics processes version] "the health should contain all of the fields"
assert equal ($health.processes | columns) [created permits started] "the processes health should contain the counts and the permits"
assert equal $health.processes.created 0 "an idle server should have no created processes"
assert equal $health.processes.started 0 "an idle server should have no started processes"
assert ($health.database.available_connections > 0) "the database should report available connections"
assert (($health.version | str length) > 0) "the version should not be empty"
