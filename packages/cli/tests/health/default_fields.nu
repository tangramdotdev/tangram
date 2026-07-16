use ../../test.nu *

# The default health reports the database, diagnostics, processes, and version fields.

let server = spawn

let health = tg health | from json
assert equal ($health | columns) [database diagnostics processes version] "the health should contain all of the fields"
assert equal ($health.processes | columns) [capacity started] "the processes health should contain the capacity and the started count"
assert equal ($health.processes.capacity | columns) [available total] "the capacity should contain the available and total capacity"
assert equal $health.processes.capacity.available $health.processes.capacity.total "an idle runner should have all of its capacity available"
assert ($health.processes.capacity.total.cpus > 0) "the runner should have CPU capacity"
assert equal $health.processes.capacity.total.memory ($health.processes.capacity.total.cpus * (1e9 | into int)) "the runner should have 1 GB of memory per CPU by default"
assert equal $health.processes.started 0 "an idle server should have no started processes"
assert ($health.database.available_connections > 0) "the database should report available connections"
assert (($health.version | str length) > 0) "the version should not be empty"
