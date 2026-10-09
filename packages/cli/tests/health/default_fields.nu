use ../lib/test.nu *

# The default health reports the database, diagnostics, processes, and version fields.

let local = server spawn

let health = tg health | from json
assert equal ($health | columns) [database diagnostics processes version] "the health should contain all of the fields"
assert equal ($health.processes | columns) [capacity started] "the processes health should contain the capacity and the started count"
assert equal ($health.processes.capacity | columns) [available cpu_oversubscription shared_cpu_limit total] "the capacity should contain the available and total capacity and the CPU sharing configuration"
assert equal $health.processes.capacity.available $health.processes.capacity.total "an idle runner should have all of its capacity available"
assert ($health.processes.capacity.shared_cpu_limit > 0) "the runner should have CPU capacity"
assert equal $health.processes.capacity.total.cpu.dedicated 0 "the runner should not dedicate CPUs by default"
assert equal $health.processes.capacity.total.cpu.shared ($health.processes.capacity.shared_cpu_limit * $health.processes.capacity.cpu_oversubscription) "the runner should offer oversubscribed shared CPU slots"
assert equal $health.processes.capacity.total.memory ($health.processes.capacity.shared_cpu_limit * 1_073_741_824) "the runner should have 1 GiB of memory per CPU by default"
assert equal $health.processes.started 0 "an idle server should have no started processes"
assert ($health.database.available_connections > 0) "the database should report available connections"
assert (($health.version | str length) > 0) "the version should not be empty"
