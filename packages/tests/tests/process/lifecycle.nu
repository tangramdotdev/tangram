use ../../util/test.nu *
use ../../util/lifecycle.nu *

# Every process method can be called at every stage of a process's lifecycle.

let local = server spawn --config {
	runner: { cpus: 1, overcommit: { cpu: 1 } },
	scheduler: { default_cpu: 1, default_memory: 1_073_741_824 },
}

let commands = lifecycle_commands
process_lifecycle $commands
