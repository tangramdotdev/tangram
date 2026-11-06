use test.nu *

# Call the methods that read a process, which answer at every stage.
def process_reads [process: string, stage: string] {
	success (tg get $process | complete) $'get should answer when ($stage)'
	success (tg process status $process | complete) $'status should answer when ($stage)'
	success (tg process status --timeout 100ms $process | complete) $'status with a timeout should end when ($stage)'
	success (tg process children $process | complete) $'children should answer when ($stage)'
	success (tg process children --timeout 100ms $process | complete) $'children with a timeout should end when ($stage)'
	success (tg process log $process | complete) $'log should answer when ($stage)'
	success (tg process metadata $process | complete) $'metadata should answer when ($stage)'
	success (tg process availability $process | complete) $'availability should answer when ($stage)'
	success (tg process touch $process | complete) $'touch should answer when ($stage)'
}

# Call the methods that read a sandbox, which answer at every stage.
def sandbox_reads [sandbox: string, stage: string] {
	success (tg sandbox get $sandbox | complete) $'get should answer when ($stage)'
}

# Spawn a process into a sandbox.
def spawn_into [sandbox: string, command: string] {
	tg run --no-tokens --cached=false $'--sandbox=($sandbox)' $command | complete
}

# Build the commands that the lifecycle tests spawn.
export def --env lifecycle_commands [] {
	let path = artifact {
		tangram.ts: '
			export function hello() {
				return tg.command({
					args: ["-ec", "echo hello"],
					executable: "/bin/sh",
					host: tg.host.current,
				});
			}

			export function spin() {
				return tg.command({
					args: ["-ec", "while :; do :; done"],
					executable: "/bin/sh",
					host: tg.host.current,
				});
			}
		',
	}
	let hello = tg build $'($path)#hello' | str trim
	let spin = tg build $'($path)#spin' | str trim

	{ hello: $hello, spin: $spin }
}

# Call every process method at every stage of a process's lifecycle, on a runner that fits one sandbox.
export def --env process_lifecycle [commands: record] {
	let spin = $commands.spin
	# Fill the runner, so that the process waits in the queue.
	let sandbox = tg sandbox create --wait --no-tokens --cpu 1 | referent node
	let spawned = tg process spawn --no-tokens --verbose --sandbox=true --network $spin | from json
	let process = $spawned.process

	# The process is starting.
	assert equal (tg get $process | from json | get status) starting
	process_reads $process starting

	# The process is started.
	tg sandbox destroy --wait $sandbox
	loop {
		if (tg get $process | from json | get status) == started { break }
		sleep 100ms
	}
	process_reads $process started

	# The process is finished.
	success (tg process signal --signal KILL $process | complete) 'signal should kill the process'
	let outcome = tg wait $process | from json
	assert ($outcome.exit != 0)
	process_reads $process finished
	failure (tg process output $process | complete) 'output should report the failure when finished'
	success (tg process signal --signal KILL $process | complete) 'signal should have no effect when finished'
	success (tg process cancel $process $spawned.lease | complete) 'cancel should have no effect when finished'
}

# Call every sandbox method at every stage of a sandbox's lifecycle, on a runner that fits one sandbox.
export def --env sandbox_lifecycle [commands: record] {
	let hello = $commands.hello
	# Fill the runner, so that the sandbox waits in the queue.
	let filler = tg sandbox create --wait --no-tokens --cpu 1 | referent node
	let sandbox = tg sandbox create --no-tokens --cpu 1 | referent node

	# The sandbox is starting. A start is already satisfied, and a spawn waits for the sandbox.
	assert equal (tg sandbox get $sandbox | from json | get data.status) starting
	sandbox_reads $sandbox starting
	success (tg sandbox start $sandbox | complete) 'start should have no effect when starting'
	let spawn = job spawn {
		let job_id = job id
		spawn_into $sandbox $hello | job send --tag $job_id 0
	}

	# The sandbox is started, and the spawn proceeds.
	tg sandbox destroy --wait $filler
	let output = job recv --tag $spawn --timeout 30sec
	success $output 'a spawn should wait for the sandbox to start'
	assert ($output.stdout | str contains 'hello')
	assert equal (tg sandbox get $sandbox | from json | get data.status) started
	sandbox_reads $sandbox started
	success (tg sandbox start $sandbox | complete) 'start should have no effect when started'
	let output = spawn_into $sandbox $hello
	success $output 'a spawn should run when started'

	# The sandbox is stopped. A spawn fails, because it needs a start.
	success (tg sandbox stop --wait $sandbox | complete) 'stop should stop the sandbox'
	assert equal (tg sandbox get $sandbox | from json | get data.status) stopped
	sandbox_reads $sandbox stopped
	success (tg sandbox stop $sandbox | complete) 'stop should have no effect when stopped'
	failure (spawn_into $sandbox $hello) 'a spawn should fail when stopped'

	# The sandbox can be started again.
	success (tg sandbox start --wait $sandbox | complete) 'start should start the sandbox'
	assert equal (tg sandbox get $sandbox | from json | get data.status) started
	success (spawn_into $sandbox $hello) 'a spawn should run after a restart'

	# The sandbox is destroyed, so a destroy has no effect, and anything else but a read fails.
	success (tg sandbox destroy --wait $sandbox | complete) 'destroy should destroy the sandbox'
	assert equal (tg sandbox get $sandbox | from json | get data.status) destroyed
	sandbox_reads $sandbox destroyed
	success (tg sandbox destroy $sandbox | complete) 'destroy should have no effect when destroyed'
	failure (tg sandbox stop $sandbox | complete) 'stop should fail when destroyed'
	failure (tg sandbox start $sandbox | complete) 'start should fail when destroyed'
	failure (spawn_into $sandbox $hello) 'a spawn should fail when destroyed'
}
