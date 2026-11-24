use ../../util/test.nu *

# A process that runs in a sandbox claimed from the pool while the server cleans finishes successfully with its output.

let server = server spawn --config {
	advanced: { checkpoints: true },
	runner: { sandbox_pool_size: 1 },
}

# Create the FIFOs through which the process reports that it started and waits to continue.
let directory = mktemp -d
let started = $directory | path join started
let resume = $directory | path join resume
^mkfifo $started $resume

# Run a process in a new sandbox claimed from the pool, which writes a file in the sandbox before it reports that it started and reads the file after it resumes.
let take = checkpoint watch runner.sandbox.pool.take
let run = job spawn {
	let id = job id
	tg run --no-tokens --sandbox --mount $'($directory):/fifo' --executable /bin/sh -- -c 'echo kept > /tmp/file; echo started > /fifo/started; read -r line < /fifo/resume; read -r file < /tmp/file; echo "$line $file"' | complete | job send --tag $id 0
}
checkpoint pass $take | ignore
checkpoint unwatch $take

# Wait for the process to start, clean while it runs, and then let it resume.
success (timeout 30s cat $started | complete) 'the process should start'
success (tg clean | complete) 'the clean should succeed'
^sh -c 'echo resumed > "$1"' sh $resume

# The process finishes successfully with its output.
let output = job recv --tag $run --timeout 60sec
success $output 'the process should succeed'
assert equal ($output.stdout | str trim) 'resumed kept'
