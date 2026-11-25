use ../../util/test.nu *

# A process that leaves a writer with a writable file descriptor to its output after the output is checked in cannot change the checked-in artifact.

let server = server spawn --busybox --config { vfs: false }
let path = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default () => tg.command`
			mkdir -p ${tg.output}
			echo original > ${tg.output}/file.txt
			setsid sh -c "exec 3>>${tg.output}/file.txt; echo ready > /fifo/ready; read x < /fifo/go; echo tampered >&3; exec 3>&-; echo done > /fifo/done" </dev/null >/dev/null 2>&1 &
			read x < /fifo/ready
		`.env(tg.build(busybox));
	'
}
let command = tg build --no-tokens $path | str trim

# Create the FIFOs through which the process waits for its writer to escape its group, and the test tells the writer to write and waits for it to finish.
let directory = mktemp -d
let done = $directory | path join done
let go = $directory | path join go
let ready = $directory | path join ready
^mkfifo $done $go $ready

# Run the process in a sandbox that outlives it, so that its writer, which escapes the process's group, keeps running.
let sandbox = tg sandbox create --wait --no-tokens --no-time-to-live --no-time-to-stop --mount $'($directory):/fifo' | referent node
let output = tg run --no-tokens $'--sandbox=($sandbox)' $command | complete
success $output
let id = $output.stdout | str trim | lines | last

# Let the writer write to the output, and wait for it to finish.
^sh -c 'echo go > "$1"' sh $go
success (timeout 30s cat $done | complete) 'the writer should finish'

# The artifact's contents are unchanged.
let checkout = tg checkout $id | str trim
assert equal (open --raw ($checkout | path join file.txt)) "original\n"
tg sandbox destroy $sandbox
