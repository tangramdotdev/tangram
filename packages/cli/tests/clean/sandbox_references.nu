use ../lib/test.nu *

# A sandbox retains its processes, but a tagged process does not retain its destroyed sandbox.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default function () {
			return tg.file("output");
		}
	'
}

let process = tg process spawn --sandbox $path | str trim
tg wait --source=index $process

let sandbox = tg process get $process | from json | get sandbox
tg wait --source=index $sandbox

# A tagged process survives cleanup independently of its sandbox.
tg tag process $process
tg clean
failure (tg sandbox get --source=index $sandbox | complete)
tg process get --source=index $process | ignore

# Removing the tag releases the remaining process.
tg tag delete process
tg clean
failure (tg process get --source=index $process | complete)

# An existing sandbox retains an otherwise unreferenced process.
let sandbox = tg sandbox create | str trim
let process = tg process spawn $'--sandbox=($sandbox)' $path | str trim
tg wait --source=index $process | ignore
tg clean
tg process get --source=index $process | ignore

# Destroying and cleaning the sandbox releases its process.
tg sandbox destroy $sandbox
tg sandbox wait --source=index $sandbox | ignore
tg clean
failure (tg sandbox get --source=index $sandbox | complete)
failure (tg process get --source=index $process | complete)
