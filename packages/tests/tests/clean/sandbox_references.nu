use ../../util/test.nu *

# A sandbox retains its processes, but a tagged process does not retain its destroyed sandbox.

let local = server spawn

let path = artifact {
	tangram.ts: '
		export default function () {
			return tg.file("output");
		}
	'
}

let process = tg process spawn --no-tokens --sandbox $path | referent node
tg wait $process
tg index

let sandbox = tg process get $process | from json | get sandbox
tg wait $sandbox
tg index

# A tagged process survives cleanup independently of its sandbox.
tg tag process $process
tg clean
tg index
failure (tg sandbox get $sandbox | complete)
tg index
tg process get $process | ignore

# Removing the tag releases the remaining process.
tg tag delete process
tg clean
tg index
failure (tg process get $process | complete)

# An existing sandbox retains an otherwise unreferenced process.
let sandbox = tg sandbox create --no-tokens | referent node
let process = tg process spawn --no-tokens $'--sandbox=($sandbox)' $path | referent node
tg wait $process | ignore
tg index
tg clean
tg index
tg process get $process | ignore

# Destroying and cleaning the sandbox releases its process.
tg sandbox destroy --wait $sandbox
tg sandbox wait $sandbox | ignore
tg index
tg clean
tg index
failure (tg sandbox get $sandbox | complete)
tg index
failure (tg process get $process | complete)
