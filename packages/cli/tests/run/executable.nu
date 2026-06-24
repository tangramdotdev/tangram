use ../../test.nu *

# The --executable flag selects the executable to invoke across the run, exec, process exec, build, and spawn commands, including overriding the executable of a directory or builder artifact.

let server = spawn

let path = artifact {
	hello: (file --executable '
		#!/bin/sh
		echo "hello $1"
	')
	goodbye: (file --executable '
		#!/bin/sh
		echo "goodbye $1"
	')
}
let directory = tg checkin $path | str trim
let executable = tg checkin ($path | path join "hello") | str trim
let replacement = tg checkin ($path | path join "goodbye") | str trim

let output = tg run --executable $executable --arg-string run | complete
success $output
assert (($output.stdout | str trim) == "hello run")

let output = tg run --executable /bin/sh -- -c "echo trailing" | complete
success $output
assert (($output.stdout | str trim) == "trailing")

let output = tg exec --executable $executable --arg-string exec | complete
success $output
assert (($output.stdout | str trim) == "hello exec")

let output = tg process exec --executable $executable --arg-string process-exec | complete
success $output
assert (($output.stdout | str trim) == "hello process-exec")

let output = tg build --executable $executable --arg-string build | complete
success $output

let output = tg run $directory --executable hello --arg-string joined | complete
success $output
assert (($output.stdout | str trim) == "hello joined")

let builder = artifact {
	tangram.ts: '
		export default function () {
			const script = tg.file("#!/bin/sh\necho \"built $1\"", { executable: true });
			return tg.directory({ hello: script });
		}
	'
}

let output = tg run --build $builder --executable hello --arg-string joined | complete
success $output
assert (($output.stdout | str trim) == "built joined")

let output = tg run --build $builder --executable $replacement --arg-string replaced-build | complete
success $output
assert (($output.stdout | str trim) == "goodbye replaced-build")

let output = tg run ($path | path join "hello") --executable $replacement --arg-string replaced | complete
success $output
assert (($output.stdout | str trim) == "goodbye replaced")

let process = tg spawn --executable $executable --arg-string spawn | str trim
let output = tg wait $process | from json
assert ($output.exit == 0)
