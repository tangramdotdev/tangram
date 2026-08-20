use ../../test.nu *

# The availability flag prints availability alongside object and process get output.

let server = spawn

let file = tg put 'tg.file("available")' | str trim
let path = artifact {
	tangram.ts: 'export default function () { return tg.file("available"); }'
}
let build = tg build --detach --verbose $path | from json
tg wait $build.process
tg index

let object = with-env { TANGRAM_QUIET: "false" } { tg get $file --availability | complete }
success $object
assert ($object.stdout | str starts-with "tg.file(") "the object value should print to stdout"
assert ($object.stderr | str contains '{"subtree":true}') "the object's availability should print as an info message"

let process = with-env { TANGRAM_QUIET: "false" } { tg process get $build.process --availability | complete }
success $process
assert (($process.stdout | from json).status == "finished") "the process data should print to stdout"
assert ($process.stderr | str contains '"subtree":true') "the process's availability should print as an info message"
