use ../lib/test.nu *
use ../lib/command.nu

# The top-level children command gets the direct graph children of any node.

let server = server spawn

let path = artifact {
	tangram.ts: 'export default function () { return tg.file("dispatch"); }'
}
let build = tg build --detach --verbose $path | from json
tg wait $build.process
tg log $build.process --position end.0 --no-timeout o+e>| ignore
tg index

# A path reference resolves to object children, a list of ids.
let object_children = tg children $path | from json
assert (($object_children | get 0) | str starts-with "fil_") "the path reference should resolve to object children"

# A process includes its command, log, and output object.
let process_children = tg children $build.process | from json
let process = tg get --no-tokens $build.process | from json
let expected = [
	$process.log
	$process.output.value
	(command module-input $process.command)
]
assert equal $process_children $expected "the process should include its direct graph children"
