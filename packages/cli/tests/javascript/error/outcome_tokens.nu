use ../../lib/test.nu *

# Serialized process outcomes carry tokens on stack module referents.

let server = server spawn
let path = artifact {
	tangram.ts: '
		export default function () { throw new Error("boom"); }
		export function module() { return import.meta.module; }
	'
}
let module = tg build $'($path)#module'
let path = mktemp
let engine = if ($env.TANGRAM_TEST_QUICKJS? | default '') != '' { 'quickjs' } else { 'v8' }
let output = with-env { TANGRAM_OUTPUT: $path } { tg javascript --engine $engine --export default $module } | complete
failure $output
let outcome = open --raw $path | from json
assert equal $outcome.error.message boom
let frames = $outcome.error.stack | where file.kind == module
assert not ($frames | is-empty)
assert ($frames | all { |frame| not ($frame.file.value.referent.options.tokens | is-empty) })
assert not ('error_children' in $outcome)
assert not ('children' in $outcome.error)
