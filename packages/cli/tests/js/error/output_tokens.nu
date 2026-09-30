use ../../lib/test.nu *

# Error output carries one proof per module, independent of the number of stack frames.

let server = server spawn
let path = artifact {
	tangram.ts: '
		export default function () {
			Error.stackTraceLimit = 128;
			function recurse(depth) {
				if (depth === 0) { throw new Error("boom"); }
				const result = recurse(depth - 1);
				return result;
			}
			recurse(100);
		}
		export function module() { return import.meta.module; }
	'
}
let module = tg build $'($path)#module'
let path = mktemp
let engine = if ($env.TANGRAM_TEST_QUICKJS? | default '') != '' { 'quickjs' } else { 'v8' }
let output = with-env { TANGRAM_OUTPUT: $path } { tg js --engine $engine --export default $module } | complete
failure $output
let json = xattr_list $path
	| where { |name| $name == 'user.tangram.error' or ($name starts-with 'user.tangram.error.') }
	| sort --natural
	| each { |name| xattr_read $name $path }
	| str join
let error = $json | from json
assert equal $error.message boom
let frames = $error.stack | where file.kind == module
assert (($frames | length) > 10)
assert ($frames | all { |frame| $frame.file.value.referent.options?.tokens? | default {} | is-empty })
assert equal ($error.children | length) 1
let child = $error.children | first
let tokens = $child.options.tokens.local
assert equal ($tokens | length) 1
assert equal ($json | split row ($tokens | first) | length) 2
assert equal $frames.0.file.value.referent.node $child.node
