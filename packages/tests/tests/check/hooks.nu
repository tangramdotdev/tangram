use ../../util/test.nu *

# Check and format run only the selected modules' hooks on the host, every time.

let local = server spawn
let log = mktemp
$env.TANGRAM_HOOK_LOG = $log
$env.TANGRAM_HOOK_VALUE = 'inherited value'
let hook = { |name|
	'
		export const value = 1;
		export const check = async () => {
			await tg.run`printf "%s\\n" "check:LABEL:$TANGRAM_HOOK_VALUE" >> "$TANGRAM_HOOK_LOG"`;
		};
		export const format = async () => {
			await tg.run`printf "%s\\n" "format:LABEL:$TANGRAM_HOOK_VALUE" >> "$TANGRAM_HOOK_LOG"`;
		};
	' | str replace --all LABEL $name
}
let path = artifact {
	tangram.ts: ('import * as helper from "./helper.tg.ts";
' + (do $hook root))
	helper.tg.ts: (do $hook helper)
	child: { tangram.ts: (do $hook child) }
}

# Imports and nested packages do not cause additional hooks to run.
success (tg check $path | complete)
success (tg check $path | complete)
assert equal (open --raw $log | lines) ['check:root:inherited value' 'check:root:inherited value']
success (tg format $path | complete)
assert equal (open --raw $log | lines | last) 'format:root:inherited value'
assert equal (open --raw $log | lines | length) 3

# Explicit module and package arguments each run their own hooks.
success (tg check ($path | path join helper.tg.ts) ($path | path join child) | complete)
success (tg format ($path | path join helper.tg.ts) ($path | path join child) | complete)
assert equal (open --raw $log | lines | skip 3) [
	'check:helper:inherited value'
	'check:child:inherited value'
	'format:helper:inherited value'
	'format:child:inherited value'
]

# Formatting repeats the host hook and propagates its errors.
success (tg format $path | complete)
success (tg format $path | complete)
assert equal (open --raw $log | lines | last 2) ['format:root:inherited value' 'format:root:inherited value']
let failing = artifact { tangram.ts: 'export const format = () => { throw new Error("the format hook failed"); };' }
let output = tg format $failing | complete
failure $output
assert ($output.stderr | str contains 'the format hook failed') $output.stderr
