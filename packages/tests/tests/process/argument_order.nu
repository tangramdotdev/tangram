use ../../util/test.nu *

let _ = server spawn
let path = artifact {
	tangram.ts: 'export default (...args: Array<tg.Value>) => args;'
}

let output = tg run $path -a first -A '42' '--arg-string=-x' -a --help -- -A after | complete
success $output
assert equal ($output.stdout | from json) [first 42 '-x' '--help' '-A' after]

let output = tg build $path '--arg-value=true' --arg-string 'true' -A '42' | complete
success $output
assert equal ($output.stdout | from json) [true 'true' 42]

let output = tg run $path first -- -A after | complete
success $output
assert equal ($output.stdout | from json) [first '--' '-A' after]
