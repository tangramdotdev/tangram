use ../../lib/test.nu *

# Errors thrown by the embedded runtime include source-mapped internal stack locations when enabled.

let server = server spawn --config { advanced: { internal_error_locations: true } }

let path = artifact {
	tangram.ts: '
		export default function () {
			tg.assert(false);
		}
	'
}

let process_id = tg build --detach $path | str trim
tg wait $process_id

let process = tg get $process_id | from json
let error = tg get --no-tokens --pretty $process.error
let paths = $error
	| parse --regex '"value": "(?<path>packages/[^"]+)"'
	| get path
	| uniq
	| sort

snapshot $paths '
	packages/clients/js/src/assert.ts
	packages/js/src/start.ts

'
