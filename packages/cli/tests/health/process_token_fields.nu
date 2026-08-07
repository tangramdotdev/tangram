use ../../test.nu *

# A process may request only the diagnostics and version health fields from its sandbox.

let server = spawn

let full = artifact {
	tangram.ts: '
		export default function () {
			return tg.run`
				if tg health > /dev/null 2> "$TANGRAM_OUTPUT"; then
					exit 1
				fi
			`
				.sandbox()
				.then(tg.File.expect);
		}
	'
}
let output = tg build $full | str trim | tg cat $in
assert ($output | str contains "unauthorized")

let partial = artifact {
	tangram.ts: '
		export default function () {
			return tg.run`tg health --fields diagnostics,version > "$TANGRAM_OUTPUT"`
				.sandbox()
				.then(tg.File.expect);
		}
	'
}
let health = tg build $partial | str trim | tg cat $in | from json
assert equal ($health | columns) [diagnostics version] "the allowed fields should be returned"
