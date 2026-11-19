use ../../util/test.nu *

# A process token receives no health fields, even when requesting them explicitly.

let local = server spawn

let path = artifact {
	tangram.ts: '
		export default function () {
			return tg.run`
				tg health > "$TANGRAM_OUTPUT"
				tg health --fields database,diagnostics,processes,version >> "$TANGRAM_OUTPUT"
			`
				.sandbox()
				.then(tg.File.expect);
		}
	'
}
let health = tg build $path | str trim | tg cat $in | lines | each {|line| $line | from json }
assert equal $health [{} {}] "both requests should return an empty object"
