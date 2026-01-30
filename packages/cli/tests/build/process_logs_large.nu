use ../../test.nu *

# Reproduces a bug where reading a compacted log with mid-entry position repeats endlessly.

let remote = spawn -n remote

let path = artifact {
	tangram.ts: '
		export default () => {
			console.log("Line 000 content here");
			console.log("Line 001 content here");
		};
	'
}

let id = tg build -d $path | str trim
tg wait $id
tg remote put default $remote.url | complete
tg push --logs $id

# Read from remote blob starting mid-entry. Should not hang or repeat.
let output = tg --url $remote.url process log --position 5 $id o+e>| complete

# Output should be ~37 bytes, not infinite. The bug causes endless repetition.
assert (($output.stdout | str length) < 100) "Log should not repeat"
