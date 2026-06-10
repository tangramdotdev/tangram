use ../../test.nu *

# tg log can read a process's log output by position and length, both forwards and backwards.

let server = spawn
let path = artifact {
	tangram.ts: r#'
		export default async () => {
			let alphabet = "abcdefghijklmnopqrstuvwxyz";
			for (let i = 0; i < 26; i++) {
				let s = "";
				for (let j = 0; j < 20; j++) {
					s = s + alphabet[i];
				}
				console.log(s);
			}
			while(true) {
				await tg.sleep(100);
			}
		};
	'#
}
let process = tg build --detach --verbose $path | from json

# Wait for the first two log lines to be written.
wait_until { (tg log $process.process --position 21 --length 20) == 'bbbbbbbbbbbbbbbbbbbb' } "the first two log lines should be written"

# Check that we can read just one chunk forwards
let output = tg log $process.process --position 0 --length 20
snapshot $output 'aaaaaaaaaaaaaaaaaaaa'

# Check that we can read chunks backwards.
let output = tg log $process.process --position 41 --length='-20'
snapshot $output 'bbbbbbbbbbbbbbbbbbbb'

# Cancel the process.
tg cancel $process.process $process.lease
tg wait $process.process
