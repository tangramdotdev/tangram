use ../../test.nu *
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
let process = tg build -dv $path | from json

sleep 1sec

# Check that we can read just one chunk forwards
let output = tg log $process.process --position 0 --length 20
snapshot $output 'aaaaaaaaaaaaaaaaaaaa'

# Check that we can read chunks backwards.
let output = tg log $process.process --position 41 --length='-20'
snapshot $output 'bbbbbbbbbbbbbbbbbbbb'

# Cancel the process.
tg cancel $process.process $process.token
tg wait $process.process
