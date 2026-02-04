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
let id = tg build -d $path | str trim
sleep 1sec

# Check that we can read just one chunk forwards
let output = tg log $id --position 0 --length 20 
snapshot $output 'aaaaaaaaaaaaaaaaaaaa'

# Check that we can read chunks backwards.
let output = tg log $id --position 41 --length='-20'
snapshot $output 'bbbbbbbbbbbbbbbbbbbb'
