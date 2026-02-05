use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			while (true) {
				await tg.sleep(100);
			}
		};
	'
}

# Test that cancellation works normally.
let process = tg build -dv $path | from json
tg cancel $process.process $process.token
let output = tg wait $process.process | complete
let output = $output.stdout | str trim | from json
snapshot $output.error.message 'the process was canceled'

# Test that cancellation works normally.
let id = job spawn {
	tg build $path | complete
};
job kill $id

let output = tg wait $process.process | complete
let output = $output.stdout | str trim | from json
snapshot $output.error.message 'the process was canceled'

# Note: this test hangs because JS processes wait for their children to complete.
# let path = artifact {
# 	tangram.ts: '
# 		export default async () => {
# 			await Promise.race([
# 				tg.build(child), 
# 				tg.sleep(0)),
# 			]);
# 			return 42;		 
# 		};
#
# 		export let child = async () => {
# 			while (true) {
# 				await tg.sleep(100);
# 			}
# 		}
# 	'
# }
# let id = tg build $path | complete
# let children = tg process children $id | complete
# print $children
# snapshot $children ''
