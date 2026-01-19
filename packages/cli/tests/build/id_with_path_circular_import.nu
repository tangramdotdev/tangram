use ../../test.nu *

# Test building from a file with ?path= when there's a cyclical import.

let server = spawn

let path = artifact {
	tangram.ts: '
		import * as file from "./file.tg.ts";
		export default () => file.a();
	',
	file.tg.ts: '
		import root from "./tangram.ts";
		export default () => root();
		export let a = () => 42;	
	',
}
let id = tg checkin $path
let output = tg build $"($id)?path=./file.tg.ts" | complete
success $output
snapshot $output.stdout '
	42

';