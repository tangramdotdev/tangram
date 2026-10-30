use ../../util/test.nu *

# Building with echo writes the process's logged stdout and stderr to stdout and stderr while it runs, and replays them for a cache hit.

let local = server spawn
let path = artifact {
	tangram.ts: '
		export default () => {
			console.log("to stdout");
			console.error("to stderr");
			return 1;
		};
	'
}
for _ in 1..2 {
	let output = tg build --echo $path | complete
	success $output
	assert ($output.stdout | str contains "to stdout") $output.stdout
	assert ($output.stderr | str contains "to stderr") $output.stderr
}

# Echo cannot be used with a view.
let output = tg build --echo --view inline $path | complete
failure $output

# A process echoes the stdio of the children that it awaits with echo, in TypeScript and in Python.
let path = artifact {
	tangram.ts: '
		export default async () => {
			await tg.build(echoed).echo();
			await tg.build(quiet);
			return "done";
		};
		export let echoed = () => {
			console.log("from the echoed child");
			return 1;
		};
		export let quiet = () => {
			console.log("from the quiet child");
			return 2;
		};
	'
}
let output = tg build --echo $path | complete
success $output
assert ($output.stdout | str contains "from the echoed child") $output.stdout
assert not ($output.stdout | str contains "from the quiet child") $output.stdout
let path = artifact {
	main.tg.py: '
		def echoed():
		    print("from the echoed child")
		    return 1

		def quiet():
		    print("from the quiet child")
		    return 2

		async def default():
		    await tg.build(echoed).echo()
		    await tg.build(quiet)
		    return "done"
	'
}
let output = tg build --echo ($path | path join main.tg.py) | complete
success $output
assert ($output.stdout | str contains "from the echoed child") $output.stdout
assert not ($output.stdout | str contains "from the quiet child") $output.stdout
