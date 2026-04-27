use ../../test.nu *

let server = spawn --config {
	runner: {
		concurrency: 1,
	},
}

# Spawn long-running builds and immediately cancel each. Eventually fresh builds begin to hang.

let long = artifact {
	tangram.ts: '
		export default async (tag: string) => {
			await tg.run`sleep 60; echo ${tag}`.sandbox();
			return "done";
		};
	',
}

for i in 0..20 {
	let process = tg build -dv $"($long)#default" -a $"iter-($i)" | from json
	tg cancel $process.process $process.token
}

let short = artifact {
	tangram.ts: '
		export default () => "hello";
	',
}

let output = timeout 15s tg build $short | complete
success $output "build should not hang after repeated cancels"
