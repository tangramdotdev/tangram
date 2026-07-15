use ../../test.nu *

# Rapidly canceling many long-running builds under a single-concurrency runner
# does not prevent a subsequent build from completing.
#
# Regression test added in cd5bbb68.

let server = spawn --config {
	runner: {
		permits: 1,
	},
}

# Spawn long-running builds and immediately cancel each.

let long = artifact {
	tangram.ts: '
		export default async function (tag: string) {
			await tg.run`sleep 60; echo ${tag}`.sandbox();
			return "done";
		}
	',
}

for i in 0..20 {
	let process = tg build --detach --verbose $"($long)#default" --arg-string $"iter-($i)" | from json
	tg cancel $process.process $process.lease
}

let short = artifact {
	tangram.ts: '
		export default function () { return "hello"; }
	',
}

let output = timeout 15s tg build $short | complete
success $output "build should not hang after repeated cancels"
