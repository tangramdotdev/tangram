use ../../util/test.nu *

# A watched checkin of a file that imports its own directory, where its lockfile is, succeeds when nothing changed and its tag dependency has a source import.

let local = server spawn --config {
	advanced: {
		checkpoints: true
	}
}

let path = artifact {
	dependencies: {
		b.tg.ts: ''
		a.tg.ts: '
			import "b" with { source: "./b.tg.ts" };
		'
	}
	package: {
		main.tg.ts: '
			import "a";
			import "." with { type: "directory" };
		'
	}
}
tg tag b ($path | path join dependencies b.tg.ts)
tg tag a ($path | path join dependencies a.tg.ts)

let module_path = $path | path join package main.tg.ts
tg checkin --no-tokens --watch --lock=file $module_path | ignore

# Hold the next watched checkin after it writes its lock and before it publishes the watch.
let publish_watch = (
	tg checkpoint watch checkin.watch.publish --params ({ path: $module_path } | to json)
	| from json
	| get watch
)
let checkin = job spawn {
	let job_id = job id
	let output = tg checkin --no-tokens --watch $module_path | complete
	$output | job send --tag $job_id 0
}
tg checkpoint wait checkin.watch.publish $publish_watch 0 | ignore

# Synchronously deliver the event of the lockfile, which the checkin's own lock write causes.
tg watch touch $module_path ($path | path join package main.tg.lock)

tg checkpoint continue checkin.watch.publish $publish_watch 0
tg checkpoint unwatch checkin.watch.publish $publish_watch
let output = job recv --tag $checkin --timeout 10sec
success $output "the watched checkin should succeed"
