use ../../test.nu *

# When a child finishes, the runner aborts its control handler and then drops the
# child from its state. The aborted handler is not cancelled until it next yields,
# so it keeps answering the control requests already queued for it, and by then
# the state is gone. A wait whose control request lands in that window gets
# "failed to find the process" and the parent build fails.
#
# Run with --release and --stress: the window closes in a debug build.

let server = spawn --busybox --config {
	tokio_single_threaded: false,
	indexer: {
		database_outbox_wakeup_interval: 2.0,
		object_outbox_wakeup_interval: 2.0,
		poll_interval: 2.0,
	},
}

let path = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export async function leaf(...args: Array<{ i: string }>) {
			let { i } = args[0]!;
			return await tg.build`echo "${i}" > ${tg.output}`.env(tg.build(busybox));
		}
		export default async function () {
			return await Promise.all(
				Array.from({ length: 128 }, (_, i) => tg.build(leaf, { i: String(i) })),
			);
		}
	'
}

let output = tg build $path | complete
success $output
