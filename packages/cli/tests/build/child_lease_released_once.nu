use ../../test.nu *

# Releasing a child lease is idempotent. If a child handle releases its lease
# before parent completion, the parent's cleanup release is a no-op and logs no
# error.

let server = spawn --config { tracing: { stderr_format: 'json' } }

# The two spawns deduplicate to one child holding two leases, so cancelling the
# first leaves the child running until the parent finishes.
let path = artifact {
	tangram.ts: '
		export const slow = async () => {
			await tg.sleep(60);
			return "slow";
		};

		export default async () => {
			let first = await tg.build(slow).spawn();
			await tg.build(slow).spawn();
			await first.cancel();
			return "done";
		};
	'
}

tg build $path | ignore

snapshot (server_errors $server) ''
