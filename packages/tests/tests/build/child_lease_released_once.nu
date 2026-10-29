use ../../util/test.nu *

# Cancelling one of two handles to a deduplicated child leaves the parent able to finish, and the child is cancelled once the parent finishes.

let local = server spawn

# The two spawns deduplicate to one child holding two leases, so cancelling the first leaves the child running until the parent finishes.
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

let id = tg build --no-tokens --detach $path | referent node
let output = tg output $id | complete
success $output
assert equal ($output.stdout | str trim) '"done"'

# The child is finished with an error after the parent releases its remaining lease.
let child = tg children $id | from json | where $it starts-with pcs_ | get 0
let output = timeout 10s tg wait $child | from json
assert equal $output.exit 1
