use ../../test.nu *

# Test that the watchdog does not cancel a locally-executing process whose
# token_count reaches 0 because a remote cache hit drained its only token.

let remote = spawn -n remote -u "http://localhost:8473" -c { tokio_single_threaded: false }

# A build that takes long enough for the watchdog to fire.
let pkg = artifact {
	tangram.ts: '
		export default async () => {
			await tg.sleep(3);
			return tg.directory({ "result.txt": tg.file("done") });
		};
	'
}

# Build on the remote server so it has the finished process.
let process_id = tg build -d $pkg | str trim
tg wait $process_id
tg index

# Start a fresh server connected to the remote.
let fresh = spawn -n fresh -c { tokio_single_threaded: false }
tg remote put default $remote.url

# Build the same package on the fresh server. The spawn race should be won
# by the remote, cancelling the creator's token. The watchdog must not
# cancel the locally-started process task.
let fresh_result = do { tg build $pkg } | complete

# Check for watchdog cancellations.
let cancelled_count = (sqlite3 ($fresh.directory | path join "database") "SELECT count(*) FROM processes WHERE error_code = 'cancellation'" | str trim | into int)
print -e $"Cancelled processes: ($cancelled_count)"
assert ($cancelled_count == 0) $"watchdog cancelled ($cancelled_count) executing processes"

success $fresh_result
