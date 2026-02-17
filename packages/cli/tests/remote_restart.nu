use ../test.nu *

# Spawn a server in a given directory.
let config =  { 
	runner: false,
	advanced: {
		single_process: false,
	}
}
let remote = spawn -n remote --cloud --config $config --url http://localhost:8476

# Spawn a remote runner.
let runner = spawn -n runner --config {
	runner: {
		remotes: ["default"]
	}
	remotes: [
		{
			name: "default"
			url: $remote.url
			reconnect: {
				backoff: 0.1,
				jitter: 0.05,
				max_delay: 5,
				max_retries: 32,
			}
			retry: {
				backoff: 0.1,
				jitter: 0.05,
				max_delay: 5,
				max_retries: 32,
			}
		}
	]
}

# Spawn a local server.
let local = spawn -n local --config {
	remotes: [
		{
			name: "default",
			url: $remote.url
			reconnect: {
				backoff: 0.1,
				jitter: 0.05,
				max_delay: 5,
				max_retries: 32,
			}
			retry: {
				backoff: 0.1,
				jitter: 0.05,
				max_delay: 5,
				max_retries: 32,
			}
		}
	]
}

let path = artifact {
	tangram.ts: '
		export default async () => {
			for (let i = 0; i < 16; i++) {
				console.log(`log line ${i}`);
				await tg.sleep(0.250);
			}
		};
	'
}

# Run the process.
let process = tg -u $local.url run -d $path --remote

# In the background perform a long lived task.
job spawn {
	tg -u $local.url log $process | complete | job send 0
}

# Kill the server after one second.
sleep 1sec
print 'killing remote'
let pid = open ($remote.directory | path join 'lock') | into int
kill --signal 2 $pid

# Wait for the process to finish.
if $nu.os-info.name == "linux" { ^tail --pid $pid -f /dev/null } else { while (ps | where pid == $pid | is-not-empty) { sleep 10ms } }
print 'server stopped.'

# Restart the remote server.
spawn --directory $remote.directory -n remote --cloud --config $config --url $remote.url

# Ensure we can check the health.
let health = tg -u $remote.url health | complete
success $health

let output = tg -u $local.url process wait $process | complete
success $output
snapshot ($output.stdout | from json) '
	exit: 0

'

# Get the output.
let output = job recv
success $output
snapshot $output.stdout '
	log line 0
	log line 1
	log line 2
	log line 3
	log line 4
	log line 5
	log line 6
	log line 7
	log line 8
	log line 9
	log line 10
	log line 11
	log line 12
	log line 13
	log line 14
	log line 15

'
