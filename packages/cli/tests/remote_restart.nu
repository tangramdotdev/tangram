use ../test.nu *

# After a remote runner produces a process output, restarting the remote server preserves the process log so it can still be retrieved from the local server.

# Spawn a server in a given directory.
let config =  { 
	runner: false,
	advanced: {
		single_process: false,
	}
}
let remote = spawn --name remote --cloud --config $config

# Spawn a remote runner.
let runner = spawn --name runner --config {
	runner: {
		remote: "default"
	}
	remotes: {
		default: {
			url: $remote.url
		}
	}
}

# Spawn a local server.
let local = spawn --name local --config {
	remotes: {
		default: {
			url: $remote.url
		}
	}
}

let path = artifact {
	tangram.ts: '
		export default async function () {
			for (let i = 0; i < 16; i++) {
				console.log(`log line ${i}`);
				await tg.sleep(0.250);
			}
		}
	'
}

# Run the process.
let process = tg --url $local.url run --detach $path --remote

# Wait for the process to finish.
let output = tg --url $local.url process wait $process | complete
success $output
snapshot ($output.stdout | from json) '
	exit: 0

'

# Kill the server.
print 'killing remote'
let pid = open ($remote.directory | path join 'lock') | into int
kill --signal 2 $pid

# Wait for the server to stop.
if $nu.os-info.name == "linux" { ^tail --pid $pid -f /dev/null } else { while (ps | where pid == $pid | is-not-empty) { sleep 10ms } }
print 'server stopped.'

# Restart the remote server.
spawn --directory $remote.directory --name remote --cloud --config $config --url $remote.url

# Ensure we can check the health.
let health = tg --url $remote.url health | complete
success $health

# Get the output.
let output = tg --url $local.url log --no-timeout $process | complete
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
