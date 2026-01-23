use ../test.nu *

# Spawn a server in a given directory.
let config =  { 
	runner: false,
}
let remote = spawn -n remote --cloud --config $config 

# Spawn a remote runner.
let runner = spawn -n runner --config { 
	runner: {
		remotes: ["default"]
	}
	remotes: [
		{
			name: "default"
			url: $remote.url
		}
	]
}

let path = artifact {
	tangram.ts: '
		export default async () => {
			for (let i = 0; i < 20; i++) {
				console.log(`log line ${i}`);
				await tg.sleep(0.250);
			}
		};
	'
}

# Run the process.
let process = tg -u $remote.url run -d $path 

# In the background do a long lived task.
job spawn {
	tg log $process | complete | job send 0
}

# Kill the server after awhile
sleep 1sec
print 'killing remote'
let pid = open ($remote.directory | path join 'lock') | into int
kill --signal 2 $pid

# Wait for the process to finish.
tail --pid $pid -f
print 'server stopped.'

# Restart the remote server.
spawn --directory $remote.directory -n remote --cloud --config $config --url $remote.url

# Ensure we can check the health.
let health = tg -u $remote.url health | complete
success $health

# Get the output.
let output = job recv
success $output
snapshot $output.stdout ''