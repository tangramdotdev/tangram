use ../test.nu *

let config = {
	runner: false,
	advanced: {
		single_process: false,
	},
	scheduler: {
		runner_ttl: 3,
	},
}
let remote = spawn --name remote --cloud --config $config

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
			await tg.sleep(60);
		}
	'
}

let process = tg --url $local.url run --detach $path --remote
sleep 3sec

let pid = open ($runner.directory | path join 'lock') | into int
kill --signal 9 $pid

# Wait for the runner to stop.
if $nu.os-info.name == "linux" { ^tail --pid $pid -f /dev/null } else { while (ps | where pid == $pid | is-not-empty) { sleep 10ms } }

let output = tg --url $local.url process wait $process | complete
snapshot $output.stdout '
	{"error":"err_01gmam66a4rw87atwd2darxctm56kspwcyeby8aymr1f9mb8nhzsj0","exit":1}

'
snapshot $output.stderr ''
