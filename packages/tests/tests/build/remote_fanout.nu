use ../../lib/test.nu *

# A build fanning out eight children across four concurrent remote runners
# completes without releasing leases for children it already waited for.
#
# Regression test for 802b850c (#765).

# Start the remote server.
let root_token = random chars
let config = {
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
	scheduler: { default_cpu: 1, default_memory: 1_073_741_824 },
}
let remote = server spawn --name remote --cloud --config $config

# Spawn four concurrent runners
let runners = ["runner1", "runner2", "runner3", "runner4"] | each { |name|
	let created = tg --url $remote.url --token $root_token runner create | from json

	# Start the runner server.
	let config = {
		advanced: {
			checkpoints: true,
		},
		remotes: {
			default: {
				token: $created.token.token
				url: $remote.url
			}
		},
		scheduler: { default_cpu: 1, default_memory: 1_073_741_824 },
		runner: {
			cpus: 1,
			overcommit: { cpu: 1 },
			id: $created.data.id
			memory: 1_073_741_824,
			remote: "default",
			token: $created.token.token
		}
	}
	server spawn --name $name --config $config
}

# Block any attempt by a parent to release an awaited child's lease.
let release_watches = $runners | each { |runner|
	tg --url $runner.url checkpoint watch runner.process.child_lease.release
	| from json
	| get watch
}

# Hold each process as it finishes, so that the children overlap across the runners.
let finish_watches = $runners | each { |runner| checkpoint watch runner.process.finish --url $runner.url }

# Start the local server.
let config = {
	remotes: {
		default: {
			token: $root_token
			url: $remote.url
		}
	}
}
let local = server spawn --name local --config $config

let path = artifact {
	tangram.ts: '
		export default async function () {
			let children = [];
			for (let i = 0; i < 8; i++) {
				let process = tg.build(child, i);
				children.push(process);
			}
			return Promise.all(children);
		}
		export function child(n: number) {
			return n;
		}
	'
};

# Run a remote build
let id = tg build --no-tokens --remote --detach $path | referent node

# Wait for every runner to hold a finishing child, which proves that the children run concurrently, and then let them finish.
$finish_watches | par-each { |watch| checkpoint wait $watch } | ignore
$finish_watches | each { |watch| checkpoint unwatch $watch } | ignore
let output = timeout 10s tg wait $id | complete
for entry in ($runners | enumerate) {
	let watch = $release_watches | get $entry.index
	tg --url $entry.item.url checkpoint unwatch runner.process.child_lease.release $watch
}
success $output "the parent should not release leases for children it waited for"
snapshot ($output.stdout | str trim) '{"exit":0,"output":[0,1,2,3,4,5,6,7]}'
