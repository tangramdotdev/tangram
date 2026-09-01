use ../../test.nu *

# A build fanning out eight children across four concurrent remote runners
# completes without releasing leases for children it already waited for.
#
# Regression test for 802b850c (#765).

# Start the remote server.
let root_token = random chars
let config = {
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
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
		runner: {
			cpus: 1,
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
		export async function child(n: number) {
			await tg.sleep(0.5);
			return n;
		}
	'
};

# Run a remote build
let id = tg build --remote --detach $path
let output = timeout 10s tg wait $id | complete
for entry in ($runners | enumerate) {
	let watch = $release_watches | get $entry.index
	tg --url $entry.item.url checkpoint unwatch runner.process.child_lease.release $watch
}
success $output "the parent should not release leases for children it waited for"
snapshot ($output.stdout | str trim) '{"exit":0,"output":[0,1,2,3,4,5,6,7]}'
