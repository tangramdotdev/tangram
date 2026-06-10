use ../../test.nu *

# A build fanning out eight children across four concurrent remote
# runners completes and returns every child's result.
#
# Regression test for 802b850c (#765).

# Start the remote server.
let config = {
	runner: false,
}
let remote = spawn --name remote --cloud --config $config

# Spawn four concurrent runners
let runners = ["runner1", "runner2", "runner3", "runner4"] | each { |name|
	# Start the runner server.
	let config = {
		remotes: {
			default: {
				url: $remote.url
			}
		},
		runner: {
			concurrency: 1,
			remote: "default",
		}
	}
	spawn --name $name --config $config
}

# Start the local server.
let config = {
	remotes: {
		default: {
			url: $remote.url
		}
	}
}
let local = spawn --name local --config $config

let path = artifact {
	tangram.ts: '
		export default async () => {
			let children = [];
			for (let i = 0; i < 8; i++) {
				let process = tg.build(child, i);
				children.push(process);
			}
			return Promise.all(children);
		}
		export const child = async (n: number) => {
			await tg.sleep(0.5);
			return n;
		};
	'
};

# Run a remote build
let id = tg build --remote --detach $path
let output = tg wait $id
snapshot $output '{"exit":0,"output":[0,1,2,3,4,5,6,7]}'
