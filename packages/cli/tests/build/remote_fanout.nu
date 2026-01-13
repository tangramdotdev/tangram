use ../../test.nu *

# Start the remote server.
let config = {
	runner: false,
}
let remote = spawn -n remote --cloud --config $config

# Spawn four concurrent runners
let runners = ["runner1", "runner2", "runner3", "runner4"] | each { |name|
	# Start the runner server.
	let config = {
		remotes: [
			{
				name: "default",
				url: $remote.url
			}
		],
		runner: {
			concurrency: 1,
			remotes: ["default"],
		}
	}
	spawn -n $name --config $config
}

# Start the local server.
let config = {
	remotes: [
		{
			name: "default",
			url: $remote.url
		}
	]
}
let local = spawn -n local --config $config

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
let id = tg build --remote -d $path
let output = tg wait $id
snapshot $output '{"exit":0,"output":[0,1,2,3,4,5,6,7]}'
