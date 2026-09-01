use ../../test.nu *

# Restarting the remote server mid-build does not lose log output, and the full stdout and stderr streams are still readable afterward.

let root_token = random chars
let config = {
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
}
let remote = server spawn --name remote  --config $config
let created = tg --url $remote.url --token $root_token runner create | from json
let config = {
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
let runner = server spawn --name runner --config ($config | merge deep {
	advanced: {
		checkpoints: true,
	},
})

let config = {
	remotes: {
		default: {
			token: $root_token
			url: $remote.url
		}
	},
}
let local = server spawn --name local --config $config

let path = artifact {
	tangram.ts: r#'
		export default async function () {
			let alphabet = "abcdefghijklmnopqrstuvwxyz";
			for (let i = 0; i < 26; i++) {
				let s = "";
				for (let j = 0; j < 20; j++) {
					s = s + alphabet[i];
				}
				console.log('stdout', s);
				console.error('stderr', s);
				await tg.sleep(0.1)
			}
		}
	'#
}

let start_watch = (
	tg --url $runner.url checkpoint watch runner.process.start
	| from json
	| get watch
)
let id = tg --url $local.url build --remote --detach -E TANGRAM_QUIET=true $path

# Replace the remote after it schedules the process but before the runner
# starts it, then verify that the runner delivers the complete logs.
tg --url $runner.url checkpoint wait runner.process.start $start_watch 0 | ignore
tg --url $remote.url server restart
tg --url $runner.url checkpoint continue runner.process.start $start_watch 0
tg --url $runner.url checkpoint unwatch runner.process.start $start_watch

let output = tg --url $local.url wait $id
let stdout = tg --url $local.url log $id --stream=stdout
snapshot $stdout '
	stdout aaaaaaaaaaaaaaaaaaaa
	stdout bbbbbbbbbbbbbbbbbbbb
	stdout cccccccccccccccccccc
	stdout dddddddddddddddddddd
	stdout eeeeeeeeeeeeeeeeeeee
	stdout ffffffffffffffffffff
	stdout gggggggggggggggggggg
	stdout hhhhhhhhhhhhhhhhhhhh
	stdout iiiiiiiiiiiiiiiiiiii
	stdout jjjjjjjjjjjjjjjjjjjj
	stdout kkkkkkkkkkkkkkkkkkkk
	stdout llllllllllllllllllll
	stdout mmmmmmmmmmmmmmmmmmmm
	stdout nnnnnnnnnnnnnnnnnnnn
	stdout oooooooooooooooooooo
	stdout pppppppppppppppppppp
	stdout qqqqqqqqqqqqqqqqqqqq
	stdout rrrrrrrrrrrrrrrrrrrr
	stdout ssssssssssssssssssss
	stdout tttttttttttttttttttt
	stdout uuuuuuuuuuuuuuuuuuuu
	stdout vvvvvvvvvvvvvvvvvvvv
	stdout wwwwwwwwwwwwwwwwwwww
	stdout xxxxxxxxxxxxxxxxxxxx
	stdout yyyyyyyyyyyyyyyyyyyy
	stdout zzzzzzzzzzzzzzzzzzzz
'

let stderr = tg --url $local.url log $id --stream=stderr out+err>|
snapshot $stderr '
	stderr aaaaaaaaaaaaaaaaaaaa
	stderr bbbbbbbbbbbbbbbbbbbb
	stderr cccccccccccccccccccc
	stderr dddddddddddddddddddd
	stderr eeeeeeeeeeeeeeeeeeee
	stderr ffffffffffffffffffff
	stderr gggggggggggggggggggg
	stderr hhhhhhhhhhhhhhhhhhhh
	stderr iiiiiiiiiiiiiiiiiiii
	stderr jjjjjjjjjjjjjjjjjjjj
	stderr kkkkkkkkkkkkkkkkkkkk
	stderr llllllllllllllllllll
	stderr mmmmmmmmmmmmmmmmmmmm
	stderr nnnnnnnnnnnnnnnnnnnn
	stderr oooooooooooooooooooo
	stderr pppppppppppppppppppp
	stderr qqqqqqqqqqqqqqqqqqqq
	stderr rrrrrrrrrrrrrrrrrrrr
	stderr ssssssssssssssssssss
	stderr tttttttttttttttttttt
	stderr uuuuuuuuuuuuuuuuuuuu
	stderr vvvvvvvvvvvvvvvvvvvv
	stderr wwwwwwwwwwwwwwwwwwww
	stderr xxxxxxxxxxxxxxxxxxxx
	stderr yyyyyyyyyyyyyyyyyyyy
	stderr zzzzzzzzzzzzzzzzzzzz
'
