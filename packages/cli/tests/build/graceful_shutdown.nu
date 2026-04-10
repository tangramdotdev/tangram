use ../../test.nu *
let config = {
	runner: false,
}
let remote = spawn -n remote  -c $config
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
let runner = spawn -n runner --config $config

let config = {
	remotes: [
		{
			name: "default",
			url: $remote.url
		}
	],
}
let local = spawn -n local --config $config

let path = artifact {
	tangram.ts: r#'
		export default async () => {
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
		};
	'#
}

let id = tg --url $local.url build --remote -d $path
# tg --url $remote.url server restart
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
