use ../../test.nu *
let server = spawn
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
			}
		};
	'#
}
let output = tg run $path | complete
success $output

# Check that we can read just one chunk forwards
let stdout = $output.stdout | str trim -r -c "\n"
assert ($stdout == ([
	"stdout aaaaaaaaaaaaaaaaaaaa",
	"stdout bbbbbbbbbbbbbbbbbbbb",
	"stdout cccccccccccccccccccc",
	"stdout dddddddddddddddddddd",
	"stdout eeeeeeeeeeeeeeeeeeee",
	"stdout ffffffffffffffffffff",
	"stdout gggggggggggggggggggg",
	"stdout hhhhhhhhhhhhhhhhhhhh",
	"stdout iiiiiiiiiiiiiiiiiiii",
	"stdout jjjjjjjjjjjjjjjjjjjj",
	"stdout kkkkkkkkkkkkkkkkkkkk",
	"stdout llllllllllllllllllll",
	"stdout mmmmmmmmmmmmmmmmmmmm",
	"stdout nnnnnnnnnnnnnnnnnnnn",
	"stdout oooooooooooooooooooo",
	"stdout pppppppppppppppppppp",
	"stdout qqqqqqqqqqqqqqqqqqqq",
	"stdout rrrrrrrrrrrrrrrrrrrr",
	"stdout ssssssssssssssssssss",
	"stdout tttttttttttttttttttt",
	"stdout uuuuuuuuuuuuuuuuuuuu",
	"stdout vvvvvvvvvvvvvvvvvvvv",
	"stdout wwwwwwwwwwwwwwwwwwww",
	"stdout xxxxxxxxxxxxxxxxxxxx",
	"stdout yyyyyyyyyyyyyyyyyyyy",
	"stdout zzzzzzzzzzzzzzzzzzzz",
] | str join "\n"))
let stderr = $output.stderr
let stderr = $stderr | str replace -ar 'pcs_00[0-9a-z]{26}' 'PROCESS'
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
