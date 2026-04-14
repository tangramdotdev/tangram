use ../../test.nu *

# Test that a sandboxed process can check in a file created on the overlay root.

if $nu.os-info.name != "linux" {
	return
}

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			return tg.build`
					echo "hello from overlay" > /overlay_test.txt
					tangram checkin /overlay_test.txt > ${tg.output}
				`;
		}
	',
}

let output = tg build $path
let checkin_id = tg read $output | str trim
let contents = tg read $checkin_id | str trim
snapshot $contents 'hello from overlay'
