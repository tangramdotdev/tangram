use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			await tg.run(
				{
					args: [
						"-c",
						`
							read line
							echo "stdout:$line"
							echo "stderr:$line" 1>&2
						`,
					],
					executable: "sh",
					host: tg.process.env.TANGRAM_HOST,
					sandbox: true,
					stdin: "inherit",
					stdout: "inherit",
					stderr: "inherit",
				},
			);
		}
	',
}

let output = "hello\n" | tg run $path | complete
success $output
assert (($output.stdout | str trim -r -c "\n") == "stdout:hello")
assert (($output.stderr | lines | last) == "stderr:hello")
