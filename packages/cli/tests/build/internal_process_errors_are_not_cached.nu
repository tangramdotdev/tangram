use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		const inner = () => tg.build({
			args: ["-c", "true"],
			executable: "sh",
			host: "not-a-real-host",
		}).named("inner");

		export const first = async () => await inner();
		export const second = async () => await inner();
	',
}

let first = tg build -dv $"($path)#first" | from json
let first_output = tg output $first.process | complete
failure $first_output

let first_child = tg process children $first.process | from json | get 0.process
let first_error = tg get $first_child | from json | get error
let first_error_pretty = tg get $first_error --pretty
assert ($first_error_pretty | str contains '"code": "internal"') "The error should have the internal code."

let second = tg build -dv $"($path)#second" | from json
let second_output = tg output $second.process | complete
failure $second_output

let second_child = tg process children $second.process | from json | get 0.process
assert not equal $first_child $second_child "The internal error should not be reused as a cache hit."
