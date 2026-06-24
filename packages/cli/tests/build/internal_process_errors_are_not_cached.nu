use ../../test.nu *

# A process that fails with an internal error is not reused as a cache hit, so a second build of the same command runs a fresh process.

let server = spawn

let path = artifact {
	tangram.ts: '
		function inner() { return tg.build({
			args: ["-c", "true"],
			executable: "sh",
			host: "not-a-real-host",
		}).named("inner"); }

		export async function first() { return await inner(); }
		export async function second() { return await inner(); }
	',
}

let first = tg build --detach --verbose $"($path)#first" | from json
let first_output = tg output $first.process | complete
failure $first_output

let first_child = tg process children $first.process | from json | get 0.process
let first_error = tg get $first_child | from json | get error
let first_error_pretty = tg get $first_error --pretty
snapshot ($first_error_pretty | redact $path | normalize_ids) '
	tg.error({
	  "code": "internal",
	  "message": "failed to run the process",
	  "source": tg.error({
	    "message": "cannot run process with host",
	    "values": {
	      "host": "not-a-real-host",
	    },
	  }),
	  "values": {
	    "process": "<process>",
	  },
	})
'

let second = tg build --detach --verbose $"($path)#second" | from json
let second_output = tg output $second.process | complete
failure $second_output

let second_child = tg process children $second.process | from json | get 0.process
assert not equal $first_child $second_child "The internal error should not be reused as a cache hit."
