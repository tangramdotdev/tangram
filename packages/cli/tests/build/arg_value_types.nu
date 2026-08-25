use ../../test.nu *

# A build parses each `--arg-value` value kind into the corresponding JS type, unlike `--arg-string` which always produces a string.

let server = server spawn

let path = artifact {
	tangram.ts: 'export default function (v: unknown) { return `${typeof v} ${JSON.stringify(v)}`; }'
}

snapshot (tg build $path --arg-string '42') '"string \"42\""'
snapshot (tg build $path --arg-value '42') '"number 42"'
snapshot (tg build $path --arg-value 'true') '"boolean true"'
snapshot (tg build $path --arg-value 'null') '"object null"'
snapshot (tg build $path --arg-value '[1,2,3]') '"object [1,2,3]"'
snapshot (tg build $path --arg-value '{"a":1}') '"object {\"a\":1}"'
snapshot (tg build $path --arg-value '"Tangram"') '"string \"Tangram\""'
snapshot (tg build $path --arg-string '-x') '"string \"-x\""'
snapshot (tg build $path --arg-value '-1') '"number -1"'
