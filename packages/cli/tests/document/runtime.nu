use ../../test.nu *

# Documenting the runtime produces documentation for the tangram.d.ts module.

let server = spawn

let output = tg document --runtime | complete
success $output

let json = $output.stdout | from json
let exports = $json.exports | columns
for name in ["Artifact" "Blob" "Directory" "File"] {
	assert ($name in $exports) $"the runtime documentation should export ($name)"
}
