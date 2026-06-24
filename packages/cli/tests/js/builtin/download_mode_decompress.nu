use ../../../test.nu *

# tg.download with the "decompress" mode decompresses the downloaded archive, returning a file artifact.

skip_if_offline

let server = spawn

let arch = $nu.os-info.arch
let archive = if $nu.os-info.name == "macos" {
	"dash_universal_darwin.tar.zst"
} else {
	$"dash_($arch)_linux.tar.zst"
}
let url = $"https://github.com/tangramdotdev/bootstrap/releases/download/v2026.01.26/($archive)"

let module = '
	export default async function () {
		let result = await tg.download("URL_PLACEHOLDER", undefined, { mode: "decompress" });
		return (result instanceof tg.File) && tg.Artifact.is(result);
	}
' | str replace "URL_PLACEHOLDER" $url

let path = artifact {
	tangram.ts: $module
}

let output = tg build $path
snapshot $output 'true'
