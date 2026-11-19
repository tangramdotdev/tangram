#!/usr/bin/env nu

const repository_path = path self '../../'

# The variables that enable Developer ID signing and notarization. Each one holds a path, and the files stay outside the Tangram store. TANGRAM_APPLE_P12 names a .p12 file with the Developer ID Application certificate and its private key, TANGRAM_APPLE_P12_PASSWORD_FILE names a file that contains the password of the .p12 file, and TANGRAM_APPLE_API_KEY names an App Store Connect API key JSON file, as written by `rcodesign encode-app-store-connect-api-key`.
const variables = [TANGRAM_APPLE_P12 TANGRAM_APPLE_P12_PASSWORD_FILE TANGRAM_APPLE_API_KEY]

# Sign the staged macOS release binaries with a Developer ID certificate and notarize them when the signing variables are set.
def main [
	...targets: string # The macOS release targets whose staged binaries to sign.
] {
	cd $repository_path
	let binaries = $targets | each {|target| $'release/tangram_($target)' | path expand }
	if ($binaries | is-empty) {
		return
	}
	for binary in $binaries {
		if ($binary | path type) != file {
			error make { msg: $"expected a staged executable at ($binary)" }
		}
	}

	# Require all of the signing variables or none of them.
	let set = $variables | where {|name| $env | get -o $name | is-not-empty }
	if ($set | is-empty) {
		print -e $"The macOS release binaries are not Developer ID signed or notarized. Set ($variables | str join ', ') to sign and notarize them."
		return
	}
	let missing = $variables | where {|name| $name not-in $set }
	if ($missing | is-not-empty) {
		error make { msg: $"the signing variables are only partially set, so set ($missing | str join ', ') as well or unset all of them" }
	}
	for name in $variables {
		if ($env | get $name | path type) != file {
			error make { msg: $"the file named by ($name) does not exist" }
		}
	}
	let p12_path = $env.TANGRAM_APPLE_P12 | path expand
	let p12_password_path = $env.TANGRAM_APPLE_P12_PASSWORD_FILE | path expand
	let api_key_path = $env.TANGRAM_APPLE_API_KEY | path expand
	let entitlements_path = $repository_path | path join scripts/release/tangram.entitlements

	let temporary_path = ^mktemp -d release/.sign.XXXXXXXX | str trim | path expand
	let result = try {
		sign $binaries $temporary_path $p12_path $p12_password_path $api_key_path $entitlements_path
		null
	} catch {|error|
		$error
	}
	rm -rf $temporary_path
	if $result != null {
		error make { msg: $"failed to sign and notarize the macOS release binaries: ($result.msg)" }
	}
}

def sign [
	binaries: list<string>
	temporary_path: path
	p12_path: path
	p12_password_path: path
	api_key_path: path
	entitlements_path: path
] {
	# Check out the Linux or macOS rcodesign from std, which build.tg.ts already depends on.
	let workspace_path = $repository_path | path join ../packages/packages/std/wrap/workspace.tg.ts | path expand
	let rcodesign = $temporary_path | path join rcodesign
	print -e "==> Checking out rcodesign"
	^tg build $"($workspace_path)#rcodesign" $"--checkout=($rcodesign)"
	^chmod +x $rcodesign

	for staged in $binaries {
		let name = $staged | path basename
		let directory = $temporary_path | path join $name
		mkdir $directory
		let binary = $directory | path join tangram
		^install -m 755 $staged $binary

		# Sign a copy of the binary with the hardened runtime.
		print -e $"==> Signing ($name)"
		(
			^$rcodesign sign
				--code-signature-flags runtime
				--entitlements-xml-file $entitlements_path
				--for-notarization
				--p12-file $p12_path
				--p12-password-file $p12_password_path
				$binary
		)

		# Notarize the binary in a zip file, because the notary service does not accept a bare executable. A bare executable cannot be stapled, so Gatekeeper checks the ticket online.
		print -e $"==> Notarizing ($name)"
		let zip = $temporary_path | path join $"($name).zip"
		^zip -j -q -X $zip $binary
		^$rcodesign notary-submit --api-key-file $api_key_path --wait $zip

		# Replace the staged binary with the signed copy.
		mv -f $binary $staged
		print -e $"==> Signed and notarized ($name)"
	}
}
