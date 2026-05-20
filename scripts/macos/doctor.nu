#!/usr/bin/env nu

def require-command [command: string, help: string] {
	if (which $command | is-empty) {
		error make {
			msg: $"($command) is not installed"
			help: $help
		}
	}
}

def main [
	--signing
	--release
	--notary-profile: string
] {
	if $nu.os-info.name != "macos" {
		error make { msg: "the macOS app can only be built on macOS" }
	}

	require-command cargo "Install Rust with rustup."
	require-command security "Install macOS."
	require-command xcode-select "Install Xcode from the App Store."
	require-command xcodebuild "Install Xcode from the App Store."
	require-command xcodegen "Install XcodeGen with `brew install xcodegen`."
	require-command xcrun "Install Xcode from the App Store."

	let selected_toolchain = (^xcode-select -p | complete)
	if $selected_toolchain.exit_code != 0 {
		error make {
			msg: "an Xcode command line toolchain is not selected"
			help: "Run `sudo xcode-select --switch /Applications/Xcode.app/Contents/Developer`."
		}
	}
	let developer_directory = ($selected_toolchain.stdout | str trim)
	if not ($developer_directory | str contains ".app/Contents/Developer") {
		error make {
			msg: "the selected command line toolchain is not a complete Xcode installation"
			help: "Run `sudo xcode-select --switch /Applications/Xcode.app/Contents/Developer`."
		}
	}

	if $signing or $release {
		let identities = (^security find-identity -v -p codesigning | complete)
		if $identities.exit_code != 0 or not ($identities.stdout | str contains "Apple Development") {
			error make {
				msg: "an Apple Development signing identity is not available"
				help: "Sign in to the Tangram Apple Developer account in Xcode and create an Apple Development certificate."
			}
		}

		if $release and not ($identities.stdout | str contains "Developer ID Application") {
			error make {
				msg: "a Developer ID Application signing identity is not available"
				help: "Create a Developer ID Application certificate for the Tangram Apple Developer account."
			}
		}
	}

	let profile = if ($notary_profile | is-empty) {
		$env.TANGRAM_NOTARY_PROFILE? | default "Tangram"
	} else {
		$notary_profile
	}
	if $release {
		let history = (^xcrun notarytool history --keychain-profile $profile | complete)
		if $history.exit_code != 0 {
			error make {
				msg: $"the notarytool profile `($profile)` is missing or invalid"
				help: $"Run `xcrun notarytool store-credentials '($profile)' --apple-id <apple-id> --team-id <team-id>`."
			}
		}
	}

	let workflow = if $release {
		"release builds"
	} else if $signing {
		"signed development builds"
	} else {
		"unsigned checks"
	}
	print $"the macOS toolchain is ready for ($workflow)"
}
