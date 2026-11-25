#!/usr/bin/env nu

use ./identifiers.nu

def step [message: string] {
	print $"\n==> ($message)"
}

def main [
	--build-number: string
	--helper: path # A prebuilt tangram executable for this Mac's architecture to embed instead of building one with Cargo.
	--install
	--no-sign
	--notary-profile: string
	--provider: path # A prebuilt libtangram_vfs_provider.a for this Mac's architecture to link instead of building one with Cargo.
	--version: string
] {
	let root = ($env.FILE_PWD | path dirname | path dirname)
	let path = ($root | path join "packages/macos")
	let build_path = ($path | path join "build/Distribution")
	let archive_path = ($build_path | path join "Tangram.xcarchive")
	let derived_data_path = ($build_path | path join "DerivedData")
	let export_path = ($build_path | path join "Export")
	let archived_app = ($archive_path | path join "Products/Applications/Tangram.app")
	let app_path = if $no_sign { $archived_app } else { $export_path | path join "Tangram.app" }
	let artifact_path = ($build_path | path join "Tangram.zip")
	let dmg_path = ($build_path | path join "Tangram.dmg")
	let appcast_path = ($build_path | path join "appcast.xml")
	let project = ($path | path join "Tangram.xcodeproj")
	let result_bundle_path = ($build_path | path join "TangramArchive.xcresult")
	let prebuilt = $helper != null or $provider != null
	if $prebuilt and ($helper == null or $provider == null) {
		error make {msg: '--helper and --provider must be passed together'}
	}
	let source_path = if $prebuilt { $helper | path expand } else { $root | path join "target/release/tangram" }
	let provider_path = if $prebuilt { $provider | path expand } else { null }
	let library_path = ($build_path | path join "Libraries")
	let export_options_path = ($path | path join "ExportOptions.plist")
	let profile = if ($notary_profile | is-empty) {
		$env.TANGRAM_NOTARY_PROFILE? | default "Tangram"
	} else {
		$notary_profile
	}
	let doctor = ($root | path join "scripts/macos/doctor.nu")

	cd $root
	let version = $version | default ((open Cargo.toml).workspace.package.version)
	let build_number = $build_number | default (if $no_sign { "1" } else { "" })
	if $version !~ '^[0-9]+\.[0-9]+\.[0-9]+$' {
		error make {msg: 'expected a numeric macOS release version'}
	}
	if $no_sign and $install {
		error make {msg: '--install requires a signed macOS app'}
	}
	if $build_number !~ '^[1-9][0-9]*$' {
		error make {msg: 'pass a positive --build-number for the macOS release'}
	}
	let public_key_path = ($path | path join "SparklePublicKey.txt")
	let public_key = if $no_sign {
		$env.TANGRAM_SPARKLE_TEST_PUBLIC_KEY? | default ""
	} else {
		if not ($public_key_path | path exists) {
			error make {msg: 'set up the production Sparkle key before packaging a macOS release'}
		}
		open --raw $public_key_path | str trim
	}
	if not $no_sign and $public_key !~ '^[A-Za-z0-9+/]{43}=$' {
		error make {msg: 'the production Sparkle public key is invalid'}
	}
	let feed_url = if $no_sign {
		$env.TANGRAM_SPARKLE_TEST_FEED_URL? | default ""
	} else {
		"https://pkgs.tangram.dev/stable/macos/appcast.xml"
	}
	step "Checking the macOS release environment"
	let prebuilt_flags = if $prebuilt { ['--prebuilt'] } else { [] }
	if $no_sign {
		^nu $doctor ...$prebuilt_flags
	} else {
		^nu $doctor ...$prebuilt_flags --release --notary-profile $profile
	}

	let arch = (^uname -m | str trim)
	if $prebuilt {
		step "Checking the prebuilt Tangram helper and provider library"
		for file in [$source_path $provider_path] {
			if ($file | path type) != file {
				error make {msg: $'the prebuilt file was not found at ($file)'}
			}
			if $arch not-in (^lipo -archs $file | str trim | split row ' ') {
				error make {msg: $'($file) is not built for ($arch)'}
			}
		}
	} else {
		step "Building the Tangram helper"
		^cargo build --release -p tangram_cli -p tangram_vfs_provider
	}

	step "Generating the Xcode project"
	$env.TANGRAM_DEVELOPMENT_TEAM = (identifiers).development_team
	^xcodegen generate --spec ($path | path join "project.yml")

	step "Archiving Tangram"
	rm -rf $build_path
	mkdir $build_path
	let library_args = if $prebuilt {
		# Give the prebuilt provider library the name that the extension links.
		mkdir $library_path
		^install -m 644 $provider_path ($library_path | path join libtangram_vfs_provider.a)
		[$"LIBRARY_SEARCH_PATHS=($library_path)"]
	} else {
		[]
	}
	let signing_args = if $no_sign { ['CODE_SIGNING_ALLOWED=NO'] } else { ['-allowProvisioningUpdates'] }
	(
		xcodebuild
			...$signing_args
			-archivePath $archive_path
			-configuration Release
			-derivedDataPath $derived_data_path
			-destination $"platform=macOS,arch=($arch)"
			-project $project
			-resultBundlePath $result_bundle_path
			-scheme Tangram
			-showBuildTimingSummary
			$"ARCHS=($arch)"
			$"CURRENT_PROJECT_VERSION=($build_number)"
			$"MARKETING_VERSION=($version)"
			$"TANGRAM_HELPER_PATH=($source_path)"
			$"TANGRAM_SPARKLE_FEED_URL=($feed_url)"
			$"TANGRAM_SPARKLE_PUBLIC_KEY=($public_key)"
			...$library_args
			archive
	)

	let archived_helper = ($archived_app | path join "Contents/Helpers/tangram")
	if not ($archived_helper | path exists) {
		error make { msg: "the archive does not contain the Tangram helper" }
	}

	if not $no_sign {
		step "Exporting Tangram with Developer ID signing"
		(
			xcodebuild
				-allowProvisioningUpdates
				-archivePath $archive_path
				-exportArchive
				-exportOptionsPlist $export_options_path
				-exportPath $export_path
		)
	}
	if not ($app_path | path exists) {
		error make { msg: $"the app was not found at ($app_path)" }
	}
	if (^test -x ($app_path | path join "Contents/Helpers/tangram") | complete).exit_code != 0 {
		error make {msg: 'the app does not contain the executable Tangram helper'}
	}
	let identity = if $no_sign {
		null
	} else {
		verify-signing $app_path
	}

	step "Creating the drag-to-Applications disk image"
	^nu ($root | path join "scripts/macos/dmg.nu") $app_path $dmg_path
	if $identity != null {
		^codesign --sign $identity --timestamp $dmg_path
		^xcrun notarytool submit $dmg_path --keychain-profile $profile --wait
		^xcrun stapler staple $dmg_path
		^xcrun stapler validate $dmg_path
		^codesign --verify --strict $dmg_path
		^spctl --assess --type open --context context:primary-signature --verbose=2 $dmg_path
		# The disk image submission also creates a ticket for the bundled app.
		^xcrun stapler staple $app_path
		^xcrun stapler validate $app_path
		^codesign --verify --deep --strict --verbose=2 $app_path
		^spctl -a -vvv -t exec $app_path
	}
	^hdiutil verify $dmg_path

	if not $no_sign {
		step "Signing the Sparkle update"
		let tool = ($derived_data_path | path join "SourcePackages/artifacts/sparkle/Sparkle/bin/generate_appcast")
		^python3 scripts/release/sparkle.py $tool $dmg_path $appcast_path $build_number
	}

	step "Creating the distributable archive"
	^ditto -c -k --keepParent $app_path $artifact_path

	if $install {
		let installed_app = "/Applications/Tangram.app"
		step $"Installing Tangram at ($installed_app)"
		^pkill -x Tangram | complete | ignore
		rm -rf $installed_app
		^ditto $app_path $installed_app
		^/System/Library/Frameworks/CoreServices.framework/Frameworks/LaunchServices.framework/Support/lsregister -f -R -trusted $installed_app
		^pluginkit -r ($installed_app | path join "Contents/Extensions/TangramFSKit.appex") | complete | ignore
		^pluginkit -a ($installed_app | path join "Contents/Extensions/TangramFSKit.appex")
		^xcrun stapler validate $installed_app
		^spctl -a -vvv -t exec $installed_app
	}

	print $"\nPackaged Tangram at ($dmg_path) and ($artifact_path)."
}

def verify-signing [app: path] {
	^codesign --verify --deep --strict --verbose=2 $app
	let signature = (^codesign -dv --verbose=4 $app | complete)
	let details = $signature.stdout + $signature.stderr
	let authority = ($details | lines | where {|line| $line starts-with "Authority=Developer ID Application"} | get -o 0)
	if $signature.exit_code != 0 or $authority == null {
		error make { msg: "the exported app is not signed with a Developer ID Application identity" }
	}
	if not ($details | str contains "Timestamp=") {
		error make { msg: "the exported app does not have a secure signing timestamp" }
	}
	$authority | str replace "Authority=" ""
}
