#!/usr/bin/env nu

use ./identifiers.nu

def step [message: string] {
	print $"\n==> ($message)"
}

def main [
	--install
	--notary-profile: string
] {
	let root = ($env.FILE_PWD | path dirname | path dirname)
	let path = ($root | path join "packages/macos")
	let build_path = ($path | path join "build/Distribution")
	let archive_path = ($build_path | path join "Tangram.xcarchive")
	let derived_data_path = ($build_path | path join "DerivedData")
	let export_path = ($build_path | path join "Export")
	let app_path = ($export_path | path join "Tangram.app")
	let notarization_path = ($build_path | path join "Tangram-notarization.zip")
	let artifact_path = ($build_path | path join "Tangram.zip")
	let project = ($path | path join "Tangram.xcodeproj")
	let result_bundle_path = ($build_path | path join "TangramArchive.xcresult")
	let source_path = ($root | path join "target/release/tangram")
	let export_options_path = ($path | path join "ExportOptions.plist")
	let profile = if ($notary_profile | is-empty) {
		$env.TANGRAM_NOTARY_PROFILE? | default "Tangram"
	} else {
		$notary_profile
	}
	let doctor = ($root | path join "scripts/macos/doctor.nu")

	cd $root
	step "Checking the macOS release environment"
	^nu $doctor --release --notary-profile $profile

	step "Building the Tangram helper"
	^cargo build --release -p tangram_cli -p tangram_vfs_provider

	step "Generating the Xcode project"
	$env.TANGRAM_DEVELOPMENT_TEAM = (identifiers).development_team
	^xcodegen generate --spec ($path | path join "project.yml")

	step "Archiving Tangram"
	rm -rf $build_path
	mkdir $build_path
	let arch = (^uname -m | str trim)
	(
		xcodebuild
			-allowProvisioningUpdates
			-archivePath $archive_path
			-configuration Release
			-derivedDataPath $derived_data_path
			-destination $"platform=macOS,arch=($arch)"
			-project $project
			-resultBundlePath $result_bundle_path
			-scheme Tangram
			-showBuildTimingSummary
			$"ARCHS=($arch)"
			$"TANGRAM_HELPER_PATH=($source_path)"
			archive
	)

	let archived_helper = ($archive_path | path join "Products/Applications/Tangram.app/Contents/Helpers/tangram")
	if not ($archived_helper | path exists) {
		error make { msg: "the archive does not contain the Tangram helper" }
	}

	step "Exporting Tangram with Developer ID signing"
	(
		xcodebuild
			-allowProvisioningUpdates
			-archivePath $archive_path
			-exportArchive
			-exportOptionsPlist $export_options_path
			-exportPath $export_path
	)
	if not ($app_path | path exists) {
		error make { msg: $"the exported app was not found at ($app_path)" }
	}
	^codesign --verify --deep --strict --verbose=2 $app_path
	let signature = (^codesign -dv --verbose=4 $app_path | complete)
	let signature_details = $signature.stdout + $signature.stderr
	if $signature.exit_code != 0 or not ($signature_details | str contains "Authority=Developer ID Application") {
		error make { msg: "the exported app is not signed with a Developer ID Application identity" }
	}
	if not ($signature_details | str contains "Timestamp=") {
		error make { msg: "the exported app does not have a secure signing timestamp" }
	}

	step "Submitting Tangram for notarization"
	^ditto -c -k --keepParent $app_path $notarization_path
	^xcrun notarytool submit $notarization_path --keychain-profile $profile --wait

	step "Stapling and verifying the notarization ticket"
	^xcrun stapler staple $app_path
	^xcrun stapler validate $app_path
	^codesign --verify --deep --strict --verbose=2 $app_path
	^spctl -a -vvv -t exec $app_path

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

	print $"\nTangram was signed, notarized, stapled, and written to ($artifact_path)."
}
