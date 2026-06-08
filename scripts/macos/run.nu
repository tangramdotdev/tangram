#!/usr/bin/env nu

def main [--release] {
	let configuration = if $release { "Release" } else { "Debug" }
	let app_name = if $release { "Tangram" } else { "Tangram Dev" }
	let built_app = $"packages/macos/build/($configuration)/($app_name).app"
	let installed_app = ($env.HOME | path join "Applications" $"($app_name).app")
	let build_args = if $release { ["--release"] } else { [] }
	nu scripts/macos/build.nu ...$build_args
	mkdir ($installed_app | path dirname)
	rm -rf $installed_app
	ditto $built_app $installed_app
	^pluginkit -r ($built_app | path join "Contents/Extensions/TangramFSKit.appex") | complete | ignore
	^pluginkit -r ($"packages/macos/build/($configuration)/TangramFSKit.appex") | complete | ignore
	rm -rf $built_app
	rm -rf $"packages/macos/build/($configuration)/TangramFSKit.appex"
	^/System/Library/Frameworks/CoreServices.framework/Frameworks/LaunchServices.framework/Support/lsregister -f -R -trusted $installed_app
	^pluginkit -r ($installed_app | path join "Contents/Extensions/TangramFSKit.appex") | complete | ignore
	^pluginkit -a ($installed_app | path join "Contents/Extensions/TangramFSKit.appex")
	try {
		osascript -e $"tell application \"($app_name)\" to quit"
	}
	^open -n $installed_app
}
