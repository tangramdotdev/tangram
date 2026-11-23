#!/usr/bin/env nu

def main [app: path, output: path] {
	let staging = ^mktemp -d | str trim
	try {
		^ditto $app ($staging | path join 'Tangram.app')
		^ln -s /Applications ($staging | path join Applications)
		^hdiutil create -srcfolder $staging -volname Tangram -fs HFS+ -format UDZO $output
	} catch {|error|
		rm -r $staging
		error make $error.raw
	}
	rm -r $staging
}
