#!/usr/bin/env nu

def main [] {
	let cwd = $env.PWD
	let languages = {
		javascript: 'tangram-javascript'
		typescript: 'tangram-typescript'
	}
	for language in ($languages | transpose zed tangram) {
		let api_url = $'https://api.github.com/repos/zed-industries/zed/contents/crates/languages/src/($language.zed)'
		let language_path = $'($cwd)/languages/($language.tangram)'
		rm -rf $language_path
		mkdir $language_path
		let files = http get $api_url | where type == 'file'
		for file in $files {
			let output_path = $'($language_path)/($file.name)'
			http get $file.download_url | save -f $output_path
			print -e $'downloaded ($file.download_url)'
		}
	}
}
