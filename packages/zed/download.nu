#!/usr/bin/env nu

def main [] {
	let cwd = $env.PWD
	let files = [
		'brackets.scm'
		'contexts.scm'
		'embedding.scm'
		'highlights.scm'
		'imports.scm'
		'indents.scm'
		'injections.scm'
		'outline.scm'
		'overrides.scm'
		'runnables.scm'
		'textobjects.scm'
	]
	let languages = {
		javascript: 'tangram-javascript'
		typescript: 'tangram-typescript'
	}
	for language in ($languages | transpose zed tangram) {
		let zed_name = $language.zed
		let tangram_name = $language.tangram
		let url = $'https://raw.githubusercontent.com/zed-industries/zed/main/crates/languages/src/($zed_name)'
		let language_path = $'($cwd)/languages/($tangram_name)'
		mkdir $language_path
		for file in $files {
			if (($language.zed == 'typescript') and ($file == 'contexts.scm')) {
				continue
			}
			let url = $'($url)/($file)'
			let output_path = $'($language_path)/($file)'
			http get $url | save -f $output_path
			print -e $'downloaded ($url)'
		}
	}
}
