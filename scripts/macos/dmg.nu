#!/usr/bin/env nu

def main [app: path, output: path] {
	let script = ($env.FILE_PWD | path join 'dmg.py')
	^uv run --script $script ($app | path expand) ($output | path expand)
}
