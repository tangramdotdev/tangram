use ../../lib/test.nu *

# Documenting a JavaScript module uses the types from its JSDoc comments.

let local = server spawn

let path = artifact {
	tangram.js: '
		/**
		 * Greet someone.
		 * @param {string} name - The name to greet.
		 * @param {number} [times] - The number of times to greet.
		 * @returns {string} The greeting.
		 */
		export function greet(name, times = 1) {
			return `Hello, ${name}!`.repeat(times);
		}

		/** @type {Array<number>} */
		export const numbers = [1, 2, 3];
	'
}

# Document.
let output = do { cd $path; tg document } | complete
success $output

let json = $output.stdout | from json
snapshot --normalize ($json | to json --indent 2)
