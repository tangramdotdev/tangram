use ../../../test.nu *

# The printer omits absent mutation separators and preserves empty payloads and explicit separators.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let template = await tg.template("x");
			for (let kind of ["prefix", "suffix"] as const) {
				for (let separator of [undefined, null, "", ":"]) {
					let mutation = new tg.Mutation<tg.Template>({ kind, separator, template });
					for (let indentation of [null, "  "]) {
						let output = tg.Value.print(mutation, { indentation });
						if (separator === undefined || separator === null) {
							tg.assert(!output.includes(`"separator":`), output);
						} else {
							let space = indentation === null ? "" : " ";
							tg.assert(output.includes(`"separator":${space}${JSON.stringify(separator)}`), output);
						}
					}
				}
			}
			for (let value of [null, [], {}, ""]) {
				for (let mutation of [await tg.Mutation.set(value), await tg.Mutation.setIfUnset(value)]) {
					let output = tg.Value.print(mutation);
					tg.assert(output.includes(`"value":${JSON.stringify(value)}`), output);
				}
			}
			for (let mutation of [await tg.Mutation.append([]), await tg.Mutation.prepend([])]) {
				let output = tg.Value.print(mutation);
				tg.assert(output.includes(`"values":[]`), output);
			}
			let merge = tg.Value.print(await tg.Mutation.merge({}));
			tg.assert(merge.includes(`"value":{}`), merge);
			let map = { options: {}, separator: null, values: [] };
			tg.assert(tg.Value.print(map) === JSON.stringify(map));
			return true;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
