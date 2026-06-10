use ../../test.nu *

# A nested JavaScript function invoked via tg.run receives a structured argument object and returns its computed result.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			return await tg.run(foo, {
				greeting: "Hello",
				answer: 42,
				nested: ["a", "b"],
			});
		}

		export function foo(arg) {
			return `${arg.greeting}, ${arg.answer}! ${arg.nested.join(",")}`;
		}
	',
}

let output = tg run $path | from json
assert ($output == "Hello, 42! a,b")
