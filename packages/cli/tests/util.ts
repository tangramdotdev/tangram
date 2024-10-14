import { $ } from "bun";

type Arg = {
	[key: string]: undefined | string | Arg;
};

export async function directory(...args: Array<Arg>): Promise<string> {
	const path = await $`mktemp -d`.text().then((t) => t.trim());
	await inner(path, ...args);
	return path;
}

async function inner(path: string, ...args: Array<Arg>) {
	for (let arg of args) {
		if (arg === undefined) {
			continue;
		}
		for (let [key, value] of Object.entries(arg)) {
			let [firstComponent, ...trailingComponents] = key.split("/");
			if (!firstComponent) {
				throw new Error("the path cannot be empty");
			}
			const newPath = `${path}/${firstComponent}`;
			if (trailingComponents.length === 0) {
				if (value === undefined) {
					await $`rm -rf ${newPath}`;
				} else if (typeof value === "string") {
					await Bun.write(newPath, value);
				} else if (typeof value === "object") {
					await $`mkdir -p ${newPath}`;
					await inner(newPath, value);
				}
			} else {
				await $`mkdir -p ${newPath}`;
				const newKey = trailingComponents.join("/");
				await inner(newPath, { [newKey]: value });
			}
		}
	}
}
