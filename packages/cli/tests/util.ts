import { $ } from "bun";
import fs from "node:fs/promises";
import path from "node:path";

const fileSymbol = Symbol();

type Arg = {
	[key: string]: undefined | string | File | Arg;
};

type File = {
	[fileSymbol]: true;
	contents: string;
	executable?: boolean;
};

export async function directory(...args: Array<Arg>): Promise<string> {
	const path = await $`mktemp -d`.text().then((t) => t.trim());
	await directoryInner(path, ...args);
	return path;
}

async function directoryInner(path: string, ...args: Array<Arg>) {
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
					if (fileSymbol in value) {
						await Bun.write(newPath, value.contents, {
							mode: value.executable ? 0o111 : 0o110,
						});
					} else {
						await $`mkdir -p ${newPath}`;
						await directoryInner(newPath, value);
					}
				}
			} else {
				await $`mkdir -p ${newPath}`;
				const newKey = trailingComponents.join("/");
				await directoryInner(newPath, { [newKey]: value });
			}
		}
	}
}

export function file(arg: { contents: string; executable?: boolean }): File {
	return {
		[fileSymbol]: true,
		...arg,
	};
}

export async function compare(path1: string, path2: string): Promise<boolean> {
	const [stat1, stat2] = await Promise.all([fs.lstat(path1), fs.lstat(path2)]);
	if (stat1.isDirectory() && stat2.isDirectory()) {
		const [contents1, contents2] = await Promise.all([
			fs.readdir(path1),
			fs.readdir(path2),
		]);
		if (contents1.length !== contents2.length) {
			return false;
		}
		contents1.sort();
		contents2.sort();
		for (let i = 0; i < contents1.length; i++) {
			let item1 = contents1[i]!;
			let item2 = contents2[i]!;
			if (item1 !== item2) {
				return false;
			}
			const itemPath1 = path.join(path1, item1);
			const itemPath2 = path.join(path2, item2);
			const equal = await compare(itemPath1, itemPath2);
			if (!equal) {
				return false;
			}
		}
		return true;
	} else if (stat1.isFile() && stat2.isFile()) {
		if (stat1.size !== stat2.size) {
			return false;
		}
		const [contents1, contents2] = await Promise.all([
			Bun.file(path1).text(),
			Bun.file(path2).text(),
		]);
		if (contents1 !== contents2) {
			return false;
		}
		let executable1 = stat1.mode & 0o111;
		let executable2 = stat2.mode & 0o111;
		if (executable1 !== executable2) {
			console.log("e");
			return false;
		}
		return true;
	} else if (stat1.isSymbolicLink() && stat2.isSymbolicLink()) {
		const [link1, link2] = await Promise.all([
			fs.readlink(path1),
			fs.readlink(path2),
		]);
		if (link1 !== link2) {
			return false;
		}
		return true;
	} else {
		return false;
	}
}
