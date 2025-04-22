import * as tg from "./index.ts";

export type Blob = tg.Leaf | tg.Branch;

export let blob = async (...args: tg.Args<Blob.Arg>) => {
	return await Blob.new(...args);
};

export declare namespace Blob {
	let new_: (...args: tg.Args<Blob.Arg>) => Promise<Blob>;
	export { new_ as new };
}

export namespace Blob {
	export type Arg = undefined | string | Uint8Array | Blob;

	export type Id = string;

	export let new_ = async (...args: tg.Args<Blob.Arg>): Promise<Blob> => {
		let resolved = await Promise.all(args.map(tg.resolve));
		let children = (
			await Promise.all(
				resolved.map(async (arg) => {
					if (arg === undefined) {
						return [];
					} else if (typeof arg === "string" || arg instanceof Uint8Array) {
						return [await syscall("blob_create", arg)];
					} else {
						return [arg];
					}
				}),
			)
		).flat(1);
		let blob: Blob;
		if (!children || children.length === 0) {
			blob = new tg.Leaf({
				object: { bytes: new Uint8Array() },
			});
		} else if (children.length === 1) {
			blob = children[0]!;
		} else {
			let children_ = await Promise.all(
				children.map(async (blob) => {
					return { blob, length: await blob.length() };
				}),
			);
			blob = new tg.Branch({
				object: { children: children_ },
			});
		}
		return blob;
	};
	Blob.new = new_;

	export let is = (value: unknown): value is Blob => {
		return value instanceof tg.Leaf || value instanceof tg.Branch;
	};

	export let expect = (value: unknown): Blob => {
		tg.assert(is(value));
		return value;
	};

	export let assert = (value: unknown): asserts value is Blob => {
		tg.assert(is(value));
	};
}
