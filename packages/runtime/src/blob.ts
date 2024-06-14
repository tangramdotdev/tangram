import type { Args } from "./args.ts";
import { assert as assert_ } from "./assert.ts";
import { Branch } from "./branch.ts";
import { Checksum } from "./checksum.ts";
import { Leaf, leaf } from "./leaf.ts";
import { resolve } from "./resolve.ts";
import { target } from "./target.ts";
import { flatten } from "./util.ts";

export type Blob = Leaf | Branch;

export let blob = async (...args: Args<Blob.Arg>) => {
	return await Blob.new(...args);
};

export declare namespace Blob {
	let new_: (...args: Args<Blob.Arg>) => Promise<Blob>;
	export { new_ as new };
}

export namespace Blob {
	export type Arg = undefined | string | Uint8Array | Blob;

	export type Id = string;

	export type CompressionFormat = "bz2" | "gz" | "xz" | "zst";

	export let new_ = async (...args: Args<Blob.Arg>): Promise<Blob> => {
		let resolved = await Promise.all(args.map(resolve));
		let flattened = flatten(resolved);
		let children = (
			await Promise.all(
				flattened.map(async (arg) => {
					if (arg === undefined) {
						return [];
					} else if (typeof arg === "string") {
						return [await leaf(arg)];
					} else if (arg instanceof Uint8Array) {
						return [await leaf(arg)];
					} else {
						return [arg];
					}
				}),
			)
		).flat(1);
		let blob: Blob;
		if (!children || children.length === 0) {
			blob = new Leaf({
				object: { bytes: new Uint8Array() },
			});
		} else if (children.length === 1) {
			blob = children[0]!;
		} else {
			let children_ = await Promise.all(
				children.map(async (blob) => {
					return { blob, size: await blob.size() };
				}),
			);
			blob = new Branch({
				object: { children: children_ },
			});
		}
		return blob;
	};
	Blob.new = new_;

	export let is = (value: unknown): value is Blob => {
		return value instanceof Leaf || value instanceof Branch;
	};

	export let expect = (value: unknown): Blob => {
		assert_(is(value));
		return value;
	};

	export let assert = (value: unknown): asserts value is Blob => {
		assert_(is(value));
	};

	export let compress = async (
		blob: Blob,
		format: CompressionFormat,
	): Promise<Blob> => {
		let value = await (
			await target({
				host: "builtin",
				args: ["compress", blob, format],
				env: undefined,
			})
		).output();
		assert_(Blob.is(value));
		return value;
	};

	export let decompress = async (
		blob: Blob,
		format: CompressionFormat,
	): Promise<Blob> => {
		let value = await (
			await target({
				host: "builtin",
				args: ["decompress", blob, format],
				env: undefined,
			})
		).output();
		assert_(Blob.is(value));
		return value;
	};

	export let download = async (
		url: string,
		checksum: Checksum,
	): Promise<Blob> => {
		let value = await (
			await target({
				host: "builtin",
				args: ["download", url],
				checksum: "unsafe",
				env: undefined,
			})
		).output();
		assert_(Blob.is(value));
		let algorithm = Checksum.algorithm(checksum);
		let actual = await (
			await target({
				host: "builtin",
				args: ["checksum", value, algorithm],
				env: undefined,
			})
		).output();
		if (actual !== checksum) {
			throw new Error(
				`invalid checksum, expected ${checksum} but got ${actual}`,
			);
		}
		return value;
	};

	export let checksum = async (
		blob: Blob,
		algorithm: Checksum.Algorithm,
	): Promise<Checksum> => {
		let value = await (
			await target({
				host: "builtin",
				args: ["checksum", blob, algorithm],
				env: undefined,
			})
		).output();
		return value as Checksum;
	};
}
