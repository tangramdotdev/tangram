import type { Args } from "./args.ts";
import { assert as assert_ } from "./assert.ts";
import { Branch } from "./branch.ts";
import type { Checksum } from "./checksum.ts";
import { Leaf, leaf } from "./leaf.ts";
import { resolve } from "./resolve.ts";
import { flatten } from "./util.ts";

export type Blob = Leaf | Branch;

export let blob = async (...args: Args<Blob.Arg>) => {
	return await Blob.new(...args);
};

export let compress = async (
	blob: Blob,
	format: Blob.CompressionFormat,
): Promise<Blob> => {
	return await syscall("blob_compress", blob, format);
};

export let decompress = async (
	blob: Blob,
	format: Blob.CompressionFormat,
): Promise<Blob> => {
	return await syscall("blob_decompress", blob, format);
};

export let download = async (
	url: string,
	checksum: Checksum,
): Promise<Blob> => {
	return await syscall("blob_download", url, checksum);
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
		return await syscall("blob_compress", blob, format);
	};

	export let decompress = async (
		blob: Blob,
		format: CompressionFormat,
	): Promise<Blob> => {
		return await syscall("blob_decompress", blob, format);
	};

	export let download = async (
		url: string,
		checksum: Checksum,
	): Promise<Blob> => {
		return await syscall("blob_download", url, checksum);
	};

	export let checksum = async (
		blob: Blob,
		algorithm: Checksum.Algorithm,
	): Promise<Checksum> => {
		return await syscall("blob_checksum", blob, algorithm);
	};
}
