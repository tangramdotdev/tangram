import { Args } from "./args.ts";
import type { Artifact } from "./artifact.ts";
import { assert as assert_, unreachable } from "./assert.ts";
import { Branch } from "./branch.ts";
import type { Checksum } from "./checksum.ts";
import * as encoding from "./encoding.ts";
import { Leaf } from "./leaf.ts";
import { mutation } from "./mutation.ts";

export type Blob = Leaf | Branch;

export let blob = async (...args: Args<Blob.Arg>) => {
	return await Blob.new(...args);
};

export let download = async (
	url: string,
	checksum: Checksum,
): Promise<Blob> => {
	return await Blob.download(url, checksum);
};

export declare namespace Blob {
	let new_: (...args: Args<Blob.Arg>) => Promise<Blob>;
	export { new_ as new };
}

export namespace Blob {
	export type Arg = undefined | string | Uint8Array | Blob | Array<Arg>;

	export type Id = string;

	export type Object_ = Array<[Blob, number]> | Uint8Array;

	export type ArchiveFormat = ".tar" | ".zip";

	export type CompressionFormat =
		| ".bz2"
		| ".gz"
		| ".lz"
		| ".xz"
		| ".zstd"
		| ".zst";

	export let new_ = async (...args: Args<Blob.Arg>): Promise<Blob> => {
		type Apply = { children: Array<Blob> };
		let { children: children_ } = await Args.apply<Blob.Arg, Apply>(
			args,
			async (arg) => {
				if (arg === undefined) {
					return {};
				} else if (typeof arg === "string") {
					let blob = new Leaf({
						object: { bytes: encoding.utf8.encode(arg) },
					});
					return {
						children: await mutation({
							kind: "array_append",
							values: [blob],
						}),
					};
				} else if (arg instanceof Uint8Array) {
					let blob = new Leaf({ object: { bytes: arg } });
					return {
						children: await mutation({
							kind: "array_append",
							values: [blob],
						}),
					};
				} else if (Blob.is(arg)) {
					return {
						children: await mutation({
							kind: "array_append",
							values: [arg],
						}),
					};
				} else {
					return unreachable();
				}
			},
		);
		if (!children_ || children_.length === 0) {
			return new Leaf({
				object: { bytes: new Uint8Array() },
			});
		} else if (children_.length === 1) {
			return children_[0]!;
		} else {
			let children = await Promise.all(
				children_.map<Promise<Branch.Child>>(async (blob) => {
					return { blob, size: await blob.size() };
				}),
			);
			return new Branch({
				object: { children },
			});
		}
	};
	Blob.new = new_;

	export let is = (value: unknown): value is Blob => {
		return Leaf.is(value) || Branch.is(value);
	};

	export let expect = (value: unknown): Blob => {
		assert_(is(value));
		return value;
	};

	export let assert = (value: unknown): asserts value is Blob => {
		assert_(is(value));
	};

	export let download = async (
		url: string,
		checksum: Checksum,
	): Promise<Blob> => {
		return await syscall("download", url, checksum);
	};

	export let archive = async (
		artifact: Artifact,
		format: Blob.ArchiveFormat,
	): Promise<Blob> => {
		return await syscall("archive", artifact, format);
	};
}
