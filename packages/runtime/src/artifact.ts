import { assert as assert_ } from "./assert.ts";
import { Blob } from "./blob.ts";
import type { Checksum } from "./checksum.ts";
import { Directory } from "./directory.ts";
import { File } from "./file.ts";
import { Symlink } from "./symlink.ts";
import { build } from "./target.ts";

export type Artifact = Directory | File | Symlink;

export namespace Artifact {
	export type Id = string;

	export type ArchiveFormat = "tar" | "zip";

	export let withId = (id: Artifact.Id): Artifact => {
		let prefix = id.substring(0, 3);
		if (prefix === "dir") {
			return Directory.withId(id);
		} else if (prefix === "fil") {
			return File.withId(id);
		} else if (prefix === "sym") {
			return Symlink.withId(id);
		} else {
			throw new Error(`invalid artifact id: ${id}`);
		}
	};

	export let is = (value: unknown): value is Artifact => {
		return (
			value instanceof Directory ||
			value instanceof File ||
			value instanceof Symlink
		);
	};

	export let expect = (value: unknown): Artifact => {
		assert_(is(value));
		return value;
	};

	export let assert = (value: unknown): asserts value is Artifact => {
		assert_(is(value));
	};

	export let archive = async (
		artifact: Artifact,
		format: ArchiveFormat,
	): Promise<Blob> => {
		let value = await build({
			host: "builtin",
			args: ["archive", artifact, format],
		});
		assert_(Blob.is(value));
		return value;
	};

	export let extract = async (
		blob: Blob,
		format: ArchiveFormat,
	): Promise<Artifact> => {
		let value = await build({
			host: "builtin",
			args: ["extract", blob, format],
		});
		assert_(Artifact.is(value));
		return value;
	};

	export let bundle = async (artifact: Artifact): Promise<Artifact> => {
		let value = await build({
			host: "builtin",
			args: ["bundle", artifact],
		});
		assert_(Artifact.is(value));
		return value;
	};

	export let checksum = async (
		artifact: Artifact,
		algorithm: Checksum.Algorithm,
	): Promise<Checksum> => {
		let value = await build({
			host: "builtin",
			args: ["checksum", artifact, algorithm],
		});
		return value as Checksum;
	};
}
