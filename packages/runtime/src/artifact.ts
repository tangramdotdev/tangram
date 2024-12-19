import * as tg from "./index.ts";

export type Artifact = tg.Directory | tg.File | tg.Symlink;

export namespace Artifact {
	export type Id = string;

	export type ArchiveFormat = "tar" | "tgar" | "zip";

	export let withId = (id: Artifact.Id): Artifact => {
		let prefix = id.substring(0, 3);
		if (prefix === "dir") {
			return tg.Directory.withId(id);
		} else if (prefix === "fil") {
			return tg.File.withId(id);
		} else if (prefix === "sym") {
			return tg.Symlink.withId(id);
		} else {
			throw new Error(`invalid artifact id: ${id}`);
		}
	};

	export let is = (value: unknown): value is Artifact => {
		return (
			value instanceof tg.Directory ||
			value instanceof tg.File ||
			value instanceof tg.Symlink
		);
	};

	export let expect = (value: unknown): Artifact => {
		tg.assert(is(value));
		return value;
	};

	export let assert = (value: unknown): asserts value is Artifact => {
		tg.assert(is(value));
	};

	export let archive = async (
		artifact: Artifact,
		format: ArchiveFormat,
	): Promise<tg.Blob> => {
		let value = await (
			await tg.target({
				host: "builtin",
				args: ["archive", artifact, format],
				env: undefined,
			})
		).output();
		tg.assert(tg.Blob.is(value));
		return value;
	};

	export let extract = async (
		blob: tg.Blob,
		format: ArchiveFormat,
	): Promise<Artifact> => {
		let value = await (
			await tg.target({
				host: "builtin",
				args: ["extract", blob, format],
				env: undefined,
			})
		).output();
		tg.assert(Artifact.is(value));
		return value;
	};

	export let bundle = async (artifact: Artifact): Promise<Artifact> => {
		let value = await (
			await tg.target({
				host: "builtin",
				args: ["bundle", artifact],
				env: undefined,
			})
		).output();
		tg.assert(Artifact.is(value));
		return value;
	};

	export let checksum = async (
		artifact: Artifact,
		algorithm: tg.Checksum.Algorithm,
	): Promise<tg.Checksum> => {
		let value = await (
			await tg.target({
				host: "builtin",
				args: ["checksum", artifact, algorithm],
				env: undefined,
			})
		).output();
		return value as tg.Checksum;
	};
}
