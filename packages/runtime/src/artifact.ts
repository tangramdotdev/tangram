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
		compression?: tg.Blob.CompressionFormat,
	): Promise<tg.File> => {
		let value = await tg.build({
			args: [artifact, format, compression],
			executable: "archive",
			host: "builtin",
		});
		tg.assert(value instanceof tg.File);
		return value;
	};

	export let bundle = async (artifact: Artifact): Promise<Artifact> => {
		let value = await tg.build({
			args: [artifact],
			executable: "bundle",
			host: "builtin",
		});
		tg.assert(Artifact.is(value));
		return value;
	};

	export let compress = async (
		file: tg.File,
		format: tg.Blob.CompressionFormat,
	): Promise<tg.File> => {
		let value = await tg.build({
			args: [file, format],
			executable: "compress",
			host: "builtin",
		});
		tg.assert(value instanceof tg.File);
		return value;
	};

	export let checksum = async (
		artifact: Artifact,
		algorithm: tg.Checksum.Algorithm,
	): Promise<tg.Checksum> => {
		let value = await tg.build({
			args: [artifact, algorithm],
			executable: "checksum",
			host: "builtin",
		});
		return value as tg.Checksum;
	};

	export let decompress = async (file: tg.File): Promise<tg.File> => {
		let value = await tg.build({
			args: [file],
			executable: "decompress",
			host: "builtin",
		});
		tg.assert(value instanceof tg.File);
		return value;
	};

	export let download = async (
		url: string,
		checksum: tg.Checksum,
		extract?: boolean,
	): Promise<Artifact> => {
		let value = await tg.build({
			args: [url, extract],
			checksum,
			executable: "download",
			host: "builtin",
		});
		if (tg.Blob.is(value)) {
			return tg.file(value);
		} else if (tg.Artifact.is(value)) {
			return value;
		} else {
			throw new Error("expected a blob or an artifact");
		}
	};

	export let extract = async (file: tg.File): Promise<Artifact> => {
		let value = await tg.build({
			args: [file],
			executable: "extract",
			host: "builtin",
		});
		tg.assert(Artifact.is(value));
		return value;
	};
}
