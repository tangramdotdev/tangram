import { assert as assert_ } from "./assert.ts";
import type { Blob } from "./blob.ts";
import type { Checksum } from "./checksum.ts";
import { Directory } from "./directory.ts";
import { File } from "./file.ts";
import { Symlink } from "./symlink.ts";

export type Artifact = Directory | File | Symlink;

/** Archive an artifact. **/
export let archive = async (
	artifact: Artifact,
	format: Artifact.ArchiveFormat,
): Promise<Blob> => {
	return await syscall("artifact_archive", artifact, format);
};

/** Extract an artifact from an archive. **/
export let extract = async (
	blob: Blob,
	format: Artifact.ArchiveFormat,
): Promise<Artifact> => {
	return await syscall("artifact_extract", blob, format);
};

/** Bundle an artifact. **/
export let bundle = async (artifact: Artifact): Promise<Artifact> => {
	return await syscall("artifact_bundle", artifact);
};

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
		return await syscall("artifact_archive", artifact, format);
	};

	export let extract = async (
		blob: Blob,
		format: ArchiveFormat,
	): Promise<Artifact> => {
		return await syscall("artifact_extract", blob, format);
	};

	export let bundle = async (artifact: Artifact): Promise<Artifact> => {
		return await syscall("artifact_bundle", artifact);
	};

	export let checksum = async (
		artifact: Artifact,
		algorithm: Checksum.Algorithm,
	): Promise<Checksum> => {
		return await syscall("artifact_checksum", artifact, algorithm);
	};
}
