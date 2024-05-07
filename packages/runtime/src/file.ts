import { Args } from "./args.ts";
import type { Artifact } from "./artifact.ts";
import { assert as assert_, unreachable } from "./assert.ts";
import { Blob, blob } from "./blob.ts";
import { Mutation, mutation } from "./mutation.ts";
import type { Object_ } from "./object.ts";
import { type Unresolved, resolve } from "./resolve.ts";
import type {
	MaybeMutationMap,
	MaybeNestedArray,
	MutationMap,
} from "./util.ts";

export let file = async (
	...args: Array<Unresolved<MaybeNestedArray<MaybeMutationMap<File.Arg>>>>
) => {
	return await File.new(...args);
};

export class File {
	#state: File.State;

	constructor(state: File.State) {
		this.#state = state;
	}

	get state(): File.State {
		return this.#state;
	}

	static withId(id: File.Id): File {
		return new File({ id });
	}

	static async new(
		...args: Array<Unresolved<MaybeNestedArray<MaybeMutationMap<File.Arg>>>>
	): Promise<File> {
		type Apply = {
			contents?: Array<Blob.Arg>;
			executable?: Array<boolean>;
			references?: Array<Artifact>;
		};
		let {
			contents: contents_,
			executable: executable_,
			references: references_,
		} = await Args.apply<File.Arg, Apply>(
			await Promise.all(args.map(resolve)),
			async (arg) => {
				if (arg === undefined) {
					return {};
				} else if (
					typeof arg === "string" ||
					arg instanceof Uint8Array ||
					Blob.is(arg)
				) {
					return {
						contents: await mutation({ kind: "array_append", values: [arg] }),
					};
				} else if (File.is(arg)) {
					return {
						contents: await mutation({
							kind: "array_append",
							values: [await arg.contents()],
						}),
						executable: await mutation({
							kind: "array_append",
							values: [await arg.executable()],
						}),
						references: await mutation({
							kind: "array_append",
							values: [await arg.references()],
						}),
					};
				} else if (typeof arg === "object") {
					let object: Partial<MutationMap<Apply>> = {};
					if (arg.contents !== undefined) {
						object.contents = Mutation.is(arg.contents)
							? arg.contents
							: await mutation({
									kind: "array_append",
									values: [arg.contents],
								});
					}
					if (arg.executable !== undefined) {
						object.executable = Mutation.is(arg.executable)
							? arg.executable
							: await mutation({
									kind: "array_append",
									values: [arg.executable],
								});
					}
					if (arg.references !== undefined) {
						object.references = Mutation.is(arg.references)
							? arg.references
							: await mutation({
									kind: "array_append",
									values: [arg.references],
								});
					}
					return object;
				} else {
					return unreachable();
				}
			},
		);
		let contents = await blob(contents_);
		let executable = (executable_ ?? []).some((executable) => executable);
		let references = references_ ?? [];
		return new File({
			object: { contents, executable, references },
		});
	}

	static is(value: unknown): value is File {
		return value instanceof File;
	}

	static expect(value: unknown): File {
		assert_(File.is(value));
		return value;
	}

	static assert(value: unknown): asserts value is File {
		assert_(File.is(value));
	}

	async id(): Promise<File.Id> {
		await this.store();
		return this.#state.id!;
	}

	async object(): Promise<File.Object_> {
		await this.load();
		return this.#state.object!;
	}

	async load() {
		if (this.#state.object === undefined) {
			let object = await syscall("object_load", this.#state.id!);
			assert_(object.kind === "file");
			this.#state.object = object.value;
		}
	}

	async store() {
		if (this.#state.id === undefined) {
			this.#state.id = await syscall("object_store", {
				kind: "file",
				value: this.#state.object!,
			});
		}
	}

	async contents(): Promise<Blob> {
		return (await this.object()).contents;
	}

	async executable(): Promise<boolean> {
		return (await this.object()).executable;
	}

	async references(): Promise<Array<Artifact>> {
		return (await this.object()).references;
	}

	async size(): Promise<number> {
		return (await this.contents()).size();
	}

	async bytes(): Promise<Uint8Array> {
		return (await this.contents()).bytes();
	}

	async text(): Promise<string> {
		return (await this.contents()).text();
	}
}

export namespace File {
	export type Arg = undefined | string | Uint8Array | Blob | File | ArgObject;

	export type ArgObject = {
		contents?: Blob.Arg;
		executable?: boolean;
		references?: Array<Artifact>;
	};

	export type Id = string;

	export type Object_ = {
		contents: Blob;
		executable: boolean;
		references: Array<Artifact>;
	};

	export type State = Object_.State<File.Id, File.Object_>;
}
