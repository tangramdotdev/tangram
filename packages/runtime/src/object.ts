import type { Branch } from "./branch.ts";
import type { Directory } from "./directory.ts";
import type { File } from "./file.ts";
import type { Leaf } from "./leaf.ts";
import type { Lock } from "./lock.ts";
import type { Symlink } from "./symlink.ts";
import type { Target } from "./target.ts";

export type Object_ =
	| Leaf
	| Branch
	| Directory
	| File
	| Symlink
	| Lock
	| Target;

export namespace Object_ {
	export type Id = string;

	export type Kind =
		| "leaf"
		| "branch"
		| "directory"
		| "file"
		| "symlink"
		| "lock"
		| "target";

	export type Object_ =
		| { kind: "leaf"; value: Leaf.Object_ }
		| { kind: "branch"; value: Branch.Object_ }
		| { kind: "directory"; value: Directory.Object_ }
		| { kind: "file"; value: File.Object_ }
		| { kind: "symlink"; value: Symlink.Object_ }
		| { kind: "lock"; value: Lock.Object_ }
		| { kind: "target"; value: Target.Object_ };

	export type State<I, O> = {
		id?: I | undefined;
		object?: O | undefined;
	};
}
