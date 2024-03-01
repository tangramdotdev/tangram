import { unreachable } from "./assert.ts";

export class Path {
	#components: Array<Path.Component>;

	static new(...args: Array<Path.Arg>): Path {
		return args.reduce(function reduce(path: Path, arg: Path.Arg): Path {
			if (arg === undefined) {
				return path;
			} else if (typeof arg === "string") {
				if (arg.startsWith("/")) {
					path.push(Path.Component.Root);
					arg = arg.slice(1);
				}
				for (let component of arg.split("/")) {
					if (component === "") {
						continue;
					} else if (component === ".") {
						path.push(".");
					} else if (component === "..") {
						path.push("..");
					} else {
						path.push(component);
					}
				}
				return path;
			} else if (arg instanceof Path) {
				return path.join(arg);
			} else if (arg instanceof Array) {
				for (let arg_ of arg) {
					path = reduce(path, arg_);
				}
				return path;
			} else if (Path.Component.isRoot(arg)) {
				path.push(Path.Component.Root);
				return path;
			} else {
				return unreachable();
			}
		}, new Path([]));
	}

	private constructor(components: Array<Path.Component>) {
		this.#components = components;
	}

	isEmpty(): boolean {
		return this.#components.length === 0;
	}

	components(): Array<Path.Component> {
		return this.#components;
	}

	push(component: Path.Component): void {
		// Ignore the component if it is a current directory component and the path is not empty.
		if (component === "." && !this.isEmpty()) {
			return;
		}

		// If the component is a root component, then clear the path.
		if (Path.Component.isRoot(component)) {
			this.#components.length = 0;
		}

		// Add the component.
		this.#components.push(component);
	}

	pop() {
		this.#components.pop();
	}

	parent(): Path {
		return this.join("..");
	}

	join(other: Path.Arg): Path {
		let path = new Path(this.components());
		for (let component of Path.new(other).components()) {
			path.push(component);
		}
		return path;
	}

	normalize(): Path {
		let path = new Path([]);
		for (let component of this.components()) {
			let lastComponent = path.components().at(-1);
			if (
				component === ".." &&
				lastComponent !== undefined &&
				Path.Component.isNormal(lastComponent)
			) {
				path.pop();
			} else if (
				component === ".." &&
				lastComponent !== undefined &&
				Path.Component.isRoot(lastComponent)
			) {
				continue;
			} else {
				path.push(component);
			}
		}
		return path;
	}

	toString(): string {
		let string = "";
		for (let i = 0; i < this.components().length; i++) {
			let component = this.components()[i]!;
			if (Path.Component.isRoot(component)) {
				string += "/";
			} else if (component === ".") {
				if (i !== 0) {
					string += "/";
				}
				string += ".";
			} else if (component === "..") {
				if (i !== 0) {
					string += "/";
				}
				string += "..";
			} else {
				if (i !== 0) {
					string += "/";
				}
				string += component;
			}
		}
		return string;
	}

	isAbsolute(): boolean {
		let firstComponent = this.components().at(0);
		if (firstComponent === undefined) {
			return false;
		}
		return Path.Component.isRoot(firstComponent);
	}

	extension(): string | undefined {
		let lastComponent = this.components().at(-1);
		if (lastComponent === undefined) {
			return undefined;
		}
		if (Path.Component.isNormal(lastComponent)) {
			let components = lastComponent.split(".");
			if (components.length > 1) {
				return components.at(-1);
			}
		}
		return undefined;
	}
}

export namespace Path {
	export type Arg = undefined | Component | Path | Array<Arg>;

	export type Component =
		| Component.Root
		| Component.Current
		| Component.Parent
		| Component.Normal;

	export namespace Component {
		export type Root = { kind: "root" };

		export let Root = { kind: "root" } as const;

		export type Parent = "..";

		export let Parent = "..";

		export type Current = ".";

		export let Current = ".";

		export type Normal = string;

		export let isRoot = (component: Component): component is Root => {
			return typeof component === "object" && component.kind === "root";
		};

		export let isNormal = (component: Component): component is Normal => {
			return (
				typeof component === "string" && component !== "." && component !== ".."
			);
		};
	}
}
