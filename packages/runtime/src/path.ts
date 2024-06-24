import * as tg from "./index.ts";
import type { MaybeNestedArray } from "./util.ts";

export let path = (...args: Array<MaybeNestedArray<Path.Arg>>): Path => {
	return Path.new(args);
};

export class Path {
	#components: Array<Path.Component>;

	static new(...args: Array<MaybeNestedArray<Path.Arg>>): Path {
		return args.reduce(function reduce(
			path: Path,
			arg: MaybeNestedArray<Path.Arg>,
		): Path {
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
					} else if (component === Path.Component.Current) {
						path.push(Path.Component.Current);
					} else if (component === Path.Component.Parent) {
						path.push(Path.Component.Parent);
					} else {
						path.push(component);
					}
				}
				return path;
			} else if (arg instanceof Path) {
				for (let component of arg.#components) {
					path.push(component);
				}
				return path;
			} else if (arg instanceof Array) {
				for (let arg_ of arg) {
					path = reduce(path, arg_);
				}
				return path;
			} else {
				return tg.unreachable();
			}
		}, new Path());
	}

	private constructor(components: Array<Path.Component> = []) {
		this.#components = [Path.Component.Current];
		for (let component of components) {
			this.push(component);
		}
	}

	get components(): Array<Path.Component> {
		return this.#components;
	}

	push(component: Path.Component) {
		let last = this.#components.at(-1)!;
		if (
			component === Path.Component.Current ||
			(component === Path.Component.Parent && last === Path.Component.Root)
		) {
			// If the component is a current component, or is a parent component and follows a root component, then ignore it.
			return;
		} else if (
			component === Path.Component.Parent &&
			(Path.Component.isNormal(last) || last === Path.Component.Parent)
		) {
			// If the component is a parent component and follows a normal or parent component, then add it.
			this.#components.push(component);
		} else if (
			component === Path.Component.Parent &&
			last === Path.Component.Current
		) {
			// If the component is a parent component and follows a current component, then replace the path with a parent component.
			this.#components = [Path.Component.Parent];
		} else if (component === Path.Component.Root) {
			// If the component is a root component, then replace the path with a root component.
			this.#components = [Path.Component.Root];
		} else if (Path.Component.isNormal(component)) {
			// If the component is a normal component, then add it.
			this.#components.push(component);
		}
	}

	parent(): Path {
		return this.join(Path.Component.Parent);
	}

	join(other: Path.Arg): Path {
		let path = new Path(this.components);
		for (let component of Path.new(other).components) {
			path.push(component);
		}
		return path;
	}

	normalize(): Path {
		let path = new Path();
		for (let component of this.components) {
			let last = path.components.at(-1);
			if (
				component === Path.Component.Parent &&
				last !== undefined &&
				Path.Component.isNormal(last)
			) {
				// If the component is a parent component following a normal component, then remove the normal component.
				path.#components.pop();
			} else {
				// Otherwise, add the component.
				path.push(component);
			}
		}
		return path;
	}

	isInternal(): boolean {
		return this.components.at(0)! === Path.Component.Current;
	}

	isExternal(): boolean {
		return this.components.at(0)! === Path.Component.Parent;
	}

	isAbsolute(): boolean {
		return this.components.at(0)! === Path.Component.Root;
	}

	static expect(value: unknown): Path {
		tg.assert(value instanceof Path);
		return value;
	}

	static assert(value: unknown): asserts value is Path {
		tg.assert(value instanceof Path);
	}

	toString(): string {
		let string = "";
		for (let i = 0; i < this.#components.length; i++) {
			let component = this.#components[i]!;
			if (component === Path.Component.Current) {
				if (i !== 0) {
					string += "/";
				}
				string += ".";
			} else if (component === Path.Component.Parent) {
				if (i !== 0) {
					string += "/";
				}
				string += "..";
			} else if (component === Path.Component.Root) {
				string += "/";
			} else {
				if (i !== 0) {
					string += "/";
				}
				string += component;
			}
		}
		return string;
	}
}

export namespace Path {
	export type Arg = undefined | Component | Path;

	export type Component =
		| Component.Normal
		| Component.Current
		| Component.Parent
		| Component.Root;

	export namespace Component {
		export type Normal = string;

		export type Current = ".";

		export let Current = ".";

		export type Parent = "..";

		export let Parent = "..";

		export type Root = "/";

		export let Root = "/";

		export let isNormal = (component: Component): component is Normal => {
			return (
				typeof component === "string" &&
				component !== Current &&
				component !== Parent &&
				component !== Root
			);
		};
	}
}
