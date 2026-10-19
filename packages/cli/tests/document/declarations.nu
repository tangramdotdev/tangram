use ../lib/test.nu *

# Documenting a module with each kind of declaration produces documentation JSON that matches the snapshot.

let local = server spawn

let path = artifact {
	tangram.ts: '
		/**
		 * Add two numbers.
		 * @param a - The first number.
		 * @param b The second number.
		 * @returns The sum.
		 * @example
		 * add(1, 2);
		 * @since 1.0
		 */
		export function add(a: number, b = 2): number { return a + b; }

		/**
		 * Subtract two numbers.
		 * @deprecated Use add with a negative number instead.
		 */
		export function subtract(a: number, b: number): number { return a - b; }

		/** @internal */
		export function secret(): void {}

		/** Echo a string. */
		export function echo(value: string): string;
		/** Echo a number. */
		export function echo(value: number): number;
		export function echo(value: any): any { return value; }

		export const identity = <T extends object>(value: T, ...rest: Array<string>) => value;

		/** A greeting. */
		export const greeting = "hello";

		export let config = { count: 1, name: "x" };

		export const entry = (): [key: string, value?: number] => ["key"];

		export enum Color { Red, Green = "green", Blue = 4 }

		interface Base {
			id: string;
		}

		/** A shape. */
		export interface Shape<T = string> extends Base {
			/** The name. */
			readonly name: T;
			area(): number;
			area(scale: number): number;
			[key: string]: unknown;
			(x: number): void;
			new (x: number): Shape;
		}

		export type Maybe<T> = T | undefined | null;
		export type Pair = [first: string, second?: number, ...rest: Array<boolean>];
		export type Keys = keyof Shape;
		export type Unwrap<T> = T extends Promise<infer U> ? U : never;
		export type Partial<T> = { readonly [K in keyof T]?: T[K] };
		export type Prefixed = `prefix-${string}`;
		export type Guard = (value: unknown) => value is number;

		abstract class Animal {
			id = "animal";
			protected abstract sound(): string;
			/** @hidden */
			hiddenMember = 0;
		}

		/** A dog. */
		export class Dog<T> extends Animal implements Base {
			static count: number = 0;
			private secret = 1;
			#hidden = 2;
			/** Create a dog. */
			constructor(public name: string, value?: T) { super(); }
			get loud(): boolean { return true; }
			protected sound(): string { return "woof"; }
			/**
			 * Bark.
			 * @param times The number of times to bark.
			 */
			bark(times: number): string { return ""; }
			handler: (event: string) => void = () => {};
			static create(): Dog<number> { return new Dog("rex"); }
		}

		export namespace Space {
			export const inner = 1;
		}

		export function file(value: tg.File): tg.File { return value; }

		export const directory = async () => tg.directory();

		export { add as plus };

		export default () => 42;
	'
}

# Document.
let output = do { cd $path; tg document } | complete
success $output

let json = $output.stdout | from json
snapshot --normalize ($json | to json --indent 2)
