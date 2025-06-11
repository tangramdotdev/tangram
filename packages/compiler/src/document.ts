import ts from "typescript";
import { assert } from "./assert.ts";
import type { Module } from "./module.ts";
import type { Range } from "./range.ts";
import { compilerOptions, host } from "./typescript.ts";
import * as typescript from "./typescript.ts";

declare module "typescript" {
	interface TypeChecker {
		// https://github.com/microsoft/TypeScript/blob/v4.7.2/src/compiler/types.ts#L4188
		getTypeOfSymbol(symbol: Symbol): Type;
	}
}

export type Request = {
	module: Module;
};

export type Response = {
	exports: SymbolKindDeclarationIndex;
};

type SymbolKindDeclarationIndex = { [symbolName: string]: DeclarationsByKind };

export type DeclarationsByKind = {
	class?: Array<ClassDeclaration>;
	enum?: Array<EnumDeclaration>;
	function?: Array<FunctionDeclaration>;
	interface?: Array<InterfaceDeclaration>;
	namespace?: Array<NamespaceDeclaration>;
	type?: Array<TypeDeclaration>;
	variable?: Array<VariableDeclaration>;
};

type Symbol = {
	declarations: Array<Declaration>;
};

type Declaration =
	| { kind: "class"; value: ClassDeclaration }
	| { kind: "enum"; value: EnumDeclaration }
	| { kind: "function"; value: FunctionDeclaration }
	| { kind: "interface"; value: InterfaceDeclaration }
	| { kind: "namespace"; value: NamespaceDeclaration }
	| { kind: "type"; value: TypeDeclaration }
	| { kind: "variable"; value: VariableDeclaration };

type ClassDeclaration = {
	location: Location;
	constructSignature?: Signature | undefined;
	callSignature?: Signature | undefined;
	properties: Array<ClassProperty>;
	comment: Comment;
	typeParameters: { [key: string]: TypeParameter };
};

type Signature = {
	parameters: { [key: string]: Parameter };
	return: Type;
};

type ClassProperty = {
	name: string;
	type: Type;
	static?: boolean;
	accessor?: "get" | "set" | undefined;
	private?: boolean;
	comment: Comment;
};

type EnumDeclaration = {
	location: Location;
	members: { [key: string]: EnumMemberValue };
	comment: Comment;
};

type EnumMemberValue = {
	constantValue: EnumMemberConstantValue | undefined;
	initializer: string | undefined;
};

type EnumMemberConstantValue =
	| { kind: "number"; value: number }
	| { kind: "string"; value: string };

type FunctionDeclaration = {
	location: Location;
	signature: FunctionSignature;
	comment: Comment;
};

type FunctionSignature = {
	location: Location;
	parameters: { [key: string]: Parameter };
	typeParameters: { [key: string]: TypeParameter };
	return: Type;
};

type InterfaceDeclaration = {
	location: Location;
	indexSignature: IndexSignature | undefined;
	properties: { [key: string]: InterfaceProperty };
	constructSignatures: Array<Signature>;
	comment: Comment;
	typeParameters: { [key: string]: TypeParameter };
};

type InterfaceProperty = {
	type: Type;
	readonly: boolean | undefined;
};

type NamespaceDeclaration = {
	location: Location;
	exports: SymbolKindDeclarationIndex;
	comment: Comment;
};

type TypeDeclaration = {
	location: Location;
	typeParameters: { [key: string]: TypeParameter };
	type: Type;
	comment: Comment;
};

type VariableDeclaration = {
	location: Location;
	type: Type;
	comment: Comment;
};

type DefaultExportDeclaration = {
	location: Location;
	type: Type;
	comment: Comment;
};

type Type =
	| { kind: "array"; value: ArrayType }
	| { kind: "conditional"; value: ConditionalType }
	| { kind: "function"; value: FunctionType }
	| { kind: "indexed_access"; value: IndexedAccessType }
	| { kind: "infer"; value: InferType }
	| { kind: "intersection"; value: IntersectionType }
	| { kind: "keyword"; value: KeywordType }
	| { kind: "literal"; value: LiteralType }
	| { kind: "mapped"; value: MappedType }
	| { kind: "object"; value: ObjectType }
	| { kind: "other"; value: string }
	| { kind: "predicate"; value: PredicateType }
	| { kind: "reference"; value: ReferenceType }
	| { kind: "template_literal"; value: TemplateLiteralType }
	| { kind: "tuple"; value: TupleType }
	| { kind: "type_operator"; value: TypeOperatorType }
	| { kind: "type_query"; value: TypeQueryType }
	| { kind: "union"; value: UnionType };

type ArrayType = {
	type: Type;
};

type ConditionalType = {
	checkType: Type;
	extendsType: Type;
	trueType: Type;
	falseType: Type;
};

type FunctionType = {
	signatures: Array<FunctionSignature>;
};

type Parameter = {
	optional: boolean;
	dotDotDotToken: boolean;
	type: Type;
};

type TypeParameter = {
	constraint: Type | undefined;
	default: Type | undefined;
};

type IndexedAccessType = {
	objectType: Type;
	indexType: Type;
};

type InferType = {
	typeParameter: { [key: string]: TypeParameter };
};

type IntersectionType = {
	types: Array<Type>;
};

type KeywordType =
	| "any"
	| "bigint"
	| "boolean"
	| "never"
	| "null"
	| "number"
	| "string"
	| "symbol"
	| "undefined"
	| "unknown"
	| "void";

type LiteralType =
	| { kind: "undefined" }
	| { kind: "null" }
	| { kind: "boolean"; value: boolean }
	| { kind: "number"; value: number }
	| { kind: "string"; value: string };

type MappedType = {
	type: Type;
	typeParameterName: string;
	constraint: Type;
	nameType: Type | undefined;
};

type ObjectType = {
	properties: { [key: string]: ObjectProperty };
	indexSignature: IndexSignature | undefined;
	constructSignatures: Array<Signature>;
};

type ObjectProperty = {
	optional: boolean;
	type: Type;
};

type IndexSignature = {
	name: string;
	key: Parameter;
	type: Type;
};

type PredicateType = {
	name: string;
	asserts: boolean;
	type: Type | undefined;
};

type ReferenceType = {
	location: Location;
	isExported: boolean;
	name: string;
	fullyQualifiedName: string;
	typeArguments: Array<Type>;
	isTypeParameter: boolean;
};

type TemplateLiteralType = {
	head: string;
	templateSpans: Array<TemplateLiteralTypeSpan>;
};

type TemplateLiteralTypeSpan = {
	type: Type;
	literal: string;
};

type TupleType = {
	types: Array<Type>;
};

type TypeOperatorType = {
	operator: string;
	type: Type;
};

type TypeQueryType = {
	name: string;
};

type UnionType = {
	types: Array<Type>;
};

type Location = {
	module: Module;
	range: Range;
};

type Comment = {
	text: string;
	tags: Array<{ name: string; comment: string }>;
};

export let handle = (request: Request): Response => {
	// Create the program and type checker.
	let program = ts.createProgram({
		rootNames: [typescript.fileNameFromModule(request.module)],
		options: compilerOptions,
		host,
	});
	let typeChecker = program.getTypeChecker();

	// Get the module's exports.
	let sourceFile = program.getSourceFile(
		typescript.fileNameFromModule(request.module),
	)!;
	let symbol = typeChecker.getSymbolAtLocation(sourceFile)!;
	let exports_: any;
	if (!symbol) {
		exports_ = typeChecker
			.getSymbolsInScope(sourceFile, ts.SymbolFlags.ModuleMember)
			.filter((s) =>
				s.getDeclarations()?.some((d) => d.getSourceFile() === sourceFile),
			);
	} else {
		exports_ = typeChecker.getExportsOfModule(symbol);
	}

	let packageExports = [...exports_];
	for (let export_ of exports_) {
		packageExports.push(...getExportedSymbols(typeChecker, export_));
	}

	let thisModule = typescript.moduleFromFileName(sourceFile.fileName);

	// Convert the exports.
	let exports: { [key: string]: Symbol } = {};
	for (let export_ of exports_) {
		exports[export_.getName()] = convertSymbol(
			typeChecker,
			packageExports,
			thisModule,
			export_,
		);
	}

	return {
		exports: groupByKind(exports),
	};
};

function groupByKind(input: {
	[key: string]: Symbol;
}): SymbolKindDeclarationIndex {
	let output: SymbolKindDeclarationIndex = {};

	for (const [key, value] of Object.entries(input)) {
		output[key] = value.declarations.reduce<DeclarationsByKind>(
			(index, declaration) => {
				switch (declaration.kind) {
					case "class": {
						if (!index.class) {
							index.class = [];
						}
						index.class.push(declaration.value);
						break;
					}
					case "enum": {
						if (!index.enum) {
							index.enum = [];
						}
						index.enum.push(declaration.value);
						break;
					}
					case "function": {
						if (!index.function) {
							index.function = [];
						}
						index.function.push(declaration.value);
						break;
					}
					case "interface": {
						if (!index.interface) {
							index.interface = [];
						}
						index.interface.push(declaration.value);
						break;
					}
					case "namespace": {
						if (!index.namespace) {
							index.namespace = [];
						}
						index.namespace.push(declaration.value);
						break;
					}
					case "type": {
						if (!index.type) {
							index.type = [];
						}
						index.type.push(declaration.value);
						break;
					}
					case "variable": {
						if (!index.variable) {
							index.variable = [];
						}
						index.variable.push(declaration.value);
						break;
					}
				}
				return index;
			},
			{},
		);
	}

	return output;
}

let getExportedSymbols = (
	typeChecker: ts.TypeChecker,
	export_: ts.Symbol,
): Array<ts.Symbol> => {
	let exports_: Array<ts.Symbol> = [];
	let symbolFlags = export_.getFlags();

	// Namespace.
	if (
		ts.SymbolFlags.NamespaceModule & symbolFlags ||
		ts.SymbolFlags.ValueModule & symbolFlags
	) {
		let exports = typeChecker.getExportsOfModule(export_);
		for (let export_ of exports) {
			exports_.push(...getExportedSymbols(typeChecker, export_));
		}
		exports_.push(...exports);
	}

	// Alias.
	if (ts.SymbolFlags.Alias & symbolFlags) {
		return getExportedSymbols(
			typeChecker,
			getAliasedSymbolIfAliased(typeChecker, export_),
		);
	}

	return exports_;
};

let convertSymbol = (
	typeChecker: ts.TypeChecker,
	packageExports: Array<ts.Symbol>,
	thisModule: Module,
	moduleExport: ts.Symbol,
): Symbol => {
	// Get the flags.
	let symbolFlags = moduleExport.getFlags();
	let declarations: Array<Declaration> = [];

	// Namespace or Value Module.
	if (
		ts.SymbolFlags.NamespaceModule & symbolFlags ||
		ts.SymbolFlags.ValueModule & symbolFlags
	) {
		// A NamespaceModule is a namespace that contains ONLY types.
		// A ValueModule is a module that contains values.
		declarations.push({
			kind: "namespace",
			value: convertModuleSymbol(
				typeChecker,
				packageExports,
				thisModule,
				moduleExport,
			),
		});
	}

	// Class.
	if (ts.SymbolFlags.Class & symbolFlags) {
		declarations.push({
			kind: "class",
			value: convertClassSymbol(
				typeChecker,
				moduleExport,
				packageExports,
				thisModule,
			),
		});
	}

	// Variable.
	if (ts.SymbolFlags.Variable & symbolFlags) {
		let variableDeclaration = convertVariableSymbol(
			typeChecker,
			moduleExport,
			packageExports,
			thisModule,
		);
		if (variableDeclaration.type.kind === "function") {
			declarations.push({
				kind: "function",
				value:
					convertVariableDeclarationToFunctionDeclaration(variableDeclaration),
			});
		} else {
			declarations.push({
				kind: "variable",
				value: variableDeclaration,
			});
		}
	}

	// Function.
	if (ts.SymbolFlags.Function & symbolFlags) {
		let functionDeclarations: Array<Declaration> = convertFunctionSymbol(
			typeChecker,
			moduleExport,
			packageExports,
			thisModule,
		).map((functionDeclaration) => ({
			kind: "function",
			value: functionDeclaration,
		}));
		declarations.push(...functionDeclarations);
	}

	// TypeAlias.
	if (ts.SymbolFlags.TypeAlias & symbolFlags) {
		declarations.push({
			kind: "type",
			value: convertTypeAliasSymbol(
				typeChecker,
				moduleExport,
				packageExports,
				thisModule,
			),
		});
	}

	// Alias.
	if (ts.SymbolFlags.Alias & symbolFlags) {
		let symbol = convertSymbol(
			typeChecker,
			packageExports,
			thisModule,
			getAliasedSymbolIfAliased(typeChecker, moduleExport),
		);
		declarations.push(...symbol.declarations);
	}

	// Enum.
	if (ts.SymbolFlags.Enum & symbolFlags) {
		let enumDeclarations: Array<Declaration> = convertEnumSymbol(
			typeChecker,
			moduleExport,
		).map((enumDeclaration) => ({
			kind: "enum",
			value: enumDeclaration,
		}));
		declarations.push(...enumDeclarations);
	}

	// Interface.
	if (
		ts.SymbolFlags.Interface & symbolFlags &&
		!(ts.SymbolFlags.Class & symbolFlags)
	) {
		let interfaceDeclarations: Array<Declaration> = convertInterfaceSymbol(
			typeChecker,
			moduleExport,
			packageExports,
			thisModule,
		).map((interfaceDeclaration) => ({
			kind: "interface",
			value: interfaceDeclaration,
		}));
		declarations.push(...interfaceDeclarations);
	}

	// Handle default export Property.
	if (
		ts.SymbolFlags.Property & moduleExport.flags &&
		moduleExport.getName() === "default"
	) {
		declarations.push({
			kind: "variable",
			value: convertDefaultExportSymbol(
				typeChecker,
				moduleExport,
				packageExports,
				thisModule,
			),
		});
	}

	return { declarations };
};

let convertVariableDeclarationToFunctionDeclaration = (
	declaration: VariableDeclaration,
): FunctionDeclaration => {
	return {
		location: declaration.location,
		signature: (declaration.type.value as FunctionType).signatures[0]!,
		comment: declaration.comment,
	};
};

// ModuleSymbol.
let convertModuleSymbol = (
	typeChecker: ts.TypeChecker,
	packageExports: Array<ts.Symbol>,
	thisModule: Module,
	symbol: ts.Symbol,
): NamespaceDeclaration => {
	// Convert the exports of the namespace.
	let exports: { [key: string]: Symbol } = {};
	let namespaceExports = typeChecker.getExportsOfModule(symbol);
	for (let namespaceExport of namespaceExports) {
		let flags = namespaceExport.getFlags();
		if (flags & ts.SymbolFlags.ModuleMember) {
			exports[namespaceExport.getName()] = convertSymbol(
				typeChecker,
				packageExports,
				thisModule,
				namespaceExport,
			);
		}
	}

	// Convert the declaration locations.
	let declaration = symbol.declarations?.filter((declaration) => {
		return (
			declaration.kind === ts.SyntaxKind.ModuleDeclaration ||
			declaration.kind === ts.SyntaxKind.SourceFile
		);
	})?.[0];
	if (!declaration) {
		let kind = symbol.declarations?.[0]?.kind;
		throw new Error(kind ? ts.SyntaxKind[kind] : "");
	}

	return {
		location: convertLocation(declaration),
		exports: groupByKind(exports),
		comment: convertComment(typeChecker, declaration, symbol),
	};
};

// ClassSymbol.
let convertClassSymbol = (
	typeChecker: ts.TypeChecker,
	symbol: ts.Symbol,
	packageExports: Array<ts.Symbol>,
	thisModule: Module,
): ClassDeclaration => {
	// Get the class declaration.
	let declaration = symbol
		.getDeclarations()
		?.find((d) => ts.isClassDeclaration(d));
	if (!declaration) {
		throw new Error();
	}

	let properties: Array<ClassProperty> = [];

	// Get the instance type properties of the class.
	let instanceType = typeChecker.getDeclaredTypeOfSymbol(symbol);
	for (let instanceProperty of typeChecker.getPropertiesOfType(instanceType)) {
		const propertySymbolParent = instanceProperty.declarations?.[0]?.parent;
		if (!propertySymbolParent || !ts.isClassDeclaration(propertySymbolParent)) {
			continue;
		}
		if (propertySymbolParent !== declaration) {
			continue;
		}
		if (instanceProperty.flags & ts.SymbolFlags.ClassMember) {
			properties.push(
				convertClassPropertySymbol(
					typeChecker,
					instanceProperty,
					packageExports,
					thisModule,
				),
			);
		}
	}

	// Get the static properties of the class.
	let staticType = typeChecker.getTypeOfSymbolAtLocation(symbol, declaration);
	for (let staticProperty of typeChecker.getPropertiesOfType(staticType)) {
		// Ignore prototype.
		if (
			staticProperty.flags &
			(ts.SymbolFlags.ModuleMember | ts.SymbolFlags.Prototype)
		)
			continue;

		if (staticProperty.flags & ts.SymbolFlags.ClassMember) {
			properties.push(
				convertClassPropertySymbol(
					typeChecker,
					staticProperty,
					packageExports,
					thisModule,
				),
			);
		}
	}

	// Get the constructor signature.
	let signature = staticType.getConstructSignatures()[0]!;
	let constructSignature = undefined;
	let constructSignatureParent = signature.declaration?.parent;
	if (
		constructSignatureParent &&
		ts.isClassDeclaration(constructSignatureParent)
	) {
		constructSignature = convertSignature(
			typeChecker,
			signature,
			packageExports,
			thisModule,
		);
	}

	// Get the call signature.
	let callSignatures = instanceType.getCallSignatures().map((signature) => {
		return convertSignature(typeChecker, signature, packageExports, thisModule);
	});

	let typeParameters: { [key: string]: TypeParameter } = {};
	if (declaration.typeParameters) {
		typeParameters = Object.fromEntries(
			declaration.typeParameters?.map((typeParameter) => [
				typeParameter.name.getText(),
				convertTypeParameterNode(
					typeChecker,
					typeParameter,
					packageExports,
					thisModule,
				),
			]),
		);
	}

	return {
		location: convertLocation(declaration),
		properties,
		constructSignature,
		callSignature: callSignatures[0],
		comment: convertComment(typeChecker, declaration, symbol),
		typeParameters,
	};
};

// VariableSymbol.
let convertVariableSymbol = (
	typeChecker: ts.TypeChecker,
	symbol: ts.Symbol,
	packageExports: Array<ts.Symbol>,
	thisModule: Module,
): VariableDeclaration => {
	// Get the declaration.
	let declaration = symbol
		.getDeclarations()
		?.find((d): d is ts.VariableDeclaration => ts.isVariableDeclaration(d));
	if (!declaration) {
		throw new Error();
	}

	// Convert the declaration.
	let type: Type;
	if (ts.isVariableDeclaration(declaration) && declaration.type) {
		type = convertTypeNode(
			typeChecker,
			declaration.type,
			packageExports,
			thisModule,
		);
	} else {
		type = convertType(
			typeChecker,
			typeChecker.getTypeOfSymbol(symbol),
			packageExports,
			thisModule,
		);
	}

	return {
		location: convertLocation(declaration),
		type,
		comment: convertComment(typeChecker, declaration, symbol),
	};
};

// DefaultExport.
let convertDefaultExportSymbol = (
	typeChecker: ts.TypeChecker,
	symbol: ts.Symbol,
	packageExports: Array<ts.Symbol>,
	thisModule: Module,
): DefaultExportDeclaration => {
	let declaration = symbol.valueDeclaration;
	if (!declaration) {
		throw new Error();
	}
	// Convert the declaration.
	let type = convertType(
		typeChecker,
		typeChecker.getTypeOfSymbol(symbol),
		packageExports,
		thisModule,
	);

	return {
		location: convertLocation(declaration),
		type,
		comment: convertComment(typeChecker, declaration, symbol),
	};
};

// FunctionSymbol.
let convertFunctionSymbol = (
	typeChecker: ts.TypeChecker,
	symbol: ts.Symbol,
	packageExports: Array<ts.Symbol>,
	thisModule: Module,
): Array<FunctionDeclaration> => {
	// Get the declarations.
	let declarations = symbol
		.getDeclarations()
		?.filter((d): d is ts.FunctionDeclaration => ts.isFunctionDeclaration(d));
	if (!declarations) {
		throw new Error();
	}

	// Convert the declarations.
	return declarations?.map((declaration) =>
		convertFunctionDeclaration(
			typeChecker,
			declaration,
			packageExports,
			thisModule,
		),
	);
};

// FunctionDeclaration.
let convertFunctionDeclaration = (
	typeChecker: ts.TypeChecker,
	declaration: ts.FunctionDeclaration,
	packageExports: Array<ts.Symbol>,
	thisModule: Module,
): FunctionDeclaration => {
	// Get the parameters.
	let parameters = Object.fromEntries(
		declaration.parameters.map((parameter) => {
			let optional = false;
			let dotDotDotToken = false;
			if (ts.isParameter(parameter) && parameter.questionToken) {
				optional = true;
			}
			if (ts.isParameter(parameter) && parameter.dotDotDotToken) {
				dotDotDotToken = true;
			}
			let type = typeChecker.getTypeAtLocation(parameter);
			return [
				parameter.name.getText(),
				{
					type: parameter?.type
						? convertTypeNode(
								typeChecker,
								parameter.type,
								packageExports,
								thisModule,
							)
						: convertType(typeChecker, type, packageExports, thisModule),
					optional,
					dotDotDotToken,
				},
			];
		}),
	);

	// Get the type paramaters.
	let typeParameters: { [key: string]: TypeParameter } = {};
	let callSignatureTypeParameters = declaration.typeParameters;
	if (callSignatureTypeParameters) {
		typeParameters = Object.fromEntries(
			callSignatureTypeParameters.map((typeParameter) => {
				let type = typeChecker.getTypeAtLocation(typeParameter);
				return [
					typeParameter.name.getText(),
					convertTypeParameterType(
						typeChecker,
						type,
						packageExports,
						thisModule,
					),
				];
			}),
		);
	}

	// Get the return type.
	let returnType: Type;
	let signature = typeChecker.getSignatureFromDeclaration(declaration);
	if (!signature) {
		throw new Error();
	}
	let predicate = typeChecker.getTypePredicateOfSignature(signature);
	if (predicate) {
		returnType = {
			kind: "predicate",
			value: convertTypePredicate(
				typeChecker,
				predicate,
				packageExports,
				thisModule,
			),
		};
	} else if (declaration.type) {
		returnType = convertTypeNode(
			typeChecker,
			declaration.type,
			packageExports,
			thisModule,
		);
	} else {
		returnType = convertType(
			typeChecker,
			signature.getReturnType(),
			packageExports,
			thisModule,
		);
	}

	// Get the symbol.
	let symbol = typeChecker.getSymbolAtLocation(declaration);

	return {
		location: convertLocation(declaration),
		signature: {
			location: convertLocation(declaration),
			parameters,
			typeParameters,
			return: returnType,
		},
		comment: convertComment(typeChecker, declaration, symbol),
	};
};

// TypeAliasSymbol.
let convertTypeAliasSymbol = (
	typeChecker: ts.TypeChecker,
	symbol: ts.Symbol,
	packageExports: Array<ts.Symbol>,
	thisModule: Module,
): TypeDeclaration => {
	// Get the declaration.
	let declaration = symbol
		.getDeclarations()
		?.find((d): d is ts.TypeAliasDeclaration => ts.isTypeAliasDeclaration(d));
	if (!declaration) {
		throw new Error();
	}

	// Convert the declaration.
	let type = convertTypeNode(
		typeChecker,
		declaration.type,
		packageExports,
		thisModule,
	);

	let typeParameters: { [key: string]: TypeParameter } = {};
	if (declaration.typeParameters) {
		typeParameters = Object.fromEntries(
			declaration.typeParameters?.map((typeParameter) => [
				typeParameter.name.getText(),
				convertTypeParameterNode(
					typeChecker,
					typeParameter,
					packageExports,
					thisModule,
				),
			]),
		);
	}

	return {
		location: convertLocation(declaration),
		type,
		typeParameters,
		comment: convertComment(typeChecker, declaration, symbol),
	};
};

// EnumSymbol.
let convertEnumSymbol = (
	typeChecker: ts.TypeChecker,
	symbol: ts.Symbol,
): Array<EnumDeclaration> => {
	// Get the declarations.
	let declarations = symbol
		.getDeclarations()
		?.filter((d): d is ts.EnumDeclaration => ts.isEnumDeclaration(d));
	if (!declarations) {
		throw new Error();
	}

	return declarations.map((declaration) => {
		// Get the members.
		let members: { [key: string]: EnumMemberValue } = {};
		for (let enumMember of declaration.members) {
			let enumMemberSymbol = typeChecker.getSymbolAtLocation(enumMember.name);
			if (!enumMemberSymbol) {
				throw new Error();
			}
			members[enumMemberSymbol.getName()] = convertEnumMemberSymbol(
				typeChecker,
				enumMemberSymbol,
			);
		}
		return {
			location: convertLocation(declaration),
			members,
			comment: convertComment(typeChecker, declaration, symbol),
		};
	});
};

// InterfaceSymbol.
let convertInterfaceSymbol = (
	typeChecker: ts.TypeChecker,
	symbol: ts.Symbol,
	packageExports: Array<ts.Symbol>,
	thisModule: Module,
): Array<InterfaceDeclaration> => {
	// Get the declarations.
	let declarations = symbol
		.getDeclarations()
		?.filter((d): d is ts.InterfaceDeclaration => ts.isInterfaceDeclaration(d));
	if (!declarations || declarations.length === 0) {
		throw new Error();
	}

	return declarations.map((declaration) => {
		// Get the index signature.
		let indexSignature = declaration.members
			.filter((d): d is ts.IndexSignatureDeclaration =>
				ts.isIndexSignatureDeclaration(d),
			)
			.map((member) =>
				convertIndexSignature(typeChecker, member, packageExports, thisModule),
			)?.[0];

		// Get the construct signatures.
		let constructSignatures = declaration.members
			.filter((d): d is ts.ConstructSignatureDeclaration =>
				ts.isConstructSignatureDeclaration(d),
			)
			.map((member) => {
				return convertConstructSignatureDeclaration(
					typeChecker,
					member,
					packageExports,
					thisModule,
				);
			});

		// Get the members.
		let properties = Object.fromEntries(
			declaration.members
				.filter((d) => !ts.isIndexSignatureDeclaration(d))
				.map((member) => {
					return [
						member.name?.getText(),
						convertTypeElement(typeChecker, member, packageExports, thisModule),
					];
				}),
		);

		let typeParameters = declaration.typeParameters?.map((typeParameter) => [
			typeParameter.name.getText(),
			convertTypeParameterNode(
				typeChecker,
				typeParameter,
				packageExports,
				thisModule,
			),
		]);

		return {
			location: convertLocation(declaration),
			typeParameters: Object.fromEntries(typeParameters || []),
			indexSignature,
			constructSignatures,
			properties,
			comment: convertComment(typeChecker, declaration, symbol),
		};
	});
};

// IndexSignature.
let convertIndexSignature = (
	typeChecker: ts.TypeChecker,
	declaration: ts.IndexSignatureDeclaration,
	packageExports: Array<ts.Symbol>,
	thisModule: Module,
): IndexSignature => {
	let parameter = declaration.parameters[0]!;
	let key = convertParameterNode(
		typeChecker,
		parameter,
		packageExports,
		thisModule,
	);
	return {
		name: parameter.name.getText(),
		type: convertTypeNode(
			typeChecker,
			declaration.type,
			packageExports,
			thisModule,
		),
		key,
	};
};

// ConstructSignature.
let convertSignature = (
	typeChecker: ts.TypeChecker,
	signature: ts.Signature,
	packageExports: Array<ts.Symbol>,
	thisModule: Module,
): Signature => {
	// Get the parameters.
	let parameters = Object.fromEntries(
		signature.parameters.map((parameter) => {
			let optional = false;
			let dotDotDotToken = false;
			if (parameter.flags & ts.SymbolFlags.Optional) {
				optional = true;
			}
			let declaration =
				parameter.getDeclarations()?.[0] as ts.ParameterDeclaration;
			if (declaration.dotDotDotToken) {
				dotDotDotToken = true;
			}
			return [
				parameter.getName(),
				{
					type: declaration?.type
						? convertTypeNode(
								typeChecker,
								declaration.type,
								packageExports,
								thisModule,
							)
						: convertType(
								typeChecker,
								typeChecker.getTypeOfSymbol(parameter),
								packageExports,
								thisModule,
							),
					optional,
					dotDotDotToken,
				},
			];
		}),
	);

	// Get the return type.
	let declaration = signature.declaration as
		| ts.SignatureDeclaration
		| undefined;
	let returnType: Type;
	if (declaration?.type) {
		returnType = convertTypeNode(
			typeChecker,
			declaration.type,
			packageExports,
			thisModule,
		);
	} else {
		returnType = convertType(
			typeChecker,
			signature.getReturnType(),
			packageExports,
			thisModule,
		);
	}

	return { parameters, return: returnType };
};

// ConstructSignatureDeclaration.
let convertConstructSignatureDeclaration = (
	typeChecker: ts.TypeChecker,
	declaration: ts.ConstructSignatureDeclaration,
	packageExports: Array<ts.Symbol>,
	thisModule: Module,
): Signature => {
	// Get the parameters.
	let parameters = Object.fromEntries(
		declaration.parameters.map((parameter) => {
			let optional = false;
			let dotDotDotToken = false;
			if (parameter.questionToken) {
				optional = true;
			}
			if (parameter.dotDotDotToken) {
				dotDotDotToken = true;
			}
			return [
				parameter.name.getText(),
				{
					type: parameter.type
						? convertTypeNode(
								typeChecker,
								parameter.type,
								packageExports,
								thisModule,
							)
						: convertType(
								typeChecker,
								typeChecker.getTypeAtLocation(parameter),
								packageExports,
								thisModule,
							),
					optional,
					dotDotDotToken,
				},
			];
		}),
	);

	// Get the return type.
	let returnType: Type;
	if (declaration.type) {
		returnType = convertTypeNode(
			typeChecker,
			declaration.type,
			packageExports,
			thisModule,
		);
	} else {
		let type = typeChecker.getTypeAtLocation(declaration);
		returnType = convertType(typeChecker, type, packageExports, thisModule);
	}

	return { parameters, return: returnType };
};

// TypeElement.
let convertTypeElement = (
	typeChecker: ts.TypeChecker,
	member: ts.TypeElement,
	packageExports: Array<ts.Symbol>,
	thisModule: Module,
): InterfaceProperty => {
	// Get the type.
	let type = typeChecker.getTypeAtLocation(member);
	return {
		type: convertType(typeChecker, type, packageExports, thisModule),
		readonly:
			(ts.getCombinedModifierFlags(member) & ts.ModifierFlags.Readonly) !== 0,
	};
};

// EnumMemberSymbol.
let convertEnumMemberSymbol = (
	typeChecker: ts.TypeChecker,
	symbol: ts.Symbol,
): EnumMemberValue => {
	// Get the declaration.
	let declaration = symbol.getDeclarations()![0]! as ts.EnumMember;

	// Get the constant value.
	let constantValue = typeChecker.getConstantValue(declaration);

	// Convert the value.
	let value: EnumMemberConstantValue | undefined = undefined;
	let initializer = declaration.initializer?.getText();
	if (typeof constantValue === "string") {
		value = {
			kind: "string",
			value: constantValue,
		};
	}
	if (typeof constantValue === "number") {
		value = {
			kind: "number",
			value: constantValue,
		};
	}

	return { constantValue: value, initializer };
};

// ClassPropertySymbol.
let convertClassPropertySymbol = (
	typeChecker: ts.TypeChecker,
	property: ts.Symbol,
	packageExports: Array<ts.Symbol>,
	thisModule: Module,
): ClassProperty => {
	let flags = ts.getCombinedModifierFlags(property.valueDeclaration!);
	let type = convertType(
		typeChecker,
		typeChecker.getTypeOfSymbol(property),
		packageExports,
		thisModule,
	);
	return {
		name: property.getName(),
		comment: convertComment(typeChecker, property.valueDeclaration!, property),
		type,
		static: (flags & ts.ModifierFlags.Static) !== 0,
		private: (flags & ts.ModifierFlags.Private) !== 0,
		accessor:
			property.valueDeclaration!.kind === ts.SyntaxKind.GetAccessor
				? "get"
				: property.valueDeclaration!.kind === ts.SyntaxKind.SetAccessor
					? "set"
					: undefined,
	};
};

let convertType = (
	typeChecker: ts.TypeChecker,
	type: ts.Type,
	packageExports: Array<ts.Symbol>,
	thisModule: Module,
): Type => {
	let node = typeChecker.typeToTypeNode(
		type,
		undefined,
		ts.NodeBuilderFlags.IgnoreErrors,
	)!;
	if (node.kind === ts.SyntaxKind.ArrayType) {
		return {
			kind: "array",
			value: convertArrayType(typeChecker, type, packageExports, thisModule),
		};
	} else if (node.kind === ts.SyntaxKind.FunctionType) {
		return {
			kind: "function",
			value: convertFunctionType(typeChecker, type, packageExports, thisModule),
		};
	} else if (node.kind === ts.SyntaxKind.IndexedAccessType) {
		return {
			kind: "indexed_access",
			value: convertIndexedAccessType(
				typeChecker,
				type as ts.IndexedAccessType,
				packageExports,
				thisModule,
			),
		};
	} else if (node.kind === ts.SyntaxKind.IntersectionType) {
		return {
			kind: "intersection",
			value: convertIntersectionType(
				typeChecker,
				type as ts.IntersectionType,
				packageExports,
				thisModule,
			),
		};
	} else if (keywordSet.has(node.kind)) {
		return { kind: "keyword", value: convertKeywordType(node.kind) };
	} else if (node.kind === ts.SyntaxKind.LiteralType) {
		return {
			kind: "literal",
			value: convertLiteralTypeNode(typeChecker, node as ts.LiteralTypeNode),
		};
	} else if (node.kind === ts.SyntaxKind.TypeLiteral) {
		return {
			kind: "object",
			value: convertObjectType(typeChecker, type, packageExports, thisModule),
		};
	} else if (node.kind === ts.SyntaxKind.TypeReference) {
		return {
			kind: "reference",
			value: convertTypeReferenceType(
				typeChecker,
				type,
				packageExports,
				thisModule,
			),
		};
	} else if (node.kind === ts.SyntaxKind.TupleType) {
		return {
			kind: "tuple",
			value: convertTupleType(
				typeChecker,
				type as ts.TupleType,
				packageExports,
				thisModule,
			),
		};
	} else if (node.kind === ts.SyntaxKind.UnionType) {
		return {
			kind: "union",
			value: convertUnionType(typeChecker, type as ts.UnionType),
		};
	} else {
		return { kind: "other", value: typeChecker.typeToString(type) };
	}
};

// ArrayType.
let convertArrayType = (
	typeChecker: ts.TypeChecker,
	type: ts.Type,
	packageExports: Array<ts.Symbol>,
	thisModule: Module,
): ArrayType => {
	return {
		type: convertType(
			typeChecker,
			typeChecker.getTypeArguments(type as ts.TypeReference)[0]!,
			packageExports,
			thisModule,
		),
	};
};

// FunctionType.
let convertFunctionType = (
	typeChecker: ts.TypeChecker,
	type: ts.Type,
	packageExports: Array<ts.Symbol>,
	thisModule: Module,
): FunctionType => {
	let signatures = type.getCallSignatures().map((signature) => {
		// Get the declaration.
		let declaration = signature.getDeclaration() as ts.SignatureDeclaration;

		// Get the parameters.
		let parameters = Object.fromEntries(
			signature.getParameters().map((parameter) => {
				let parameterType = typeChecker.getTypeOfSymbol(parameter);
				let parameterDeclaration: ts.ParameterDeclaration | undefined =
					parameter.valueDeclaration as ts.ParameterDeclaration;
				let optional = false;
				let dotDotDotToken = false;
				if (parameterDeclaration) {
					if (
						ts.isParameter(parameterDeclaration) &&
						parameterDeclaration.questionToken
					) {
						optional = true;
					}
					if (
						ts.isParameter(parameterDeclaration) &&
						parameterDeclaration.questionToken
					) {
						dotDotDotToken = true;
					}
				}
				return [
					parameter.getName(),
					{
						type: parameterDeclaration?.type
							? convertTypeNode(
									typeChecker,
									parameterDeclaration.type,
									packageExports,
									thisModule,
								)
							: convertType(
									typeChecker,
									parameterType,
									packageExports,
									thisModule,
								),
						optional,
						dotDotDotToken,
					},
				];
			}),
		);

		// Get the type parameters.
		let typeParameters: { [key: string]: TypeParameter } = {};
		let callSignatureTypeParameters = signature.getTypeParameters();
		if (callSignatureTypeParameters) {
			typeParameters = Object.fromEntries(
				callSignatureTypeParameters.map((typeParameter) => [
					typeParameter.symbol.getName(),
					convertTypeParameterType(
						typeChecker,
						typeParameter,
						packageExports,
						thisModule,
					),
				]),
			);
		}

		let returnType: Type;
		let predicate = typeChecker.getTypePredicateOfSignature(signature);
		if (predicate) {
			returnType = {
				kind: "predicate",
				value: convertTypePredicate(
					typeChecker,
					predicate,
					packageExports,
					thisModule,
				),
			};
		} else if (declaration.type) {
			returnType = convertTypeNode(
				typeChecker,
				declaration.type,
				packageExports,
				thisModule,
			);
		} else {
			returnType = convertType(
				typeChecker,
				signature.getReturnType(),
				packageExports,
				thisModule,
			);
		}
		return {
			location: convertLocation(declaration),
			parameters,
			typeParameters,
			return: returnType,
		};
	});

	return {
		signatures,
	};
};

// IndexedAccessType.
let convertIndexedAccessType = (
	typeChecker: ts.TypeChecker,
	type: ts.IndexedAccessType,
	packageExports: Array<ts.Symbol>,
	thisModule: Module,
): IndexedAccessType => {
	return {
		objectType: convertType(
			typeChecker,
			type.objectType,
			packageExports,
			thisModule,
		),
		indexType: convertType(
			typeChecker,
			type.indexType,
			packageExports,
			thisModule,
		),
	};
};

// IntersectionType
let convertIntersectionType = (
	typeChecker: ts.TypeChecker,
	type: ts.IntersectionType,
	packageExports: Array<ts.Symbol>,
	thisModule: Module,
): IntersectionType => {
	return {
		types: type.types.map((type) =>
			convertType(typeChecker, type, packageExports, thisModule),
		),
	};
};

// KeywordType.
let keywordToName: { [key: number]: string } = {
	[ts.SyntaxKind.AnyKeyword]: "any",
	[ts.SyntaxKind.BigIntKeyword]: "bigint",
	[ts.SyntaxKind.BooleanKeyword]: "boolean",
	[ts.SyntaxKind.NeverKeyword]: "never",
	[ts.SyntaxKind.NumberKeyword]: "number",
	[ts.SyntaxKind.ObjectKeyword]: "object",
	[ts.SyntaxKind.StringKeyword]: "string",
	[ts.SyntaxKind.SymbolKeyword]: "symbol",
	[ts.SyntaxKind.UndefinedKeyword]: "undefined",
	[ts.SyntaxKind.UnknownKeyword]: "unknown",
	[ts.SyntaxKind.VoidKeyword]: "void",
};
let keywordSet = new Set([
	ts.SyntaxKind.AnyKeyword,
	ts.SyntaxKind.BigIntKeyword,
	ts.SyntaxKind.BooleanKeyword,
	ts.SyntaxKind.NeverKeyword,
	ts.SyntaxKind.NumberKeyword,
	ts.SyntaxKind.ObjectKeyword,
	ts.SyntaxKind.StringKeyword,
	ts.SyntaxKind.SymbolKeyword,
	ts.SyntaxKind.UndefinedKeyword,
	ts.SyntaxKind.UnknownKeyword,
	ts.SyntaxKind.VoidKeyword,
]);
let convertKeywordType = (kind: ts.SyntaxKind): KeywordType => {
	return keywordToName[kind] as KeywordType;
};

// ObjectType.
let convertObjectType = (
	typeChecker: ts.TypeChecker,
	type: ts.Type,
	packageExports: Array<ts.Symbol>,
	thisModule: Module,
): ObjectType => {
	// Get the index signature.
	let indexSignature: IndexSignature | undefined;
	let indexSymbol = type.symbol?.members?.get("__index" as ts.__String);
	if (indexSymbol) {
		let declaration =
			indexSymbol.declarations![0]! as ts.IndexSignatureDeclaration;
		indexSignature = convertIndexSignature(
			typeChecker,
			declaration,
			packageExports,
			thisModule,
		);
	}

	// Get the construct signatures.
	let constructSignatures = type.getConstructSignatures().map((signature) => {
		let signatureDeclaration = signature.getDeclaration();
		assert(ts.isConstructSignatureDeclaration(signatureDeclaration));
		return convertConstructSignatureDeclaration(
			typeChecker,
			signatureDeclaration,
			packageExports,
			thisModule,
		);
	});

	// Get the properties.
	let properties = typeChecker.getPropertiesOfType(type).map((property) => {
		// Convert the property.
		let declaration = property.getDeclarations()?.[0]!;
		let type: Type;
		if (ts.isPropertySignature(declaration) && declaration.type) {
			type = convertTypeNode(
				typeChecker,
				declaration.type,
				packageExports,
				thisModule,
			);
		} else {
			type = convertType(
				typeChecker,
				typeChecker.getTypeOfSymbol(property),
				packageExports,
				thisModule,
			);
		}
		let optional = false;
		if (property.flags & ts.SymbolFlags.Optional) {
			optional = true;
		}
		let objectProperty: ObjectProperty = { type, optional };
		return [property.getName(), objectProperty];
	});

	return {
		properties: Object.fromEntries(properties),
		indexSignature,
		constructSignatures,
	};
};

// TupleType.
let convertTupleType = (
	typeChecker: ts.TypeChecker,
	type: ts.TupleType,
	packageExports: Array<ts.Symbol>,
	thisModule: Module,
): TupleType => {
	return {
		types: typeChecker
			.getTypeArguments(type)
			.map((type) =>
				convertType(typeChecker, type, packageExports, thisModule),
			),
	};
};

// TypeParameter.
let convertTypeParameterType = (
	typeChecker: ts.TypeChecker,
	type: ts.Type,
	packageExports: Array<ts.Symbol>,
	thisModule: Module,
): TypeParameter => {
	let constraint = type.getConstraint();
	let default_ = type.getDefault();
	return {
		constraint: constraint
			? convertType(typeChecker, constraint, packageExports, thisModule)
			: undefined,
		default: default_
			? convertType(typeChecker, default_, packageExports, thisModule)
			: undefined,
	};
};

// TypePredicate.
let convertTypePredicate = (
	typeChecker: ts.TypeChecker,
	type: ts.TypePredicate,
	packageExports: Array<ts.Symbol>,
	thisModule: Module,
): PredicateType => {
	let asserts =
		type.kind === ts.TypePredicateKind.AssertsIdentifier ||
		type.kind === ts.TypePredicateKind.AssertsThis;
	return {
		name: type.parameterName ?? "this",
		type: type.type
			? convertType(typeChecker, type.type, packageExports, thisModule)
			: undefined,
		asserts,
	};
};

// TypeReferenceType.
let convertTypeReferenceType = (
	typeChecker: ts.TypeChecker,
	type: ts.Type,
	packageExports: Array<ts.Symbol>,
	thisModule: Module,
): ReferenceType => {
	let isTypeParameter = (type.flags & ts.TypeFlags.TypeParameter) !== 0;
	if (type.aliasSymbol) {
		let aliasSymbol = type.aliasSymbol;
		let typeArguments = type.aliasTypeArguments?.map((typeArgument) =>
			convertType(typeChecker, typeArgument, packageExports, thisModule),
		);
		let declaration = aliasSymbol.declarations![0]!;
		let fullyQualifiedName = getFullyQualifiedName(
			typeChecker,
			declaration,
			aliasSymbol,
		);
		return {
			name: aliasSymbol.getName(),
			fullyQualifiedName,
			location: convertLocation(declaration),
			typeArguments: typeArguments ?? [],
			isTypeParameter,
			isExported: isExported(
				typeChecker,
				packageExports,
				thisModule,
				aliasSymbol,
				declaration,
			),
		};
	} else {
		let typeArguments = typeChecker
			.getTypeArguments(type as ts.TypeReference)
			.map((typeArgument) => {
				return convertType(
					typeChecker,
					typeArgument,
					packageExports,
					thisModule,
				);
			});
		let symbol = type.symbol;
		let declaration = symbol.declarations![0]!;
		let name = getAliasedSymbolIfAliased(typeChecker, symbol).getName();
		let fullyQualifiedName = getFullyQualifiedName(
			typeChecker,
			declaration,
			symbol,
		);
		return {
			name,
			fullyQualifiedName,
			location: convertLocation(declaration),
			typeArguments: typeArguments ?? [],
			isTypeParameter,
			isExported: isExported(
				typeChecker,
				packageExports,
				thisModule,
				symbol,
				declaration,
			),
		};
	}
};

// UnionType.
let convertUnionType = (
	typeChecker: ts.TypeChecker,
	type: ts.UnionType,
): UnionType => {
	return {
		types: type.types.map((type) => {
			return {
				kind: "other",
				value: typeChecker.typeToString(type),
			};
		}),
	};
};

let convertTypeNode = (
	typeChecker: ts.TypeChecker,
	node: ts.TypeNode,
	packageExports: Array<ts.Symbol>,
	thisModule: Module,
): Type => {
	if (ts.isArrayTypeNode(node)) {
		return {
			kind: "array",
			value: convertArrayTypeNode(
				typeChecker,
				node,
				packageExports,
				thisModule,
			),
		};
	} else if (ts.isConditionalTypeNode(node)) {
		return {
			kind: "conditional",
			value: convertConditionalTypeNode(
				typeChecker,
				node,
				packageExports,
				thisModule,
			),
		};
	} else if (ts.isFunctionTypeNode(node)) {
		return {
			kind: "function",
			value: convertFunctionTypeNode(
				typeChecker,
				node,
				packageExports,
				thisModule,
			),
		};
	} else if (ts.isInferTypeNode(node)) {
		return {
			kind: "infer",
			value: convertInferTypeNode(
				typeChecker,
				node,
				packageExports,
				thisModule,
			),
		};
	} else if (ts.isIndexedAccessTypeNode(node)) {
		return {
			kind: "indexed_access",
			value: convertIndexedAccessTypeNode(
				typeChecker,
				node,
				packageExports,
				thisModule,
			),
		};
	} else if (ts.isIntersectionTypeNode(node)) {
		return {
			kind: "intersection",
			value: convertIntersectionTypeNode(
				typeChecker,
				node,
				packageExports,
				thisModule,
			),
		};
	} else if (keywordSet.has(node.kind)) {
		return { kind: "keyword", value: convertKeywordType(node.kind) };
	} else if (ts.isLiteralTypeNode(node)) {
		return {
			kind: "literal",
			value: convertLiteralTypeNode(typeChecker, node),
		};
	} else if (ts.isMappedTypeNode(node)) {
		return {
			kind: "mapped",
			value: convertMappedTypeNode(
				typeChecker,
				node,
				packageExports,
				thisModule,
			),
		};
	} else if (ts.isTypeLiteralNode(node)) {
		return {
			kind: "object",
			value: convertObjectTypeNode(
				typeChecker,
				node,
				packageExports,
				thisModule,
			),
		};
	} else if (ts.isTypePredicateNode(node)) {
		return {
			kind: "predicate",
			value: convertTypePredicateNode(
				typeChecker,
				node,
				packageExports,
				thisModule,
			),
		};
	} else if (ts.isTypeReferenceNode(node)) {
		return {
			kind: "reference",
			value: convertTypeReferenceTypeNode(
				typeChecker,
				node,
				packageExports,
				thisModule,
			),
		};
	} else if (ts.isTemplateLiteralTypeNode(node)) {
		return {
			kind: "template_literal",
			value: convertTemplateLiteralTypeNode(
				typeChecker,
				node,
				packageExports,
				thisModule,
			),
		};
	} else if (ts.isTupleTypeNode(node)) {
		return {
			kind: "tuple",
			value: convertTupleTypeNode(
				typeChecker,
				node,
				packageExports,
				thisModule,
			),
		};
	} else if (ts.isTypeOperatorNode(node)) {
		return {
			kind: "type_operator",
			value: convertTypeOperatorNode(
				typeChecker,
				node,
				packageExports,
				thisModule,
			),
		};
	} else if (ts.isTypeQueryNode(node)) {
		return {
			kind: "type_query",
			value: convertTypeQueryNode(typeChecker, node),
		};
	} else if (ts.isUnionTypeNode(node)) {
		return {
			kind: "union",
			value: convertUnionTypeNode(
				typeChecker,
				node,
				packageExports,
				thisModule,
			),
		};
	} else {
		let type = typeChecker.getTypeFromTypeNode(node);
		return { kind: "other", value: typeChecker.typeToString(type) };
	}
};

// ArrayTypeNode.
let convertArrayTypeNode = (
	typeChecker: ts.TypeChecker,
	node: ts.ArrayTypeNode,
	packageExports: Array<ts.Symbol>,
	thisModule: Module,
): ArrayType => {
	return {
		type: convertTypeNode(
			typeChecker,
			node.elementType,
			packageExports,
			thisModule,
		),
	};
};

// ConditionalTypeNode.
let convertConditionalTypeNode = (
	typeChecker: ts.TypeChecker,
	node: ts.ConditionalTypeNode,
	packageExports: Array<ts.Symbol>,
	thisModule: Module,
): ConditionalType => {
	return {
		checkType: convertTypeNode(
			typeChecker,
			node.checkType,
			packageExports,
			thisModule,
		),
		extendsType: convertTypeNode(
			typeChecker,
			node.extendsType,
			packageExports,
			thisModule,
		),
		trueType: convertTypeNode(
			typeChecker,
			node.trueType,
			packageExports,
			thisModule,
		),
		falseType: convertTypeNode(
			typeChecker,
			node.falseType,
			packageExports,
			thisModule,
		),
	};
};

// FunctionTypeNode.
let convertFunctionTypeNode = (
	typeChecker: ts.TypeChecker,
	node: ts.FunctionTypeNode,
	packageExports: Array<ts.Symbol>,
	thisModule: Module,
): FunctionType => {
	let parameters = node.parameters?.map((parameter) => [
		parameter.name.getText(),
		convertParameterNode(typeChecker, parameter, packageExports, thisModule),
	]);
	let typeParameters = node.typeParameters?.map((typeParameter) => [
		typeParameter.name.getText(),
		convertTypeParameterNode(
			typeChecker,
			typeParameter,
			packageExports,
			thisModule,
		),
	]);
	return {
		signatures: [
			{
				location: convertLocation(node),
				parameters: Object.fromEntries(parameters ?? []),
				typeParameters: Object.fromEntries(typeParameters || []),
				return: convertTypeNode(
					typeChecker,
					node.type,
					packageExports,
					thisModule,
				),
			},
		],
	};
};

// IndexedAccessTypeNode.
let convertIndexedAccessTypeNode = (
	typeChecker: ts.TypeChecker,
	node: ts.IndexedAccessTypeNode,
	packageExports: Array<ts.Symbol>,
	thisModule: Module,
): IndexedAccessType => {
	return {
		objectType: convertTypeNode(
			typeChecker,
			node.objectType,
			packageExports,
			thisModule,
		),
		indexType: convertTypeNode(
			typeChecker,
			node.indexType,
			packageExports,
			thisModule,
		),
	};
};

// InferTypeNode.
let convertInferTypeNode = (
	typeChecker: ts.TypeChecker,
	node: ts.InferTypeNode,
	packageExports: Array<ts.Symbol>,
	thisModule: Module,
): InferType => {
	return {
		typeParameter: {
			[node.typeParameter.name.getText()]: convertTypeParameterNode(
				typeChecker,
				node.typeParameter,
				packageExports,
				thisModule,
			),
		},
	};
};

// IntersectionTypeNode.
let convertIntersectionTypeNode = (
	typeChecker: ts.TypeChecker,
	node: ts.IntersectionTypeNode,
	packageExports: Array<ts.Symbol>,
	thisModule: Module,
): IntersectionType => {
	return {
		types: node.types.map((node) =>
			convertTypeNode(typeChecker, node, packageExports, thisModule),
		),
	};
};

// LiteralTypeNode.
let convertLiteralTypeNode = (
	_typeChecker: ts.TypeChecker,
	node: ts.LiteralTypeNode,
): LiteralType => {
	if (node.literal.kind === ts.SyntaxKind.StringLiteral) {
		return {
			kind: "string",
			value: node.literal.text,
		};
	} else if (node.literal.kind === ts.SyntaxKind.NumericLiteral) {
		return {
			kind: "number",
			value: Number(node.literal.text),
		};
	} else if (node.literal.kind === ts.SyntaxKind.TrueKeyword) {
		return {
			kind: "boolean",
			value: true,
		};
	} else if (node.literal.kind === ts.SyntaxKind.FalseKeyword) {
		return {
			kind: "boolean",
			value: false,
		};
	} else if (node.literal.kind === ts.SyntaxKind.NullKeyword) {
		return {
			kind: "null",
		};
	} else {
		throw new Error("Unknown");
	}
};

// MappedTypeNode.
let convertMappedTypeNode = (
	typeChecker: ts.TypeChecker,
	node: ts.MappedTypeNode,
	packageExports: Array<ts.Symbol>,
	thisModule: Module,
): MappedType => {
	return {
		type: convertTypeNode(typeChecker, node.type!, packageExports, thisModule),
		constraint: convertTypeNode(
			typeChecker,
			node.typeParameter.constraint!,
			packageExports,
			thisModule,
		),
		typeParameterName: node.typeParameter.name.text,
		nameType: node.nameType
			? convertTypeNode(typeChecker, node.nameType, packageExports, thisModule)
			: undefined,
	};
};

// ObjectTypeNode.
let convertObjectTypeNode = (
	typeChecker: ts.TypeChecker,
	node: ts.TypeLiteralNode,
	packageExports: Array<ts.Symbol>,
	thisModule: Module,
): ObjectType => {
	let type = typeChecker.getTypeAtLocation(node);
	return convertObjectType(typeChecker, type, packageExports, thisModule);
};

// ParameterNode.
let convertParameterNode = (
	typeChecker: ts.TypeChecker,
	node: ts.ParameterDeclaration,
	packageExports: Array<ts.Symbol>,
	thisModule: Module,
): Parameter => {
	return {
		optional: node.questionToken !== undefined,
		dotDotDotToken: node.dotDotDotToken !== undefined,
		type: node.type
			? convertTypeNode(typeChecker, node.type, packageExports, thisModule)
			: convertType(
					typeChecker,
					typeChecker.getTypeAtLocation(node),
					packageExports,
					thisModule,
				),
	};
};

// TemplateLiteralTypeNode.
let convertTemplateLiteralTypeNode = (
	typeChecker: ts.TypeChecker,
	node: ts.TemplateLiteralTypeNode,
	packageExports: Array<ts.Symbol>,
	thisModule: Module,
): TemplateLiteralType => {
	return {
		head: node.head.text,
		templateSpans: node.templateSpans.map((span) =>
			convertTemplateLiteralTypeSpan(
				typeChecker,
				span,
				packageExports,
				thisModule,
			),
		),
	};
};

// TemplateLiteralTypeSpan.
let convertTemplateLiteralTypeSpan = (
	typeChecker: ts.TypeChecker,
	node: ts.TemplateLiteralTypeSpan,
	packageExports: Array<ts.Symbol>,
	thisModule: Module,
): TemplateLiteralTypeSpan => {
	return {
		type: convertTypeNode(typeChecker, node.type, packageExports, thisModule),
		literal: node.literal.text,
	};
};

// TupleTypeNode.
let convertTupleTypeNode = (
	typeChecker: ts.TypeChecker,
	node: ts.TupleTypeNode,
	packageExports: Array<ts.Symbol>,
	thisModule: Module,
): TupleType => {
	return {
		types: node.elements.map((node) =>
			convertTypeNode(typeChecker, node, packageExports, thisModule),
		),
	};
};

// TypeParameterNode.
let convertTypeParameterNode = (
	typeChecker: ts.TypeChecker,
	node: ts.TypeParameterDeclaration,
	packageExports: Array<ts.Symbol>,
	thisModule: Module,
): TypeParameter => {
	return {
		constraint: node.constraint
			? convertTypeNode(
					typeChecker,
					node.constraint,
					packageExports,
					thisModule,
				)
			: undefined,
		default: node.default
			? convertTypeNode(typeChecker, node.default, packageExports, thisModule)
			: undefined,
	};
};

// TypePredicateNode.
let convertTypePredicateNode = (
	typeChecker: ts.TypeChecker,
	node: ts.TypePredicateNode,
	packageExports: Array<ts.Symbol>,
	thisModule: Module,
): PredicateType => {
	let asserts = node.assertsModifier !== undefined;
	return {
		name: node.parameterName.getText(),
		type: node.type
			? convertTypeNode(typeChecker, node.type, packageExports, thisModule)
			: undefined,
		asserts,
	};
};

// TypeOperatorNode.
let operatorToName = {
	[ts.SyntaxKind.KeyOfKeyword]: "keyof",
	[ts.SyntaxKind.UniqueKeyword]: "unique",
	[ts.SyntaxKind.ReadonlyKeyword]: "readonly",
};
let convertTypeOperatorNode = (
	typeChecker: ts.TypeChecker,
	node: ts.TypeOperatorNode,
	packageExports: Array<ts.Symbol>,
	thisModule: Module,
): TypeOperatorType => {
	return {
		operator: operatorToName[node.operator],
		type: convertTypeNode(typeChecker, node.type, packageExports, thisModule),
	};
};

// TypeQueryNode.
let convertTypeQueryNode = (
	typeChecker: ts.TypeChecker,
	node: ts.TypeQueryNode,
): TypeQueryType => {
	let symbol = typeChecker.getSymbolAtLocation(node.exprName)!;
	symbol = getAliasedSymbolIfAliased(typeChecker, symbol);
	return {
		name: node.exprName.getText(),
	};
};

// TypeReferenceTypeNode.
let convertTypeReferenceTypeNode = (
	typeChecker: ts.TypeChecker,
	node: ts.TypeReferenceNode,
	packageExports: Array<ts.Symbol>,
	thisModule: Module,
): ReferenceType => {
	let symbol = typeChecker.getSymbolAtLocation(node.typeName)!;
	let resolved = getAliasedSymbolIfAliased(typeChecker, symbol);
	let typeArguments = node.typeArguments?.map((typeArgument) =>
		convertTypeNode(typeChecker, typeArgument, packageExports, thisModule),
	);
	let declaration = resolved.declarations![0]!;
	let isTypeParameter = ts.isTypeParameterDeclaration(declaration);
	let fullyQualifiedName = getFullyQualifiedName(
		typeChecker,
		declaration,
		resolved,
	);
	return {
		location: convertLocation(declaration),
		name: node.typeName.getText(),
		fullyQualifiedName,
		typeArguments: typeArguments ?? [],
		isTypeParameter,
		isExported: isExported(
			typeChecker,
			packageExports,
			thisModule,
			symbol,
			declaration,
		),
	};
};

// UnionTypeNode.
let convertUnionTypeNode = (
	typeChecker: ts.TypeChecker,
	node: ts.UnionTypeNode,
	packageExports: Array<ts.Symbol>,
	thisModule: Module,
): UnionType => {
	return {
		types: node.types.map((node) =>
			convertTypeNode(typeChecker, node, packageExports, thisModule),
		),
	};
};

let convertLocation = (node: ts.Node): Location => {
	let sourceFile = node.getSourceFile();
	let start = ts.getLineAndCharacterOfPosition(sourceFile, node.getStart());
	let end = ts.getLineAndCharacterOfPosition(sourceFile, node.getEnd());
	let module_ = typescript.moduleFromFileName(sourceFile.fileName);
	return {
		module: module_,
		range: {
			start,
			end,
		},
	};
};

function isExported(
	typeChecker: ts.TypeChecker,
	packageExports: Array<ts.Symbol>,
	thisModule: Module,
	symbol: ts.Symbol,
	node: ts.Node,
) {
	// If the symbol is not from our package, then it is exported.
	let module_ = typescript.moduleFromFileName(node.getSourceFile().fileName);
	// If the symbol is from a library file, then it is exported.
	if (module_.kind === "dts") {
		return true;
	} else if (
		(module_.kind === "ts" || module_.kind === "js") &&
		(thisModule.kind === "ts" || thisModule.kind === "js")
	) {
		// If the symbol is from a different package, then it is exported.
		if (module_.referent !== thisModule.referent) {
			return true;
		}
	}

	// Go through all globalExports to see if our symbol is there.
	return packageExports.some((export_) => {
		let isExported = false;
		// If the export_ is an alias.
		if ((export_.flags & ts.SymbolFlags.Alias) !== 0) {
			isExported = typeChecker.getAliasedSymbol(export_) === symbol;
		}
		// If the symbol is an alias.
		if ((symbol.flags & ts.SymbolFlags.Alias) !== 0) {
			isExported =
				isExported || typeChecker.getAliasedSymbol(symbol) === export_;
		}
		return isExported || export_ === symbol;
	});
}

function getAliasedSymbolIfAliased(
	typeChecker: ts.TypeChecker,
	symbol: ts.Symbol,
) {
	if ((symbol.flags & ts.SymbolFlags.Alias) !== 0) {
		return typeChecker.getAliasedSymbol(symbol);
	}
	return symbol;
}

let convertComment = (
	_typeChecker: ts.TypeChecker,
	declaration: ts.Node,
	_symbol?: ts.Symbol,
): Comment => {
	let jsDocCommentsAndTags = ts.getJSDocCommentsAndTags(declaration)?.[0];
	if (jsDocCommentsAndTags === undefined) {
		return {
			text: "",
			tags: [],
		};
	}
	let text = ts.getTextOfJSDocComment(jsDocCommentsAndTags.comment);
	return {
		text: text ?? "",
		tags: [],
	};
};

let getFullyQualifiedName = (
	_typeChecker: ts.TypeChecker,
	declaration: ts.Node,
	symbol: ts.Symbol,
) => {
	let parent = declaration.parent;
	let fqn = [];
	while (parent) {
		if ((parent as ts.NamedDeclaration).name) {
			fqn.push((parent as ts.NamedDeclaration).name?.getText());
		}
		parent = parent.parent;
	}
	fqn.reverse();
	fqn.push(symbol.getName());
	return fqn.join(".");
};
