import { assert } from "./assert.ts";
import { Module } from "./module.ts";
import { Range } from "./range.ts";
import { compilerOptions, host } from "./typescript.ts";
import * as typescript from "./typescript.ts";
import ts from "typescript";

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
	exports: { [key: string]: Symbol };
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
	constructSignature: ConstructSignature;
	properties: Array<ClassProperty>;
	comment: Comment;
};

type ConstructSignature = {
	parameters: { [key: string]: Parameter };
	return: Type;
};

type ClassProperty = {
	name: string;
	type: Type;
	static?: boolean;
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
	constructSignatures: Array<ConstructSignature>;
	comment: Comment;
};

type InterfaceProperty = {
	type: Type;
	readonly: boolean | undefined;
};

type NamespaceDeclaration = {
	location: Location;
	exports: { [key: string]: Symbol };
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
	constructSignatures: Array<ConstructSignature>;
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
	let exports_;
	if (!symbol) {
		exports_ = typeChecker
			.getSymbolsInScope(sourceFile, ts.SymbolFlags.ModuleMember)
			.filter(
				(s) =>
					s.getDeclarations()?.some((d) => d.getSourceFile() === sourceFile),
			);
	} else {
		exports_ = typeChecker.getExportsOfModule(symbol);
	}

	// Convert the exports.
	let exports: { [key: string]: Symbol } = {};
	for (let export_ of exports_) {
		exports[export_.getName()] = convertSymbol(typeChecker, export_);
	}

	return {
		exports,
	};
};

let convertSymbol = (
	typeChecker: ts.TypeChecker,
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
			value: convertModuleSymbol(typeChecker, moduleExport),
		});
	}

	// Class.
	if (
		ts.SymbolFlags.Class & symbolFlags &&
		!(ts.SymbolFlags.Interface & symbolFlags)
	) {
		declarations.push({
			kind: "class",
			value: convertClassSymbol(typeChecker, moduleExport),
		});
	}

	// Variable.
	if (ts.SymbolFlags.Variable & symbolFlags) {
		declarations.push({
			kind: "variable",
			value: convertVariableSymbol(typeChecker, moduleExport),
		});
	}

	// Function.
	if (ts.SymbolFlags.Function & symbolFlags) {
		let functionDeclarations: Array<Declaration> = convertFunctionSymbol(
			typeChecker,
			moduleExport,
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
			value: convertTypeAliasSymbol(typeChecker, moduleExport),
		});
	}

	// Alias.
	if (ts.SymbolFlags.Alias & symbolFlags) {
		let symbol = convertSymbol(
			typeChecker,
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
		).map((interfaceDeclaration) => ({
			kind: "interface",
			value: interfaceDeclaration,
		}));
		declarations.push(...interfaceDeclarations);
	}

	// Handle default export Property.
	if (
		ts.SymbolFlags.Property & moduleExport.flags &&
		moduleExport.getName() == "default"
	) {
		declarations.push({
			kind: "variable",
			value: convertDefaultExportSymbol(typeChecker, moduleExport),
		});
	}

	return { declarations };
};

// ModuleSymbol.
let convertModuleSymbol = (
	typeChecker: ts.TypeChecker,
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
				namespaceExport,
			);
		}
	}

	// Convert the declaration locations.
	let declaration = symbol.declarations?.filter((declaration) => {
		return declaration.kind === ts.SyntaxKind.ModuleDeclaration;
	})?.[0];
	if (!declaration) {
		throw new Error();
	}

	return {
		location: convertLocation(declaration),
		exports,
		comment: convertComment(typeChecker, declaration, symbol),
	};
};

// ClassSymbol.
let convertClassSymbol = (
	typeChecker: ts.TypeChecker,
	symbol: ts.Symbol,
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
		if (instanceProperty.flags & ts.SymbolFlags.ClassMember) {
			properties.push(
				convertClassPropertySymbol(typeChecker, instanceProperty),
			);
		}
	}

	// Get the static properties of the class.
	let staticType = typeChecker.getTypeOfSymbolAtLocation(symbol, declaration);
	for (let staticProperty of typeChecker.getPropertiesOfType(staticType)) {
		// Ignore prototype.
		if (staticProperty.flags & ts.SymbolFlags.Prototype) continue;

		if (staticProperty.flags & ts.SymbolFlags.ClassMember) {
			properties.push(convertClassPropertySymbol(typeChecker, staticProperty));
		}
	}

	// Get the constructor signature.
	let constructSignature = staticType
		.getConstructSignatures()
		.map((signature) => {
			return convertConstructSignature(typeChecker, signature);
		})[0]!;

	return {
		location: convertLocation(declaration),
		properties,
		constructSignature,
		comment: convertComment(typeChecker, declaration, symbol),
	};
};

// VariableSymbol.
let convertVariableSymbol = (
	typeChecker: ts.TypeChecker,
	symbol: ts.Symbol,
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
		type = convertTypeNode(typeChecker, declaration.type);
	} else {
		type = convertType(typeChecker, typeChecker.getTypeOfSymbol(symbol));
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
): DefaultExportDeclaration => {
	let declaration = symbol.valueDeclaration;
	if (!declaration) {
		throw new Error();
	}
	// Convert the declaration.
	let type = convertType(typeChecker, typeChecker.getTypeOfSymbol(symbol));

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
		convertFunctionDeclaration(typeChecker, declaration),
	);
};

// FunctionDeclaration.
let convertFunctionDeclaration = (
	typeChecker: ts.TypeChecker,
	declaration: ts.FunctionDeclaration,
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
						? convertTypeNode(typeChecker, parameter.type)
						: convertType(typeChecker, type),
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
					convertTypeParameterType(typeChecker, type),
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
			value: convertTypePredicate(typeChecker, predicate),
		};
	} else if (declaration.type) {
		returnType = convertTypeNode(typeChecker, declaration.type);
	} else {
		returnType = convertType(typeChecker, signature.getReturnType());
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
): TypeDeclaration => {
	// Get the declaration.
	let declaration = symbol
		.getDeclarations()
		?.find((d): d is ts.TypeAliasDeclaration => ts.isTypeAliasDeclaration(d));
	if (!declaration) {
		throw new Error();
	}

	// Convert the declaration.
	let type = convertTypeNode(typeChecker, declaration.type);
	let typeParameters = declaration.typeParameters?.map((typeParameter) => [
		typeParameter.name.getText(),
		convertTypeParameterNode(typeChecker, typeParameter),
	]);

	return {
		location: convertLocation(declaration),
		type,
		typeParameters: Object.fromEntries(typeParameters ?? []),
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
): Array<InterfaceDeclaration> => {
	// Get the declarations.
	let declarations = symbol
		.getDeclarations()
		?.filter((d): d is ts.InterfaceDeclaration => ts.isInterfaceDeclaration(d));
	if (!declarations || declarations.length == 0) {
		throw new Error();
	}

	return declarations.map((declaration) => {
		// Get the index signature.
		let indexSignature = declaration.members
			.filter((d): d is ts.IndexSignatureDeclaration =>
				ts.isIndexSignatureDeclaration(d),
			)
			.map((member) => convertIndexSignature(typeChecker, member))?.[0];

		// Get the construct signatures.
		let constructSignatures = declaration.members
			.filter((d): d is ts.ConstructSignatureDeclaration =>
				ts.isConstructSignatureDeclaration(d),
			)
			.map((member) => {
				return convertConstructSignatureDeclaration(typeChecker, member);
			});

		// Get the members.
		let properties = Object.fromEntries(
			declaration.members
				.filter((d) => !ts.isIndexSignatureDeclaration(d))
				.map((member) => {
					return [
						member.name?.getText(),
						convertTypeElement(typeChecker, member),
					];
				}),
		);

		return {
			location: convertLocation(declaration),
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
): IndexSignature => {
	let parameter = declaration.parameters[0]!;
	let key = convertParameterNode(typeChecker, parameter);
	return {
		name: parameter.name.getText(),
		type: convertTypeNode(typeChecker, declaration.type),
		key,
	};
};

// ConstructSignature.
let convertConstructSignature = (
	typeChecker: ts.TypeChecker,
	signature: ts.Signature,
): ConstructSignature => {
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
						? convertTypeNode(typeChecker, declaration.type)
						: convertType(typeChecker, typeChecker.getTypeOfSymbol(parameter)),
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
		returnType = convertTypeNode(typeChecker, declaration.type);
	} else {
		returnType = convertType(typeChecker, signature.getReturnType());
	}

	return { parameters, return: returnType };
};

// ConstructSignatureDeclaration.
let convertConstructSignatureDeclaration = (
	typeChecker: ts.TypeChecker,
	declaration: ts.ConstructSignatureDeclaration,
): ConstructSignature => {
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
						? convertTypeNode(typeChecker, parameter.type)
						: convertType(
								typeChecker,
								typeChecker.getTypeAtLocation(parameter),
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
		returnType = convertTypeNode(typeChecker, declaration.type);
	} else {
		let type = typeChecker.getTypeAtLocation(declaration);
		returnType = convertType(typeChecker, type);
	}

	return { parameters, return: returnType };
};

// TypeElement.
let convertTypeElement = (
	typeChecker: ts.TypeChecker,
	member: ts.TypeElement,
): InterfaceProperty => {
	// Get the type.
	let type = typeChecker.getTypeAtLocation(member);
	return {
		type: convertType(typeChecker, type),
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
	if (typeof constantValue == "string") {
		value = {
			kind: "string",
			value: constantValue,
		};
	}
	if (typeof constantValue == "number") {
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
): ClassProperty => {
	let flags = ts.getCombinedModifierFlags(property.valueDeclaration!);
	let type = convertType(typeChecker, typeChecker.getTypeOfSymbol(property));
	return {
		name: property.getName(),
		comment: convertComment(typeChecker, property.valueDeclaration!, property),
		type,
		static: (flags & ts.ModifierFlags.Static) !== 0,
		private: (flags & ts.ModifierFlags.Private) !== 0,
	};
};

let convertType = (typeChecker: ts.TypeChecker, type: ts.Type): Type => {
	let node = typeChecker.typeToTypeNode(
		type,
		undefined,
		ts.NodeBuilderFlags.IgnoreErrors,
	)!;
	if (node.kind === ts.SyntaxKind.ArrayType) {
		return { kind: "array", value: convertArrayType(typeChecker, type) };
	} else if (node.kind === ts.SyntaxKind.FunctionType) {
		return { kind: "function", value: convertFunctionType(typeChecker, type) };
	} else if (node.kind === ts.SyntaxKind.IndexedAccessType) {
		return {
			kind: "indexed_access",
			value: convertIndexedAccessType(
				typeChecker,
				type as ts.IndexedAccessType,
			),
		};
	} else if (node.kind === ts.SyntaxKind.IntersectionType) {
		return {
			kind: "intersection",
			value: convertIntersectionType(typeChecker, type as ts.IntersectionType),
		};
	} else if (keywordSet.has(node.kind)) {
		return { kind: "keyword", value: convertKeywordType(node.kind) };
	} else if (node.kind === ts.SyntaxKind.LiteralType) {
		return {
			kind: "literal",
			value: convertLiteralTypeNode(typeChecker, node as ts.LiteralTypeNode),
		};
	} else if (node.kind === ts.SyntaxKind.TypeLiteral) {
		return { kind: "object", value: convertObjectType(typeChecker, type) };
	} else if (node.kind === ts.SyntaxKind.TypeReference) {
		return {
			kind: "reference",
			value: convertTypeReferenceType(typeChecker, type),
		};
	} else if (node.kind === ts.SyntaxKind.TupleType) {
		return {
			kind: "tuple",
			value: convertTupleType(typeChecker, type as ts.TupleType),
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
): ArrayType => {
	return {
		type: convertType(
			typeChecker,
			typeChecker.getTypeArguments(type as ts.TypeReference)[0]!,
		),
	};
};

// FunctionType.
let convertFunctionType = (
	typeChecker: ts.TypeChecker,
	type: ts.Type,
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
							? convertTypeNode(typeChecker, parameterDeclaration.type)
							: convertType(typeChecker, parameterType),
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
					convertTypeParameterType(typeChecker, typeParameter),
				]),
			);
		}

		let returnType: Type;
		let predicate = typeChecker.getTypePredicateOfSignature(signature);
		if (predicate) {
			returnType = {
				kind: "predicate",
				value: convertTypePredicate(typeChecker, predicate),
			};
		} else if (declaration.type) {
			returnType = convertTypeNode(typeChecker, declaration.type);
		} else {
			returnType = convertType(typeChecker, signature.getReturnType());
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
): IndexedAccessType => {
	return {
		objectType: convertType(typeChecker, type.objectType),
		indexType: convertType(typeChecker, type.indexType),
	};
};

// IntersectionType
let convertIntersectionType = (
	typeChecker: ts.TypeChecker,
	type: ts.IntersectionType,
): IntersectionType => {
	return {
		types: type.types.map((type) => convertType(typeChecker, type)),
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
): ObjectType => {
	// Get the index signature.
	let indexSignature: IndexSignature | undefined;
	let indexSymbol = type.symbol?.members?.get("__index" as ts.__String);
	if (indexSymbol) {
		let declaration =
			indexSymbol.declarations![0]! as ts.IndexSignatureDeclaration;
		indexSignature = convertIndexSignature(typeChecker, declaration);
	}

	// Get the construct signatures.
	let constructSignatures = type.getConstructSignatures().map((signature) => {
		let signatureDeclaration = signature.getDeclaration();
		assert(ts.isConstructSignatureDeclaration(signatureDeclaration));
		return convertConstructSignatureDeclaration(
			typeChecker,
			signatureDeclaration,
		);
	});

	// Get the properties.
	let properties = typeChecker.getPropertiesOfType(type).map((property) => {
		// Convert the property.
		let declaration = property.getDeclarations()?.[0]!;
		let type: Type;
		if (ts.isPropertySignature(declaration) && declaration.type) {
			type = convertTypeNode(typeChecker, declaration.type);
		} else {
			type = convertType(typeChecker, typeChecker.getTypeOfSymbol(property));
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
): TupleType => {
	return {
		types: typeChecker
			.getTypeArguments(type)
			.map((type) => convertType(typeChecker, type)),
	};
};

// TypeParameter.
let convertTypeParameterType = (
	typeChecker: ts.TypeChecker,
	type: ts.Type,
): TypeParameter => {
	let constraint = type.getConstraint();
	let default_ = type.getDefault();
	return {
		constraint: constraint ? convertType(typeChecker, constraint) : undefined,
		default: default_ ? convertType(typeChecker, default_) : undefined,
	};
};

// TypePredicate.
let convertTypePredicate = (
	typeChecker: ts.TypeChecker,
	type: ts.TypePredicate,
): PredicateType => {
	let asserts =
		type.kind === ts.TypePredicateKind.AssertsIdentifier ||
		type.kind === ts.TypePredicateKind.AssertsThis;
	return {
		name: type.parameterName ?? "this",
		type: type.type ? convertType(typeChecker, type.type) : undefined,
		asserts,
	};
};

// TypeReferenceType.
let convertTypeReferenceType = (
	typeChecker: ts.TypeChecker,
	type: ts.Type,
): ReferenceType => {
	let isTypeParameter = (type.flags & ts.TypeFlags.TypeParameter) !== 0;
	if (type.aliasSymbol) {
		let aliasSymbol = type.aliasSymbol;
		let typeArguments = type.aliasTypeArguments?.map((typeArgument) =>
			convertType(typeChecker, typeArgument),
		);
		let declaration = aliasSymbol.declarations![0]!;
		let fullyQualifiedName = typeChecker.getFullyQualifiedName(aliasSymbol);
		return {
			name: aliasSymbol.getName(),
			fullyQualifiedName,
			location: convertLocation(declaration),
			typeArguments: typeArguments ?? [],
			isTypeParameter,
		};
	} else {
		let typeArguments = typeChecker
			.getTypeArguments(type as ts.TypeReference)
			.map((typeArgument) => {
				return convertType(typeChecker, typeArgument);
			});
		let symbol = type.symbol;
		let declaration = symbol.declarations![0]!;
		let name = getAliasedSymbolIfAliased(typeChecker, symbol).getName();
		let fullyQualifiedName = typeChecker.getFullyQualifiedName(symbol);
		return {
			name,
			fullyQualifiedName,
			location: convertLocation(declaration),
			typeArguments: typeArguments ?? [],
			isTypeParameter,
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
): Type => {
	if (ts.isArrayTypeNode(node)) {
		return { kind: "array", value: convertArrayTypeNode(typeChecker, node) };
	} else if (ts.isConditionalTypeNode(node)) {
		return {
			kind: "conditional",
			value: convertConditionalTypeNode(typeChecker, node),
		};
	} else if (ts.isFunctionTypeNode(node)) {
		return {
			kind: "function",
			value: convertFunctionTypeNode(typeChecker, node),
		};
	} else if (ts.isInferTypeNode(node)) {
		return { kind: "infer", value: convertInferTypeNode(typeChecker, node) };
	} else if (ts.isIndexedAccessTypeNode(node)) {
		return {
			kind: "indexed_access",
			value: convertIndexedAccessTypeNode(typeChecker, node),
		};
	} else if (ts.isIntersectionTypeNode(node)) {
		return {
			kind: "intersection",
			value: convertIntersectionTypeNode(typeChecker, node),
		};
	} else if (keywordSet.has(node.kind)) {
		return { kind: "keyword", value: convertKeywordType(node.kind) };
	} else if (ts.isLiteralTypeNode(node)) {
		return {
			kind: "literal",
			value: convertLiteralTypeNode(typeChecker, node),
		};
	} else if (ts.isMappedTypeNode(node)) {
		return { kind: "mapped", value: convertMappedTypeNode(typeChecker, node) };
	} else if (ts.isTypeLiteralNode(node)) {
		return { kind: "object", value: convertObjectTypeNode(typeChecker, node) };
	} else if (ts.isTypePredicateNode(node)) {
		return {
			kind: "predicate",
			value: convertTypePredicateNode(typeChecker, node),
		};
	} else if (ts.isTypeReferenceNode(node)) {
		return {
			kind: "reference",
			value: convertTypeReferenceTypeNode(typeChecker, node),
		};
	} else if (ts.isTemplateLiteralTypeNode(node)) {
		return {
			kind: "template_literal",
			value: convertTemplateLiteralTypeNode(typeChecker, node),
		};
	} else if (ts.isTupleTypeNode(node)) {
		return { kind: "tuple", value: convertTupleTypeNode(typeChecker, node) };
	} else if (ts.isTypeOperatorNode(node)) {
		return {
			kind: "type_operator",
			value: convertTypeOperatorNode(typeChecker, node),
		};
	} else if (ts.isTypeQueryNode(node)) {
		return {
			kind: "type_query",
			value: convertTypeQueryNode(typeChecker, node),
		};
	} else if (ts.isUnionTypeNode(node)) {
		return { kind: "union", value: convertUnionTypeNode(typeChecker, node) };
	} else {
		let type = typeChecker.getTypeFromTypeNode(node);
		return { kind: "other", value: typeChecker.typeToString(type) };
	}
};

// ArrayTypeNode.
let convertArrayTypeNode = (
	typeChecker: ts.TypeChecker,
	node: ts.ArrayTypeNode,
): ArrayType => {
	return {
		type: convertTypeNode(typeChecker, node.elementType),
	};
};

// ConditionalTypeNode.
let convertConditionalTypeNode = (
	typeChecker: ts.TypeChecker,
	node: ts.ConditionalTypeNode,
): ConditionalType => {
	return {
		checkType: convertTypeNode(typeChecker, node.checkType),
		extendsType: convertTypeNode(typeChecker, node.extendsType),
		trueType: convertTypeNode(typeChecker, node.trueType),
		falseType: convertTypeNode(typeChecker, node.falseType),
	};
};

// FunctionTypeNode.
let convertFunctionTypeNode = (
	typeChecker: ts.TypeChecker,
	node: ts.FunctionTypeNode,
): FunctionType => {
	let parameters = node.parameters?.map((parameter) => [
		parameter.name.getText(),
		convertParameterNode(typeChecker, parameter),
	]);
	let typeParameters = node.typeParameters?.map((typeParameter) => [
		typeParameter.name.getText(),
		convertTypeParameterNode(typeChecker, typeParameter),
	]);
	return {
		signatures: [
			{
				location: convertLocation(node),
				parameters: Object.fromEntries(parameters ?? []),
				typeParameters: Object.fromEntries(typeParameters || []),
				return: convertTypeNode(typeChecker, node.type),
			},
		],
	};
};

// IndexedAccessTypeNode.
let convertIndexedAccessTypeNode = (
	typeChecker: ts.TypeChecker,
	node: ts.IndexedAccessTypeNode,
): IndexedAccessType => {
	return {
		objectType: convertTypeNode(typeChecker, node.objectType),
		indexType: convertTypeNode(typeChecker, node.indexType),
	};
};

// InferTypeNode.
let convertInferTypeNode = (
	typeChecker: ts.TypeChecker,
	node: ts.InferTypeNode,
): InferType => {
	return {
		typeParameter: {
			[node.typeParameter.name.getText()]: convertTypeParameterNode(
				typeChecker,
				node.typeParameter,
			),
		},
	};
};

// IntersectionTypeNode.
let convertIntersectionTypeNode = (
	typeChecker: ts.TypeChecker,
	node: ts.IntersectionTypeNode,
): IntersectionType => {
	return {
		types: node.types.map((node) => convertTypeNode(typeChecker, node)),
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
): MappedType => {
	return {
		type: convertTypeNode(typeChecker, node.type!),
		constraint: convertTypeNode(typeChecker, node.typeParameter.constraint!),
		typeParameterName: node.typeParameter.name.text,
		nameType: node.nameType
			? convertTypeNode(typeChecker, node.nameType)
			: undefined,
	};
};

// ObjectTypeNode.
let convertObjectTypeNode = (
	typeChecker: ts.TypeChecker,
	node: ts.TypeLiteralNode,
): ObjectType => {
	let type = typeChecker.getTypeAtLocation(node);
	return convertObjectType(typeChecker, type);
};

// ParameterNode.
let convertParameterNode = (
	typeChecker: ts.TypeChecker,
	node: ts.ParameterDeclaration,
): Parameter => {
	return {
		optional: node.questionToken ? true : false,
		dotDotDotToken: node.dotDotDotToken ? true : false,
		type: node.type
			? convertTypeNode(typeChecker, node.type)
			: convertType(typeChecker, typeChecker.getTypeAtLocation(node)),
	};
};

// TemplateLiteralTypeNode.
let convertTemplateLiteralTypeNode = (
	typeChecker: ts.TypeChecker,
	node: ts.TemplateLiteralTypeNode,
): TemplateLiteralType => {
	return {
		head: node.head.text,
		templateSpans: node.templateSpans.map((span) =>
			convertTemplateLiteralTypeSpan(typeChecker, span),
		),
	};
};

// TemplateLiteralTypeSpan.
let convertTemplateLiteralTypeSpan = (
	typeChecker: ts.TypeChecker,
	node: ts.TemplateLiteralTypeSpan,
): TemplateLiteralTypeSpan => {
	return {
		type: convertTypeNode(typeChecker, node.type),
		literal: node.literal.text,
	};
};

// TupleTypeNode.
let convertTupleTypeNode = (
	typeChecker: ts.TypeChecker,
	node: ts.TupleTypeNode,
): TupleType => {
	return {
		types: node.elements.map((node) => convertTypeNode(typeChecker, node)),
	};
};

// TypeParameterNode.
let convertTypeParameterNode = (
	typeChecker: ts.TypeChecker,
	node: ts.TypeParameterDeclaration,
): TypeParameter => {
	return {
		constraint: node.constraint
			? convertTypeNode(typeChecker, node.constraint)
			: undefined,
		default: node.default
			? convertTypeNode(typeChecker, node.default)
			: undefined,
	};
};

// TypePredicateNode.
let convertTypePredicateNode = (
	typeChecker: ts.TypeChecker,
	node: ts.TypePredicateNode,
): PredicateType => {
	let asserts = node.assertsModifier !== undefined;
	return {
		name: node.parameterName.getText(),
		type: node.type ? convertTypeNode(typeChecker, node.type) : undefined,
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
): TypeOperatorType => {
	return {
		operator: operatorToName[node.operator],
		type: convertTypeNode(typeChecker, node.type),
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
): ReferenceType => {
	let symbol = typeChecker.getSymbolAtLocation(node.typeName)!;
	let resolved = getAliasedSymbolIfAliased(typeChecker, symbol);
	let typeArguments = node.typeArguments?.map((typeArgument) =>
		convertTypeNode(typeChecker, typeArgument),
	);
	let declaration = resolved.declarations![0]!;
	let isTypeParameter = ts.isTypeParameterDeclaration(declaration);
	let name = resolved.getName();
	let fullyQualifiedName = typeChecker.getFullyQualifiedName(symbol);
	return {
		location: convertLocation(declaration),
		name,
		fullyQualifiedName,
		typeArguments: typeArguments ?? [],
		isTypeParameter,
	};
};

// UnionTypeNode.
let convertUnionTypeNode = (
	typeChecker: ts.TypeChecker,
	node: ts.UnionTypeNode,
): UnionType => {
	return {
		types: node.types.map((node) => convertTypeNode(typeChecker, node)),
	};
};

let convertLocation = (node: ts.Node): Location => {
	let sourceFile = node.getSourceFile();
	let start = ts.getLineAndCharacterOfPosition(sourceFile, node.getStart());
	let end = ts.getLineAndCharacterOfPosition(sourceFile, node.getEnd());
	let module_ = typescript.moduleFromFileName(sourceFile.fileName);
	let docModule: Module;
	if (module_.kind == "library") {
		docModule = module_;
	} else if (module_.kind == "normal") {
		docModule = {
			kind: "normal",
			value: {
				path: module_.value.path,
				package: module_.value.package,
				lock: module_.value.lock,
			},
		};
	} else {
		throw new Error("Invalid module kind.");
	}
	return {
		module: docModule,
		range: {
			start,
			end,
		},
	};
};

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
	if (jsDocCommentsAndTags == undefined) {
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
