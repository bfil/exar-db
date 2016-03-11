﻿declare type PropertyKey = string | number | symbol;

interface Symbol {
    /** Returns a string representation of an object. */
    toString(): string;

    /** Returns the primitive value of the specified object. */
    valueOf(): Object;

    [Symbol.toStringTag]: string;
}

interface SymbolConstructor {
    /**
      * A reference to the prototype.
      */
    prototype: Symbol;

    /**
      * Returns a new unique Symbol value.
      * @param  description Description of the new Symbol object.
      */
    (description?: string|number): symbol;

    /**
      * Returns a Symbol object from the global symbol registry matching the given key if found.
      * Otherwise, returns a new symbol with this key.
      * @param key key to search for.
      */
    for(key: string): symbol;

    /**
      * Returns a key from the global symbol registry matching the given Symbol if found.
      * Otherwise, returns a undefined.
      * @param sym Symbol to find the key for.
      */
    keyFor(sym: symbol): string;

    // Well-known Symbols

    /**
      * A method that determines if a constructor object recognizes an object as one of the
      * constructor’s instances. Called by the semantics of the instanceof operator.
      */
    hasInstance: symbol;

    /**
      * A Boolean value that if true indicates that an object should flatten to its array elements
      * by Array.prototype.concat.
      */
    isConcatSpreadable: symbol;

    /**
      * A Boolean value that if true indicates that an object may be used as a regular expression.
      */
    isRegExp: symbol;

    /**
      * A method that returns the default iterator for an object.Called by the semantics of the
      * for-of statement.
      */
    iterator: symbol;

    /**
      * A method that converts an object to a corresponding primitive value.Called by the ToPrimitive
      * abstract operation.
      */
    toPrimitive: symbol;

    /**
      * A String value that is used in the creation of the default string description of an object.
      * Called by the built- in method Object.prototype.toString.
      */
    toStringTag: symbol;

    /**
      * An Object whose own property names are property names that are excluded from the with
      * environment bindings of the associated objects.
      */
    unscopables: symbol;
}
declare var Symbol: SymbolConstructor;

interface Object {
    /**
      * Determines whether an object has a property with the specified name.
      * @param v A property name.
      */
    hasOwnProperty(v: PropertyKey): boolean;

    /**
      * Determines whether a specified property is enumerable.
      * @param v A property name.
      */
    propertyIsEnumerable(v: PropertyKey): boolean;
}

interface ObjectConstructor {
    /**
      * Copy the values of all of the enumerable own properties from one or more source objects to a
      * target object. Returns the target object.
      * @param target The target object to copy to.
      * @param sources One or more source objects to copy properties from.
      */
    assign(target: any, ...sources: any[]): any;

    /**
      * Returns an array of all symbol properties found directly on object o.
      * @param o Object to retrieve the symbols from.
      */
    getOwnPropertySymbols(o: any): symbol[];

    /**
      *  Returns true if the values are the same value, false otherwise.
      * @param value1 The first value.
      * @param value2 The second value.
      */
    is(value1: any, value2: any): boolean;

    /**
      * Sets the prototype of a specified object o to  object proto or null. Returns the object o.
      * @param o The object to change its prototype.
      * @param proto The value of the new prototype or null.
      */
    setPrototypeOf(o: any, proto: any): any;

    /**
      * Gets the own property descriptor of the specified object.
      * An own property descriptor is one that is defined directly on the object and is not
      * inherited from the object's prototype.
      * @param o Object that contains the property.
      * @param p Name of the property.
    */
    getOwnPropertyDescriptor(o: any, propertyKey: PropertyKey): PropertyDescriptor;

    /**
      * Adds a property to an object, or modifies attributes of an existing property.
      * @param o Object on which to add or modify the property. This can be a native JavaScript
      * object (that is, a user-defined object or a built in object) or a DOM object.
      * @param p The property name.
      * @param attributes Descriptor for the property. It can be for a data property or an accessor
      *  property.
      */
    defineProperty(o: any, propertyKey: PropertyKey, attributes: PropertyDescriptor): any;
}

interface Function {
    /**
      * Returns a new function object that is identical to the argument object in all ways except
      * for its identity and the value of its HomeObject internal slot.
      */
    toMethod(newHome: Object): Function;

    /**
      * Returns the name of the function. Function names are read-only and can not be changed.
      */
    name: string;
}

interface NumberConstructor {
    /**
      * The value of Number.EPSILON is the difference between 1 and the smallest value greater than 1
      * that is representable as a Number value, which is approximately:
      * 2.2204460492503130808472633361816 x 10‍−‍16.
      */
    EPSILON: number;

    /**
      * Returns true if passed value is finite.
      * Unlike the global isFininte, Number.isFinite doesn't forcibly convert the parameter to a
      * number. Only finite values of the type number, result in true.
      * @param number A numeric value.
      */
    isFinite(number: number): boolean;

    /**
      * Returns true if the value passed is an integer, false otherwise.
      * @param number A numeric value.
      */
    isInteger(number: number): boolean;

    /**
      * Returns a Boolean value that indicates whether a value is the reserved value NaN (not a
      * number). Unlike the global isNaN(), Number.isNaN() doesn't forcefully convert the parameter
      * to a number. Only values of the type number, that are also NaN, result in true.
      * @param number A numeric value.
      */
    isNaN(number: number): boolean;

    /**
      * Returns true if the value passed is a safe integer.
      * @param number A numeric value.
      */
    isSafeInteger(number: number): boolean;

    /**
      * The value of the largest integer n such that n and n + 1 are both exactly representable as
      * a Number value.
      * The value of Number.MIN_SAFE_INTEGER is 9007199254740991 2^53 − 1.
      */
    MAX_SAFE_INTEGER: number;

    /**
      * The value of the smallest integer n such that n and n − 1 are both exactly representable as
      * a Number value.
      * The value of Number.MIN_SAFE_INTEGER is −9007199254740991 (−(2^53 − 1)).
      */
    MIN_SAFE_INTEGER: number;

    /**
      * Converts a string to a floating-point number.
      * @param string A string that contains a floating-point number.
      */
    parseFloat(string: string): number;

    /**
      * Converts A string to an integer.
      * @param s A string to convert into a number.
      * @param radix A value between 2 and 36 that specifies the base of the number in numString.
      * If this argument is not supplied, strings with a prefix of '0x' are considered hexadecimal.
      * All other strings are considered decimal.
      */
    parseInt(string: string, radix?: number): number;
}

interface ArrayLike<T> {


}

interface Array<T> {
    /** Iterator */
    [Symbol.iterator](): IterableIterator<T>;

    /**
      * Returns an array of key, value pairs for every entry in the array
      */
    entries(): IterableIterator<[number, T]>;

    /**
      * Returns an list of keys in the array
      */
    keys(): IterableIterator<number>;

    /**
      * Returns an list of values in the array
      */
    values(): IterableIterator<T>;

    /**
      * Returns the value of the first element in the array where predicate is true, and undefined
      * otherwise.
      * @param predicate find calls predicate once for each element of the array, in ascending
      * order, until it finds one where predicate returns true. If such an element is found, find
      * immediately returns that element value. Otherwise, find returns undefined.
      * @param thisArg If provided, it will be used as the this value for each invocation of
      * predicate. If it is not provided, undefined is used instead.
      */
    find(predicate: (value: T, index: number, obj: Array<T>) => boolean, thisArg?: any): T;

    /**
      * Returns the index of the first element in the array where predicate is true, and undefined
      * otherwise.
      * @param predicate find calls predicate once for each element of the array, in ascending
      * order, until it finds one where predicate returns true. If such an element is found, find
      * immediately returns that element value. Otherwise, find returns undefined.
      * @param thisArg If provided, it will be used as the this value for each invocation of
      * predicate. If it is not provided, undefined is used instead.
      */
    findIndex(predicate: (value: T) => boolean, thisArg?: any): number;

    /**
      * Returns the this object after filling the section identified by start and end with value
      * @param value value to fill array section with
      * @param start index to start filling the array at. If start is negative, it is treated as
      * length+start where length is the length of the array.
      * @param end index to stop filling the array at. If end is negative, it is treated as
      * length+end.
      */
    fill(value: T, start?: number, end?: number): T[];

    /**
      * Returns the this object after copying a section of the array identified by start and end
      * to the same array starting at position target
      * @param target If target is negative, it is treated as length+target where length is the
      * length of the array.
      * @param start If start is negative, it is treated as length+start. If end is negative, it
      * is treated as length+end.
      * @param end If not specified, length of the this object is used as its default value.
      */
    copyWithin(target: number, start: number, end?: number): T[];
}

interface ArrayConstructor {
    /**
      * Creates an array from an array-like object.
      * @param arrayLike An array-like object to convert to an array.
      * @param mapfn A mapping function to call on every element of the array.
      * @param thisArg Value of 'this' used to invoke the mapfn.
      */
    from<T, U>(arrayLike: ArrayLike<T>, mapfn: (v: T, k: number) => U, thisArg?: any): Array<U>;

    /**
      * Creates an array from an iterable object.
      * @param iterable An iterable object to convert to an array.
      * @param mapfn A mapping function to call on every element of the array.
      * @param thisArg Value of 'this' used to invoke the mapfn.
      */
    from<T, U>(iterable: Iterable<T>, mapfn: (v: T, k: number) => U, thisArg?: any): Array<U>;

    /**
      * Creates an array from an array-like object.
      * @param arrayLike An array-like object to convert to an array.
      */
    from<T>(arrayLike: ArrayLike<T>): Array<T>;

    /**
      * Creates an array from an iterable object.
      * @param iterable An iterable object to convert to an array.
      */
    from<T>(iterable: Iterable<T>): Array<T>;

    /**
      * Returns a new array from a set of elements.
      * @param items A set of elements to include in the new array object.
      */
    of<T>(...items: T[]): Array<T>;
}

interface String {
    /** Iterator */
    [Symbol.iterator](): IterableIterator<string>;

    /**
      * Returns a nonnegative integer Number less than 1114112 (0x110000) that is the code point
      * value of the UTF-16 encoded code point starting at the string element at position pos in
      * the String resulting from converting this object to a String.
      * If there is no element at that position, the result is undefined.
      * If a valid UTF-16 surrogate pair does not begin at pos, the result is the code unit at pos.
      */
    codePointAt(pos: number): number;

    /**
      * Returns true if searchString appears as a substring of the result of converting this
      * object to a String, at one or more positions that are
      * greater than or equal to position; otherwise, returns false.
      * @param searchString search string
      * @param position If position is undefined, 0 is assumed, so as to search all of the String.
      */
    contains(searchString: string, position?: number): boolean;

    /**
      * Returns true if the sequence of elements of searchString converted to a String is the
      * same as the corresponding elements of this object (converted to a String) starting at
      * endPosition – length(this). Otherwise returns false.
      */
    endsWith(searchString: string, endPosition?: number): boolean;

    /**
      * Returns the String value result of normalizing the string into the normalization form
      * named by form as specified in Unicode Standard Annex #15, Unicode Normalization Forms.
      * @param form Applicable values: "NFC", "NFD", "NFKC", or "NFKD", If not specified default
      * is "NFC"
      */
    normalize(form?: string): string;

    /**
      * Returns a String value that is made from count copies appended together. If count is 0,
      * T is the empty String is returned.
      * @param count number of copies to append
      */
    repeat(count: number): string;

    /**
      * Returns true if the sequence of elements of searchString converted to a String is the
      * same as the corresponding elements of this object (converted to a String) starting at
      * position. Otherwise returns false.
      */
    startsWith(searchString: string, position?: number): boolean;

    /**
      * Returns an <a> HTML anchor element and sets the name attribute to the text value
      * @param name
      */
    anchor(name: string): string;

    /** Returns a <big> HTML element */
    big(): string;

    /** Returns a <blink> HTML element */
    blink(): string;

    /** Returns a <b> HTML element */
    bold(): string;

    /** Returns a <tt> HTML element */
    fixed(): string

    /** Returns a <font> HTML element and sets the color attribute value */
    fontcolor(color: string): string

    /** Returns a <font> HTML element and sets the size attribute value */
    fontsize(size: number): string;

    /** Returns a <font> HTML element and sets the size attribute value */
    fontsize(size: string): string;

    /** Returns an <i> HTML element */
    italics(): string;

    /** Returns an <a> HTML element and sets the href attribute value */
    link(url: string): string;

    /** Returns a <small> HTML element */
    small(): string;

    /** Returns a <strike> HTML element */
    strike(): string;

    /** Returns a <sub> HTML element */
    sub(): string;

    /** Returns a <sup> HTML element */
    sup(): string;
}

interface StringConstructor {
    /**
      * Return the String value whose elements are, in order, the elements in the List elements.
      * If length is 0, the empty string is returned.
      */
    fromCodePoint(...codePoints: number[]): string;

    /**
      * String.raw is intended for use as a tag function of a Tagged Template String. When called
      * as such the first argument will be a well formed template call site object and the rest
      * parameter will contain the substitution values.
      * @param template A well-formed template string call site representation.
      * @param substitutions A set of substitution values.
      */
    raw(template: TemplateStringsArray, ...substitutions: any[]): string;
}

interface IteratorResult<T> {
    done: boolean;
    value?: T;
}

interface Iterator<T> {
    next(): IteratorResult<T>;
    return?(value?: any): IteratorResult<T>;
    throw?(e?: any): IteratorResult<T>;
}

interface Iterable<T> {
    [Symbol.iterator](): Iterator<T>;
}

interface IterableIterator<T> extends Iterator<T> {
    [Symbol.iterator](): IterableIterator<T>;
}

interface GeneratorFunction extends Function {

}

interface GeneratorFunctionConstructor {
    /**
      * Creates a new Generator function.
      * @param args A list of arguments the function accepts.
      */
    new (...args: string[]): GeneratorFunction;
    (...args: string[]): GeneratorFunction;
    prototype: GeneratorFunction;
}
declare var GeneratorFunction: GeneratorFunctionConstructor;

interface Generator<T> extends IterableIterator<T> {
    next(value?: any): IteratorResult<T>;
    throw(exception: any): IteratorResult<T>;
    return(value: T): IteratorResult<T>;
    [Symbol.iterator](): Generator<T>;
    [Symbol.toStringTag]: string;
}

interface Math {
    /**
      * Returns the number of leading zero bits in the 32-bit binary representation of a number.
      * @param x A numeric expression.
      */
    clz32(x: number): number;

    /**
      * Returns the result of 32-bit multiplication of two numbers.
      * @param x First number
      * @param y Second number
      */
    imul(x: number, y: number): number;

    /**
      * Returns the sign of the x, indicating whether x is positive, negative or zero.
      * @param x The numeric expression to test
      */
    sign(x: number): number;

    /**
      * Returns the base 10 logarithm of a number.
      * @param x A numeric expression.
      */
    log10(x: number): number;

    /**
      * Returns the base 2 logarithm of a number.
      * @param x A numeric expression.
      */
    log2(x: number): number;

    /**
      * Returns the natural logarithm of 1 + x.
      * @param x A numeric expression.
      */
    log1p(x: number): number;

    /**
      * Returns the result of (e^x - 1) of x (e raised to the power of x, where e is the base of
      * the natural logarithms).
      * @param x A numeric expression.
      */
    expm1(x: number): number;

    /**
      * Returns the hyperbolic cosine of a number.
      * @param x A numeric expression that contains an angle measured in radians.
      */
    cosh(x: number): number;

    /**
      * Returns the hyperbolic sine of a number.
      * @param x A numeric expression that contains an angle measured in radians.
      */
    sinh(x: number): number;

    /**
      * Returns the hyperbolic tangent of a number.
      * @param x A numeric expression that contains an angle measured in radians.
      */
    tanh(x: number): number;

    /**
      * Returns the inverse hyperbolic cosine of a number.
      * @param x A numeric expression that contains an angle measured in radians.
      */
    acosh(x: number): number;

    /**
      * Returns the inverse hyperbolic sine of a number.
      * @param x A numeric expression that contains an angle measured in radians.
      */
    asinh(x: number): number;

    /**
      * Returns the inverse hyperbolic tangent of a number.
      * @param x A numeric expression that contains an angle measured in radians.
      */
    atanh(x: number): number;

    /**
      * Returns the square root of the sum of squares of its arguments.
      * @param values Values to compute the square root for.
      *     If no arguments are passed, the result is +0.
      *     If there is only one argument, the result is the absolute value.
      *     If any argument is +Infinity or -Infinity, the result is +Infinity.
      *     If any argument is NaN, the result is NaN.
      *     If all arguments are either +0 or −0, the result is +0.
      */
    hypot(...values: number[]): number;

    /**
      * Returns the integral part of the a numeric expression, x, removing any fractional digits.
      * If x is already an integer, the result is x.
      * @param x A numeric expression.
      */
    trunc(x: number): number;

    /**
      * Returns the nearest single precision float representation of a number.
      * @param x A numeric expression.
      */
    fround(x: number): number;

    /**
      * Returns an implementation-dependent approximation to the cube root of number.
      * @param x A numeric expression.
      */
    cbrt(x: number): number;

    [Symbol.toStringTag]: string;
}

interface RegExp {
    [Symbol.isRegExp]: boolean;

    /**
      * Matches a string with a regular expression, and returns an array containing the results of
      * that search.
      * @param string A string to search within.
      */
    match(string: string): string[];

    /**
      * Replaces text in a string, using a regular expression.
      * @param searchValue A String object or string literal that represents the regular expression
      * @param replaceValue A String object or string literal containing the text to replace for every
      * successful match of rgExp in stringObj.
      */
    replace(string: string, replaceValue: string): string;

    search(string: string): number;

    /**
      * Returns an Array object into which substrings of the result of converting string to a String
      * have been stored. The substrings are determined by searching from left to right for matches
      * of the this value regular expression; these occurrences are not part of any substring in the
      * returned array, but serve to divide up the String value.
      *
      * If the regular expression that contains capturing parentheses, then each time separator is
      * matched the results (including any undefined results) of the capturing parentheses are spliced.
      * @param string string value to split
      * @param limit if not undefined, the output array is truncated so that it contains no more
      * than limit elements.
      */
    split(string: string, limit?: number): string[];

    /**
      * Returns a Boolean value indicating the state of the sticky flag (y) used with a regular
      * expression. Default is false. Read-only.
      */
    sticky: boolean;

    /**
      * Returns a Boolean value indicating the state of the Unicode flag (u) used with a regular
      * expression. Default is false. Read-only.
      */
    unicode: boolean;
}

interface Map<K, V> {
    clear(): void;
    delete(key: K): boolean;
    entries(): IterableIterator<[K, V]>;
    forEach(callbackfn: (value: V, index: K, map: Map<K, V>) => void, thisArg?: any): void;
    get(key: K): V;
    has(key: K): boolean;
    keys(): IterableIterator<K>;
    set(key: K, value?: V): Map<K, V>;
    values(): IterableIterator<V>;
    [Symbol.iterator](): IterableIterator<[K, V]>;
    [Symbol.toStringTag]: string;
}

interface MapConstructor {
    new <K, V>(): Map<K, V>;
    new <K, V>(iterable: Iterable<[K, V]>): Map<K, V>;
    prototype: Map<any, any>;
}
declare var Map: MapConstructor;

interface WeakMap<K, V> {
    clear(): void;
    delete(key: K): boolean;
    get(key: K): V;
    has(key: K): boolean;
    set(key: K, value?: V): WeakMap<K, V>;
    [Symbol.toStringTag]: string;
}

interface WeakMapConstructor {
    new <K, V>(): WeakMap<K, V>;
    new <K, V>(iterable: Iterable<[K, V]>): WeakMap<K, V>;
    prototype: WeakMap<any, any>;
}
declare var WeakMap: WeakMapConstructor;

interface Set<T> {
    add(value: T): Set<T>;
    clear(): void;
    delete(value: T): boolean;
    entries(): IterableIterator<[T, T]>;
    forEach(callbackfn: (value: T, index: T, set: Set<T>) => void, thisArg?: any): void;
    has(value: T): boolean;
    keys(): IterableIterator<T>;
    values(): IterableIterator<T>;
    [Symbol.iterator](): IterableIterator<T>;
    [Symbol.toStringTag]: string;
}

interface SetConstructor {
    new <T>(): Set<T>;
    new <T>(iterable: Iterable<T>): Set<T>;
    prototype: Set<any>;
}
declare var Set: SetConstructor;

interface WeakSet<T> {
    add(value: T): WeakSet<T>;
    clear(): void;
    delete(value: T): boolean;
    has(value: T): boolean;
    [Symbol.toStringTag]: string;
}

interface WeakSetConstructor {
    new <T>(): WeakSet<T>;
    new <T>(iterable: Iterable<T>): WeakSet<T>;
    prototype: WeakSet<any>;
}
declare var WeakSet: WeakSetConstructor;

interface JSON {
    [Symbol.toStringTag]: string;
}

/**
  * Represents a raw buffer of binary data, which is used to store data for the
  * different typed arrays. ArrayBuffers cannot be read from or written to directly,
  * but can be passed to a typed array or DataView Object to interpret the raw
  * buffer as needed.
  */
interface ArrayBuffer {
    /**
      * Read-only. The length of the ArrayBuffer (in bytes).
      */

    /**
      * Returns a section of an ArrayBuffer.
      */
    slice(begin: number, end?: number): ArrayBuffer;

    [Symbol.toStringTag]: string;
}

interface ArrayBufferConstructor {
    new (byteLength: number): ArrayBuffer;
    isView(arg: any): boolean;
}
declare var ArrayBuffer: ArrayBufferConstructor;

interface DataView {
    /*buffer: ArrayBuffer;
    byteLength: number;
    byteOffset: number;*/

    /**
      * Gets the Float32 value at the specified byte offset from the start of the view. There is
      * no alignment constraint; multi-byte values may be fetched from any offset.
      * @param byteOffset The place in the buffer at which the value should be retrieved.
      */
    getFloat32(byteOffset: number, littleEndian: boolean): number;

    /**
      * Gets the Float64 value at the specified byte offset from the start of the view. There is
      * no alignment constraint; multi-byte values may be fetched from any offset.
      * @param byteOffset The place in the buffer at which the value should be retrieved.
      */
    getFloat64(byteOffset: number, littleEndian: boolean): number;

    /**
      * Gets the Int8 value at the specified byte offset from the start of the view. There is
      * no alignment constraint; multi-byte values may be fetched from any offset.
      * @param byteOffset The place in the buffer at which the value should be retrieved.
      */
    getInt8(byteOffset: number): number;

    /**
      * Gets the Int16 value at the specified byte offset from the start of the view. There is
      * no alignment constraint; multi-byte values may be fetched from any offset.
      * @param byteOffset The place in the buffer at which the value should be retrieved.
      */
    getInt16(byteOffset: number, littleEndian: boolean): number;
    /**
      * Gets the Int32 value at the specified byte offset from the start of the view. There is
      * no alignment constraint; multi-byte values may be fetched from any offset.
      * @param byteOffset The place in the buffer at which the value should be retrieved.
      */
    getInt32(byteOffset: number, littleEndian: boolean): number;

    /**
      * Gets the Uint8 value at the specified byte offset from the start of the view. There is
      * no alignment constraint; multi-byte values may be fetched from any offset.
      * @param byteOffset The place in the buffer at which the value should be retrieved.
      */
    getUint8(byteOffset: number): number;

    /**
      * Gets the Uint16 value at the specified byte offset from the start of the view. There is
      * no alignment constraint; multi-byte values may be fetched from any offset.
      * @param byteOffset The place in the buffer at which the value should be retrieved.
      */
    getUint16(byteOffset: number, littleEndian: boolean): number;

    /**
      * Gets the Uint32 value at the specified byte offset from the start of the view. There is
      * no alignment constraint; multi-byte values may be fetched from any offset.
      * @param byteOffset The place in the buffer at which the value should be retrieved.
      */
    getUint32(byteOffset: number, littleEndian: boolean): number;

    /**
      * Stores an Float32 value at the specified byte offset from the start of the view.
      * @param byteOffset The place in the buffer at which the value should be set.
      * @param value The value to set.
      * @param littleEndian If false or undefined, a big-endian value should be written,
      * otherwise a little-endian value should be written.
      */
    setFloat32(byteOffset: number, value: number, littleEndian: boolean): void;

    /**
      * Stores an Float64 value at the specified byte offset from the start of the view.
      * @param byteOffset The place in the buffer at which the value should be set.
      * @param value The value to set.
      * @param littleEndian If false or undefined, a big-endian value should be written,
      * otherwise a little-endian value should be written.
      */
    setFloat64(byteOffset: number, value: number, littleEndian: boolean): void;

    /**
      * Stores an Int8 value at the specified byte offset from the start of the view.
      * @param byteOffset The place in the buffer at which the value should be set.
      * @param value The value to set.
      */
    setInt8(byteOffset: number, value: number): void;

    /**
      * Stores an Int16 value at the specified byte offset from the start of the view.
      * @param byteOffset The place in the buffer at which the value should be set.
      * @param value The value to set.
      * @param littleEndian If false or undefined, a big-endian value should be written,
      * otherwise a little-endian value should be written.
      */
    setInt16(byteOffset: number, value: number, littleEndian: boolean): void;

    /**
      * Stores an Int32 value at the specified byte offset from the start of the view.
      * @param byteOffset The place in the buffer at which the value should be set.
      * @param value The value to set.
      * @param littleEndian If false or undefined, a big-endian value should be written,
      * otherwise a little-endian value should be written.
      */
    setInt32(byteOffset: number, value: number, littleEndian: boolean): void;

    /**
      * Stores an Uint8 value at the specified byte offset from the start of the view.
      * @param byteOffset The place in the buffer at which the value should be set.
      * @param value The value to set.
      */
    setUint8(byteOffset: number, value: number): void;

    /**
      * Stores an Uint16 value at the specified byte offset from the start of the view.
      * @param byteOffset The place in the buffer at which the value should be set.
      * @param value The value to set.
      * @param littleEndian If false or undefined, a big-endian value should be written,
      * otherwise a little-endian value should be written.
      */
    setUint16(byteOffset: number, value: number, littleEndian: boolean): void;

    /**
      * Stores an Uint32 value at the specified byte offset from the start of the view.
      * @param byteOffset The place in the buffer at which the value should be set.
      * @param value The value to set.
      * @param littleEndian If false or undefined, a big-endian value should be written,
      * otherwise a little-endian value should be written.
      */
    setUint32(byteOffset: number, value: number, littleEndian: boolean): void;


}

interface DataViewConstructor {
    new (buffer: ArrayBuffer, byteOffset?: number, byteLength?: number): DataView;
}
declare var DataView: DataViewConstructor;

/**
  * A typed array of 8-bit integer values. The contents are initialized to 0. If the requested
  * number of bytes could not be allocated an exception is raised.
  */
interface Int8Array {
    /**
      * The size in bytes of each element in the array.
      */

    /**
      * The ArrayBuffer instance referenced by the array.
      */

    /**
      * The length in bytes of the array.
      */

    /**
      * The offset in bytes of the array.
      */

    /**
      * Returns the this object after copying a section of the array identified by start and end
      * to the same array starting at position target
      * @param target If target is negative, it is treated as length+target where length is the
      * length of the array.
      * @param start If start is negative, it is treated as length+start. If end is negative, it
      * is treated as length+end.
      * @param end If not specified, length of the this object is used as its default value.
      */
    copyWithin(target: number, start: number, end?: number): Int8Array;

    /**
      * Returns an array of key, value pairs for every entry in the array
      */
    entries(): IterableIterator<[number, number]>;

    /**
      * Determines whether all the members of an array satisfy the specified test.
      * @param callbackfn A function that accepts up to three arguments. The every method calls
      * the callbackfn function for each element in array1 until the callbackfn returns false,
      * or until the end of the array.
      * @param thisArg An object to which the this keyword can refer in the callbackfn function.
      * If thisArg is omitted, undefined is used as the this value.
      */
    every(callbackfn: (value: number, index: number, array: Int8Array) => boolean, thisArg?: any): boolean;

    /**
        * Returns the this object after filling the section identified by start and end with value
        * @param value value to fill array section with
        * @param start index to start filling the array at. If start is negative, it is treated as
        * length+start where length is the length of the array.
        * @param end index to stop filling the array at. If end is negative, it is treated as
        * length+end.
        */
    fill(value: number, start?: number, end?: number): Int8Array;

    /**
      * Returns the elements of an array that meet the condition specified in a callback function.
      * @param callbackfn A function that accepts up to three arguments. The filter method calls
      * the callbackfn function one time for each element in the array.
      * @param thisArg An object to which the this keyword can refer in the callbackfn function.
      * If thisArg is omitted, undefined is used as the this value.
      */
    filter(callbackfn: (value: number, index: number, array: Int8Array) => boolean, thisArg?: any): Int8Array;

    /**
      * Returns the value of the first element in the array where predicate is true, and undefined
      * otherwise.
      * @param predicate find calls predicate once for each element of the array, in ascending
      * order, until it finds one where predicate returns true. If such an element is found, find
      * immediately returns that element value. Otherwise, find returns undefined.
      * @param thisArg If provided, it will be used as the this value for each invocation of
      * predicate. If it is not provided, undefined is used instead.
      */
    find(predicate: (value: number, index: number, obj: Array<number>) => boolean, thisArg?: any): number;

    /**
      * Returns the index of the first element in the array where predicate is true, and undefined
      * otherwise.
      * @param predicate find calls predicate once for each element of the array, in ascending
      * order, until it finds one where predicate returns true. If such an element is found, find
      * immediately returns that element value. Otherwise, find returns undefined.
      * @param thisArg If provided, it will be used as the this value for each invocation of
      * predicate. If it is not provided, undefined is used instead.
      */
    findIndex(predicate: (value: number) => boolean, thisArg?: any): number;

    /**
      * Performs the specified action for each element in an array.
      * @param callbackfn  A function that accepts up to three arguments. forEach calls the
      * callbackfn function one time for each element in the array.
      * @param thisArg  An object to which the this keyword can refer in the callbackfn function.
      * If thisArg is omitted, undefined is used as the this value.
      */
    forEach(callbackfn: (value: number, index: number, array: Int8Array) => void, thisArg?: any): void;

    /**
      * Returns the index of the first occurrence of a value in an array.
      * @param searchElement The value to locate in the array.
      * @param fromIndex The array index at which to begin the search. If fromIndex is omitted, the
      *  search starts at index 0.
      */
    indexOf(searchElement: number, fromIndex?: number): number;

    /**
      * Adds all the elements of an array separated by the specified separator string.
      * @param separator A string used to separate one element of an array from the next in the
      * resulting String. If omitted, the array elements are separated with a comma.
      */
    join(separator?: string): string;

    /**
      * Returns an list of keys in the array
      */
    keys(): IterableIterator<number>;

    /**
      * Returns the index of the last occurrence of a value in an array.
      * @param searchElement The value to locate in the array.
      * @param fromIndex The array index at which to begin the search. If fromIndex is omitted, the
      * search starts at index 0.
      */
    lastIndexOf(searchElement: number, fromIndex?: number): number;

    /**
      * The length of the array.
      */

    /**
      * Calls a defined callback function on each element of an array, and returns an array that
      * contains the results.
      * @param callbackfn A function that accepts up to three arguments. The map method calls the
      * callbackfn function one time for each element in the array.
      * @param thisArg An object to which the this keyword can refer in the callbackfn function.
      * If thisArg is omitted, undefined is used as the this value.
      */
    map(callbackfn: (value: number, index: number, array: Int8Array) => number, thisArg?: any): Int8Array;

    /**
      * Calls the specified callback function for all the elements in an array. The return value of
      * the callback function is the accumulated result, and is provided as an argument in the next
      * call to the callback function.
      * @param callbackfn A function that accepts up to four arguments. The reduce method calls the
      * callbackfn function one time for each element in the array.
      * @param initialValue If initialValue is specified, it is used as the initial value to start
      * the accumulation. The first call to the callbackfn function provides this value as an argument
      * instead of an array value.
      */
    reduce(callbackfn: (previousValue: number, currentValue: number, currentIndex: number, array: Int8Array) => number, initialValue?: number): number;

    /**
      * Calls the specified callback function for all the elements in an array. The return value of
      * the callback function is the accumulated result, and is provided as an argument in the next
      * call to the callback function.
      * @param callbackfn A function that accepts up to four arguments. The reduce method calls the
      * callbackfn function one time for each element in the array.
      * @param initialValue If initialValue is specified, it is used as the initial value to start
      * the accumulation. The first call to the callbackfn function provides this value as an argument
      * instead of an array value.
      */
    reduce<U>(callbackfn: (previousValue: U, currentValue: number, currentIndex: number, array: Int8Array) => U, initialValue: U): U;

    /**
      * Calls the specified callback function for all the elements in an array, in descending order.
      * The return value of the callback function is the accumulated result, and is provided as an
      * argument in the next call to the callback function.
      * @param callbackfn A function that accepts up to four arguments. The reduceRight method calls
      * the callbackfn function one time for each element in the array.
      * @param initialValue If initialValue is specified, it is used as the initial value to start
      * the accumulation. The first call to the callbackfn function provides this value as an
      * argument instead of an array value.
      */
    reduceRight(callbackfn: (previousValue: number, currentValue: number, currentIndex: number, array: Int8Array) => number, initialValue?: number): number;

    /**
      * Calls the specified callback function for all the elements in an array, in descending order.
      * The return value of the callback function is the accumulated result, and is provided as an
      * argument in the next call to the callback function.
      * @param callbackfn A function that accepts up to four arguments. The reduceRight method calls
      * the callbackfn function one time for each element in the array.
      * @param initialValue If initialValue is specified, it is used as the initial value to start
      * the accumulation. The first call to the callbackfn function provides this value as an argument
      * instead of an array value.
      */
    reduceRight<U>(callbackfn: (previousValue: U, currentValue: number, currentIndex: number, array: Int8Array) => U, initialValue: U): U;

    /**
      * Reverses the elements in an Array.
      */
    reverse(): Int8Array;

    /**
      * Sets a value or an array of values.
      * @param index The index of the location to set.
      * @param value The value to set.
      */
    set(index: number, value: number): void;

    /**
      * Sets a value or an array of values.
      * @param array A typed or untyped array of values to set.
      * @param offset The index in the current array at which the values are to be written.
      */
    set(array: Int8Array, offset?: number): void;

    /**
      * Returns a section of an array.
      * @param start The beginning of the specified portion of the array.
      * @param end The end of the specified portion of the array.
      */
    slice(start?: number, end?: number): Int8Array;

    /**
      * Determines whether the specified callback function returns true for any element of an array.
      * @param callbackfn A function that accepts up to three arguments. The some method calls the
      * callbackfn function for each element in array1 until the callbackfn returns true, or until
      * the end of the array.
      * @param thisArg An object to which the this keyword can refer in the callbackfn function.
      * If thisArg is omitted, undefined is used as the this value.
      */
    some(callbackfn: (value: number, index: number, array: Int8Array) => boolean, thisArg?: any): boolean;

    /**
      * Sorts an array.
      * @param compareFn The name of the function used to determine the order of the elements. If
      * omitted, the elements are sorted in ascending, ASCII character order.
      */
    sort(compareFn?: (a: number, b: number) => number): Int8Array;

    /**
      * Gets a new Int8Array view of the ArrayBuffer store for this array, referencing the elements
      * at begin, inclusive, up to end, exclusive.
      * @param begin The index of the beginning of the array.
      * @param end The index of the end of the array.
      */
    subarray(begin: number, end?: number): Int8Array;

    /**
      * Converts a number to a string by using the current locale.
      */
    toLocaleString(): string;

    /**
      * Returns a string representation of an array.
      */
    toString(): string;

    /**
      * Returns an list of values in the array
      */
    values(): IterableIterator<number>;

    //[index: number]: number;
    [Symbol.iterator](): IterableIterator<number>;
}

interface Int8ArrayConstructor {
    new (length: number): Int8Array;
    new (array: Int8Array): Int8Array;
    new (array: number[]): Int8Array;
    new (buffer: ArrayBuffer, byteOffset?: number, length?: number): Int8Array;

    /**
      * The size in bytes of each element in the array.
      */

    /**
      * Returns a new array from a set of elements.
      * @param items A set of elements to include in the new array object.
      */
    of(...items: number[]): Int8Array;

    /**
      * Creates an array from an array-like or iterable object.
      * @param arrayLike An array-like or iterable object to convert to an array.
      * @param mapfn A mapping function to call on every element of the array.
      * @param thisArg Value of 'this' used to invoke the mapfn.
      */
    from(arrayLike: ArrayLike<number> | Iterable<number>, mapfn?: (v: number, k: number) => number, thisArg?: any): Int8Array;
}
declare var Int8Array: Int8ArrayConstructor;

/**
  * A typed array of 8-bit unsigned integer values. The contents are initialized to 0. If the
  * requested number of bytes could not be allocated an exception is raised.
  */
interface Uint8Array {
    /**
      * The size in bytes of each element in the array.
      */

    /**
      * The ArrayBuffer instance referenced by the array.
      */

    /**
      * The length in bytes of the array.
      */

    /**
      * The offset in bytes of the array.
      */

    /**
      * Returns the this object after copying a section of the array identified by start and end
      * to the same array starting at position target
      * @param target If target is negative, it is treated as length+target where length is the
      * length of the array.
      * @param start If start is negative, it is treated as length+start. If end is negative, it
      * is treated as length+end.
      * @param end If not specified, length of the this object is used as its default value.
      */
    copyWithin(target: number, start: number, end?: number): Uint8Array;

    /**
      * Returns an array of key, value pairs for every entry in the array
      */
    entries(): IterableIterator<[number, number]>;

    /**
      * Determines whether all the members of an array satisfy the specified test.
      * @param callbackfn A function that accepts up to three arguments. The every method calls
      * the callbackfn function for each element in array1 until the callbackfn returns false,
      * or until the end of the array.
      * @param thisArg An object to which the this keyword can refer in the callbackfn function.
      * If thisArg is omitted, undefined is used as the this value.
      */
    every(callbackfn: (value: number, index: number, array: Uint8Array) => boolean, thisArg?: any): boolean;

    /**
        * Returns the this object after filling the section identified by start and end with value
        * @param value value to fill array section with
        * @param start index to start filling the array at. If start is negative, it is treated as
        * length+start where length is the length of the array.
        * @param end index to stop filling the array at. If end is negative, it is treated as
        * length+end.
        */
    fill(value: number, start?: number, end?: number): Uint8Array;

    /**
      * Returns the elements of an array that meet the condition specified in a callback function.
      * @param callbackfn A function that accepts up to three arguments. The filter method calls
      * the callbackfn function one time for each element in the array.
      * @param thisArg An object to which the this keyword can refer in the callbackfn function.
      * If thisArg is omitted, undefined is used as the this value.
      */
    filter(callbackfn: (value: number, index: number, array: Uint8Array) => boolean, thisArg?: any): Uint8Array;

    /**
      * Returns the value of the first element in the array where predicate is true, and undefined
      * otherwise.
      * @param predicate find calls predicate once for each element of the array, in ascending
      * order, until it finds one where predicate returns true. If such an element is found, find
      * immediately returns that element value. Otherwise, find returns undefined.
      * @param thisArg If provided, it will be used as the this value for each invocation of
      * predicate. If it is not provided, undefined is used instead.
      */
    find(predicate: (value: number, index: number, obj: Array<number>) => boolean, thisArg?: any): number;

    /**
      * Returns the index of the first element in the array where predicate is true, and undefined
      * otherwise.
      * @param predicate find calls predicate once for each element of the array, in ascending
      * order, until it finds one where predicate returns true. If such an element is found, find
      * immediately returns that element value. Otherwise, find returns undefined.
      * @param thisArg If provided, it will be used as the this value for each invocation of
      * predicate. If it is not provided, undefined is used instead.
      */
    findIndex(predicate: (value: number) => boolean, thisArg?: any): number;

    /**
      * Performs the specified action for each element in an array.
      * @param callbackfn  A function that accepts up to three arguments. forEach calls the
      * callbackfn function one time for each element in the array.
      * @param thisArg  An object to which the this keyword can refer in the callbackfn function.
      * If thisArg is omitted, undefined is used as the this value.
      */
    forEach(callbackfn: (value: number, index: number, array: Uint8Array) => void, thisArg?: any): void;

    /**
      * Returns the index of the first occurrence of a value in an array.
      * @param searchElement The value to locate in the array.
      * @param fromIndex The array index at which to begin the search. If fromIndex is omitted, the
      *  search starts at index 0.
      */
    indexOf(searchElement: number, fromIndex?: number): number;

    /**
      * Adds all the elements of an array separated by the specified separator string.
      * @param separator A string used to separate one element of an array from the next in the
      * resulting String. If omitted, the array elements are separated with a comma.
      */
    join(separator?: string): string;

    /**
      * Returns an list of keys in the array
      */
    keys(): IterableIterator<number>;

    /**
      * Returns the index of the last occurrence of a value in an array.
      * @param searchElement The value to locate in the array.
      * @param fromIndex The array index at which to begin the search. If fromIndex is omitted, the
      * search starts at index 0.
      */
    lastIndexOf(searchElement: number, fromIndex?: number): number;

    /**
      * The length of the array.
      */

    /**
      * Calls a defined callback function on each element of an array, and returns an array that
      * contains the results.
      * @param callbackfn A function that accepts up to three arguments. The map method calls the
      * callbackfn function one time for each element in the array.
      * @param thisArg An object to which the this keyword can refer in the callbackfn function.
      * If thisArg is omitted, undefined is used as the this value.
      */
    map(callbackfn: (value: number, index: number, array: Uint8Array) => number, thisArg?: any): Uint8Array;

    /**
      * Calls the specified callback function for all the elements in an array. The return value of
      * the callback function is the accumulated result, and is provided as an argument in the next
      * call to the callback function.
      * @param callbackfn A function that accepts up to four arguments. The reduce method calls the
      * callbackfn function one time for each element in the array.
      * @param initialValue If initialValue is specified, it is used as the initial value to start
      * the accumulation. The first call to the callbackfn function provides this value as an argument
      * instead of an array value.
      */
    reduce(callbackfn: (previousValue: number, currentValue: number, currentIndex: number, array: Uint8Array) => number, initialValue?: number): number;

    /**
      * Calls the specified callback function for all the elements in an array. The return value of
      * the callback function is the accumulated result, and is provided as an argument in the next
      * call to the callback function.
      * @param callbackfn A function that accepts up to four arguments. The reduce method calls the
      * callbackfn function one time for each element in the array.
      * @param initialValue If initialValue is specified, it is used as the initial value to start
      * the accumulation. The first call to the callbackfn function provides this value as an argument
      * instead of an array value.
      */
    reduce<U>(callbackfn: (previousValue: U, currentValue: number, currentIndex: number, array: Uint8Array) => U, initialValue: U): U;

    /**
      * Calls the specified callback function for all the elements in an array, in descending order.
      * The return value of the callback function is the accumulated result, and is provided as an
      * argument in the next call to the callback function.
      * @param callbackfn A function that accepts up to four arguments. The reduceRight method calls
      * the callbackfn function one time for each element in the array.
      * @param initialValue If initialValue is specified, it is used as the initial value to start
      * the accumulation. The first call to the callbackfn function provides this value as an
      * argument instead of an array value.
      */
    reduceRight(callbackfn: (previousValue: number, currentValue: number, currentIndex: number, array: Uint8Array) => number, initialValue?: number): number;

    /**
      * Calls the specified callback function for all the elements in an array, in descending order.
      * The return value of the callback function is the accumulated result, and is provided as an
      * argument in the next call to the callback function.
      * @param callbackfn A function that accepts up to four arguments. The reduceRight method calls
      * the callbackfn function one time for each element in the array.
      * @param initialValue If initialValue is specified, it is used as the initial value to start
      * the accumulation. The first call to the callbackfn function provides this value as an argument
      * instead of an array value.
      */
    reduceRight<U>(callbackfn: (previousValue: U, currentValue: number, currentIndex: number, array: Uint8Array) => U, initialValue: U): U;

    /**
      * Reverses the elements in an Array.
      */
    reverse(): Uint8Array;

    /**
      * Sets a value or an array of values.
      * @param index The index of the location to set.
      * @param value The value to set.
      */
    set(index: number, value: number): void;

    /**
      * Sets a value or an array of values.
      * @param array A typed or untyped array of values to set.
      * @param offset The index in the current array at which the values are to be written.
      */
    set(array: Uint8Array, offset?: number): void;

    /**
      * Returns a section of an array.
      * @param start The beginning of the specified portion of the array.
      * @param end The end of the specified portion of the array.
      */
    slice(start?: number, end?: number): Uint8Array;

    /**
      * Determines whether the specified callback function returns true for any element of an array.
      * @param callbackfn A function that accepts up to three arguments. The some method calls the
      * callbackfn function for each element in array1 until the callbackfn returns true, or until
      * the end of the array.
      * @param thisArg An object to which the this keyword can refer in the callbackfn function.
      * If thisArg is omitted, undefined is used as the this value.
      */
    some(callbackfn: (value: number, index: number, array: Uint8Array) => boolean, thisArg?: any): boolean;

    /**
      * Sorts an array.
      * @param compareFn The name of the function used to determine the order of the elements. If
      * omitted, the elements are sorted in ascending, ASCII character order.
      */
    sort(compareFn?: (a: number, b: number) => number): Uint8Array;

    /**
      * Gets a new Uint8Array view of the ArrayBuffer store for this array, referencing the elements
      * at begin, inclusive, up to end, exclusive.
      * @param begin The index of the beginning of the array.
      * @param end The index of the end of the array.
      */
    subarray(begin: number, end?: number): Uint8Array;

    /**
      * Converts a number to a string by using the current locale.
      */
    toLocaleString(): string;

    /**
      * Returns a string representation of an array.
      */
    toString(): string;

    /**
      * Returns an list of values in the array
      */
    values(): IterableIterator<number>;

    //[index: number]: number;
    [Symbol.iterator](): IterableIterator<number>;
}

interface Uint8ArrayConstructor {
    new (length: number): Uint8Array;
    new (array: Uint8Array): Uint8Array;
    new (array: number[]): Uint8Array;
    new (buffer: ArrayBuffer, byteOffset?: number, length?: number): Uint8Array;

    /**
      * The size in bytes of each element in the array.
      */

    /**
      * Returns a new array from a set of elements.
      * @param items A set of elements to include in the new array object.
      */
    of(...items: number[]): Uint8Array;

    /**
      * Creates an array from an array-like or iterable object.
      * @param arrayLike An array-like or iterable object to convert to an array.
      * @param mapfn A mapping function to call on every element of the array.
      * @param thisArg Value of 'this' used to invoke the mapfn.
      */
    from(arrayLike: ArrayLike<number> | Iterable<number>, mapfn?: (v: number, k: number) => number, thisArg?: any): Uint8Array;
}
declare var Uint8Array: Uint8ArrayConstructor;

/**
  * A typed array of 8-bit unsigned integer (clamped) values. The contents are initialized to 0.
  * If the requested number of bytes could not be allocated an exception is raised.
  */
interface Uint8ClampedArray {


    /**
      * Returns the this object after copying a section of the array identified by start and end
      * to the same array starting at position target
      * @param target If target is negative, it is treated as length+target where length is the
      * length of the array.
      * @param start If start is negative, it is treated as length+start. If end is negative, it
      * is treated as length+end.
      * @param end If not specified, length of the this object is used as its default value.
      */
    copyWithin(target: number, start: number, end?: number): Uint8ClampedArray;

    /**
      * Returns an array of key, value pairs for every entry in the array
      */
    entries(): IterableIterator<[number, number]>;

    /**
      * Determines whether all the members of an array satisfy the specified test.
      * @param callbackfn A function that accepts up to three arguments. The every method calls
      * the callbackfn function for each element in array1 until the callbackfn returns false,
      * or until the end of the array.
      * @param thisArg An object to which the this keyword can refer in the callbackfn function.
      * If thisArg is omitted, undefined is used as the this value.
      */
    every(callbackfn: (value: number, index: number, array: Uint8ClampedArray) => boolean, thisArg?: any): boolean;

    /**
        * Returns the this object after filling the section identified by start and end with value
        * @param value value to fill array section with
        * @param start index to start filling the array at. If start is negative, it is treated as
        * length+start where length is the length of the array.
        * @param end index to stop filling the array at. If end is negative, it is treated as
        * length+end.
        */
    fill(value: number, start?: number, end?: number): Uint8ClampedArray;

    /**
      * Returns the elements of an array that meet the condition specified in a callback function.
      * @param callbackfn A function that accepts up to three arguments. The filter method calls
      * the callbackfn function one time for each element in the array.
      * @param thisArg An object to which the this keyword can refer in the callbackfn function.
      * If thisArg is omitted, undefined is used as the this value.
      */
    filter(callbackfn: (value: number, index: number, array: Uint8ClampedArray) => boolean, thisArg?: any): Uint8ClampedArray;

    /**
      * Returns the value of the first element in the array where predicate is true, and undefined
      * otherwise.
      * @param predicate find calls predicate once for each element of the array, in ascending
      * order, until it finds one where predicate returns true. If such an element is found, find
      * immediately returns that element value. Otherwise, find returns undefined.
      * @param thisArg If provided, it will be used as the this value for each invocation of
      * predicate. If it is not provided, undefined is used instead.
      */
    find(predicate: (value: number, index: number, obj: Array<number>) => boolean, thisArg?: any): number;

    /**
      * Returns the index of the first element in the array where predicate is true, and undefined
      * otherwise.
      * @param predicate find calls predicate once for each element of the array, in ascending
      * order, until it finds one where predicate returns true. If such an element is found, find
      * immediately returns that element value. Otherwise, find returns undefined.
      * @param thisArg If provided, it will be used as the this value for each invocation of
      * predicate. If it is not provided, undefined is used instead.
      */
    findIndex(predicate: (value: number) => boolean, thisArg?: any): number;

    /**
      * Performs the specified action for each element in an array.
      * @param callbackfn  A function that accepts up to three arguments. forEach calls the
      * callbackfn function one time for each element in the array.
      * @param thisArg  An object to which the this keyword can refer in the callbackfn function.
      * If thisArg is omitted, undefined is used as the this value.
      */
    forEach(callbackfn: (value: number, index: number, array: Uint8ClampedArray) => void, thisArg?: any): void;

    /**
      * Returns the index of the first occurrence of a value in an array.
      * @param searchElement The value to locate in the array.
      * @param fromIndex The array index at which to begin the search. If fromIndex is omitted, the
      *  search starts at index 0.
      */
    indexOf(searchElement: number, fromIndex?: number): number;

    /**
      * Adds all the elements of an array separated by the specified separator string.
      * @param separator A string used to separate one element of an array from the next in the
      * resulting String. If omitted, the array elements are separated with a comma.
      */
    join(separator?: string): string;

    /**
      * Returns an list of keys in the array
      */
    keys(): IterableIterator<number>;

    /**
      * Returns the index of the last occurrence of a value in an array.
      * @param searchElement The value to locate in the array.
      * @param fromIndex The array index at which to begin the search. If fromIndex is omitted, the
      * search starts at index 0.
      */
    lastIndexOf(searchElement: number, fromIndex?: number): number;


    /**
      * Calls a defined callback function on each element of an array, and returns an array that
      * contains the results.
      * @param callbackfn A function that accepts up to three arguments. The map method calls the
      * callbackfn function one time for each element in the array.
      * @param thisArg An object to which the this keyword can refer in the callbackfn function.
      * If thisArg is omitted, undefined is used as the this value.
      */
    map(callbackfn: (value: number, index: number, array: Uint8ClampedArray) => number, thisArg?: any): Uint8ClampedArray;

    /**
      * Calls the specified callback function for all the elements in an array. The return value of
      * the callback function is the accumulated result, and is provided as an argument in the next
      * call to the callback function.
      * @param callbackfn A function that accepts up to four arguments. The reduce method calls the
      * callbackfn function one time for each element in the array.
      * @param initialValue If initialValue is specified, it is used as the initial value to start
      * the accumulation. The first call to the callbackfn function provides this value as an argument
      * instead of an array value.
      */
    reduce(callbackfn: (previousValue: number, currentValue: number, currentIndex: number, array: Uint8ClampedArray) => number, initialValue?: number): number;

    /**
      * Calls the specified callback function for all the elements in an array. The return value of
      * the callback function is the accumulated result, and is provided as an argument in the next
      * call to the callback function.
      * @param callbackfn A function that accepts up to four arguments. The reduce method calls the
      * callbackfn function one time for each element in the array.
      * @param initialValue If initialValue is specified, it is used as the initial value to start
      * the accumulation. The first call to the callbackfn function provides this value as an argument
      * instead of an array value.
      */
    reduce<U>(callbackfn: (previousValue: U, currentValue: number, currentIndex: number, array: Uint8ClampedArray) => U, initialValue: U): U;

    /**
      * Calls the specified callback function for all the elements in an array, in descending order.
      * The return value of the callback function is the accumulated result, and is provided as an
      * argument in the next call to the callback function.
      * @param callbackfn A function that accepts up to four arguments. The reduceRight method calls
      * the callbackfn function one time for each element in the array.
      * @param initialValue If initialValue is specified, it is used as the initial value to start
      * the accumulation. The first call to the callbackfn function provides this value as an
      * argument instead of an array value.
      */
    reduceRight(callbackfn: (previousValue: number, currentValue: number, currentIndex: number, array: Uint8ClampedArray) => number, initialValue?: number): number;

    /**
      * Calls the specified callback function for all the elements in an array, in descending order.
      * The return value of the callback function is the accumulated result, and is provided as an
      * argument in the next call to the callback function.
      * @param callbackfn A function that accepts up to four arguments. The reduceRight method calls
      * the callbackfn function one time for each element in the array.
      * @param initialValue If initialValue is specified, it is used as the initial value to start
      * the accumulation. The first call to the callbackfn function provides this value as an argument
      * instead of an array value.
      */
    reduceRight<U>(callbackfn: (previousValue: U, currentValue: number, currentIndex: number, array: Uint8ClampedArray) => U, initialValue: U): U;

    /**
      * Reverses the elements in an Array.
      */
    reverse(): Uint8ClampedArray;

    /**
      * Sets a value or an array of values.
      * @param index The index of the location to set.
      * @param value The value to set.
      */
    set(index: number, value: number): void;

    /**
      * Sets a value or an array of values.
      * @param array A typed or untyped array of values to set.
      * @param offset The index in the current array at which the values are to be written.
      */
    set(array: Uint8ClampedArray, offset?: number): void;

    /**
      * Returns a section of an array.
      * @param start The beginning of the specified portion of the array.
      * @param end The end of the specified portion of the array.
      */
    slice(start?: number, end?: number): Uint8ClampedArray;

    /**
      * Determines whether the specified callback function returns true for any element of an array.
      * @param callbackfn A function that accepts up to three arguments. The some method calls the
      * callbackfn function for each element in array1 until the callbackfn returns true, or until
      * the end of the array.
      * @param thisArg An object to which the this keyword can refer in the callbackfn function.
      * If thisArg is omitted, undefined is used as the this value.
      */
    some(callbackfn: (value: number, index: number, array: Uint8ClampedArray) => boolean, thisArg?: any): boolean;

    /**
      * Sorts an array.
      * @param compareFn The name of the function used to determine the order of the elements. If
      * omitted, the elements are sorted in ascending, ASCII character order.
      */
    sort(compareFn?: (a: number, b: number) => number): Uint8ClampedArray;

    /**
      * Gets a new Uint8ClampedArray view of the ArrayBuffer store for this array, referencing the elements
      * at begin, inclusive, up to end, exclusive.
      * @param begin The index of the beginning of the array.
      * @param end The index of the end of the array.
      */
    subarray(begin: number, end?: number): Uint8ClampedArray;

    /**
      * Converts a number to a string by using the current locale.
      */
    toLocaleString(): string;

    /**
      * Returns a string representation of an array.
      */
    toString(): string;

    /**
      * Returns an list of values in the array
      */
    values(): IterableIterator<number>;


    [Symbol.iterator](): IterableIterator<number>;
}

interface Uint8ClampedArrayConstructor {

    new (length: number): Uint8ClampedArray;
    new (array: Uint8ClampedArray): Uint8ClampedArray;
    new (array: number[]): Uint8ClampedArray;
    new (buffer: ArrayBuffer, byteOffset?: number, length?: number): Uint8ClampedArray;



    /**
      * Returns a new array from a set of elements.
      * @param items A set of elements to include in the new array object.
      */
    of(...items: number[]): Uint8ClampedArray;

    /**
      * Creates an array from an array-like or iterable object.
      * @param arrayLike An array-like or iterable object to convert to an array.
      * @param mapfn A mapping function to call on every element of the array.
      * @param thisArg Value of 'this' used to invoke the mapfn.
      */
    from(arrayLike: ArrayLike<number> | Iterable<number>, mapfn?: (v: number, k: number) => number, thisArg?: any): Uint8ClampedArray;
}
declare var Uint8ClampedArray: Uint8ClampedArrayConstructor;

/**
  * A typed array of 16-bit signed integer values. The contents are initialized to 0. If the
  * requested number of bytes could not be allocated an exception is raised.
  */
interface Int16Array {
    /**
      * The size in bytes of each element in the array.
      */

    /**
      * The ArrayBuffer instance referenced by the array.
      */

    /**
      * The length in bytes of the array.
      */

    /**
      * The offset in bytes of the array.
      */

    /**
      * Returns the this object after copying a section of the array identified by start and end
      * to the same array starting at position target
      * @param target If target is negative, it is treated as length+target where length is the
      * length of the array.
      * @param start If start is negative, it is treated as length+start. If end is negative, it
      * is treated as length+end.
      * @param end If not specified, length of the this object is used as its default value.
      */
    copyWithin(target: number, start: number, end?: number): Int16Array;

    /**
      * Returns an array of key, value pairs for every entry in the array
      */
    entries(): IterableIterator<[number, number]>;

    /**
      * Determines whether all the members of an array satisfy the specified test.
      * @param callbackfn A function that accepts up to three arguments. The every method calls
      * the callbackfn function for each element in array1 until the callbackfn returns false,
      * or until the end of the array.
      * @param thisArg An object to which the this keyword can refer in the callbackfn function.
      * If thisArg is omitted, undefined is used as the this value.
      */
    every(callbackfn: (value: number, index: number, array: Int16Array) => boolean, thisArg?: any): boolean;

    /**
        * Returns the this object after filling the section identified by start and end with value
        * @param value value to fill array section with
        * @param start index to start filling the array at. If start is negative, it is treated as
        * length+start where length is the length of the array.
        * @param end index to stop filling the array at. If end is negative, it is treated as
        * length+end.
        */
    fill(value: number, start?: number, end?: number): Int16Array;

    /**
      * Returns the elements of an array that meet the condition specified in a callback function.
      * @param callbackfn A function that accepts up to three arguments. The filter method calls
      * the callbackfn function one time for each element in the array.
      * @param thisArg An object to which the this keyword can refer in the callbackfn function.
      * If thisArg is omitted, undefined is used as the this value.
      */
    filter(callbackfn: (value: number, index: number, array: Int16Array) => boolean, thisArg?: any): Int16Array;

    /**
      * Returns the value of the first element in the array where predicate is true, and undefined
      * otherwise.
      * @param predicate find calls predicate once for each element of the array, in ascending
      * order, until it finds one where predicate returns true. If such an element is found, find
      * immediately returns that element value. Otherwise, find returns undefined.
      * @param thisArg If provided, it will be used as the this value for each invocation of
      * predicate. If it is not provided, undefined is used instead.
      */
    find(predicate: (value: number, index: number, obj: Array<number>) => boolean, thisArg?: any): number;

    /**
      * Returns the index of the first element in the array where predicate is true, and undefined
      * otherwise.
      * @param predicate find calls predicate once for each element of the array, in ascending
      * order, until it finds one where predicate returns true. If such an element is found, find
      * immediately returns that element value. Otherwise, find returns undefined.
      * @param thisArg If provided, it will be used as the this value for each invocation of
      * predicate. If it is not provided, undefined is used instead.
      */
    findIndex(predicate: (value: number) => boolean, thisArg?: any): number;

    /**
      * Performs the specified action for each element in an array.
      * @param callbackfn  A function that accepts up to three arguments. forEach calls the
      * callbackfn function one time for each element in the array.
      * @param thisArg  An object to which the this keyword can refer in the callbackfn function.
      * If thisArg is omitted, undefined is used as the this value.
      */
    forEach(callbackfn: (value: number, index: number, array: Int16Array) => void, thisArg?: any): void;

    /**
      * Returns the index of the first occurrence of a value in an array.
      * @param searchElement The value to locate in the array.
      * @param fromIndex The array index at which to begin the search. If fromIndex is omitted, the
      *  search starts at index 0.
      */
    indexOf(searchElement: number, fromIndex?: number): number;

    /**
      * Adds all the elements of an array separated by the specified separator string.
      * @param separator A string used to separate one element of an array from the next in the
      * resulting String. If omitted, the array elements are separated with a comma.
      */
    join(separator?: string): string;

    /**
      * Returns an list of keys in the array
      */
    keys(): IterableIterator<number>;

    /**
      * Returns the index of the last occurrence of a value in an array.
      * @param searchElement The value to locate in the array.
      * @param fromIndex The array index at which to begin the search. If fromIndex is omitted, the
      * search starts at index 0.
      */
    lastIndexOf(searchElement: number, fromIndex?: number): number;

    /**
      * The length of the array.
      */

    /**
      * Calls a defined callback function on each element of an array, and returns an array that
      * contains the results.
      * @param callbackfn A function that accepts up to three arguments. The map method calls the
      * callbackfn function one time for each element in the array.
      * @param thisArg An object to which the this keyword can refer in the callbackfn function.
      * If thisArg is omitted, undefined is used as the this value.
      */
    map(callbackfn: (value: number, index: number, array: Int16Array) => number, thisArg?: any): Int16Array;

    /**
      * Calls the specified callback function for all the elements in an array. The return value of
      * the callback function is the accumulated result, and is provided as an argument in the next
      * call to the callback function.
      * @param callbackfn A function that accepts up to four arguments. The reduce method calls the
      * callbackfn function one time for each element in the array.
      * @param initialValue If initialValue is specified, it is used as the initial value to start
      * the accumulation. The first call to the callbackfn function provides this value as an argument
      * instead of an array value.
      */
    reduce(callbackfn: (previousValue: number, currentValue: number, currentIndex: number, array: Int16Array) => number, initialValue?: number): number;

    /**
      * Calls the specified callback function for all the elements in an array. The return value of
      * the callback function is the accumulated result, and is provided as an argument in the next
      * call to the callback function.
      * @param callbackfn A function that accepts up to four arguments. The reduce method calls the
      * callbackfn function one time for each element in the array.
      * @param initialValue If initialValue is specified, it is used as the initial value to start
      * the accumulation. The first call to the callbackfn function provides this value as an argument
      * instead of an array value.
      */
    reduce<U>(callbackfn: (previousValue: U, currentValue: number, currentIndex: number, array: Int16Array) => U, initialValue: U): U;

    /**
      * Calls the specified callback function for all the elements in an array, in descending order.
      * The return value of the callback function is the accumulated result, and is provided as an
      * argument in the next call to the callback function.
      * @param callbackfn A function that accepts up to four arguments. The reduceRight method calls
      * the callbackfn function one time for each element in the array.
      * @param initialValue If initialValue is specified, it is used as the initial value to start
      * the accumulation. The first call to the callbackfn function provides this value as an
      * argument instead of an array value.
      */
    reduceRight(callbackfn: (previousValue: number, currentValue: number, currentIndex: number, array: Int16Array) => number, initialValue?: number): number;

    /**
      * Calls the specified callback function for all the elements in an array, in descending order.
      * The return value of the callback function is the accumulated result, and is provided as an
      * argument in the next call to the callback function.
      * @param callbackfn A function that accepts up to four arguments. The reduceRight method calls
      * the callbackfn function one time for each element in the array.
      * @param initialValue If initialValue is specified, it is used as the initial value to start
      * the accumulation. The first call to the callbackfn function provides this value as an argument
      * instead of an array value.
      */
    reduceRight<U>(callbackfn: (previousValue: U, currentValue: number, currentIndex: number, array: Int16Array) => U, initialValue: U): U;

    /**
      * Reverses the elements in an Array.
      */
    reverse(): Int16Array;

    /**
      * Sets a value or an array of values.
      * @param index The index of the location to set.
      * @param value The value to set.
      */
    set(index: number, value: number): void;

    /**
      * Sets a value or an array of values.
      * @param array A typed or untyped array of values to set.
      * @param offset The index in the current array at which the values are to be written.
      */
    set(array: Int16Array, offset?: number): void;

    /**
      * Returns a section of an array.
      * @param start The beginning of the specified portion of the array.
      * @param end The end of the specified portion of the array.
      */
    slice(start?: number, end?: number): Int16Array;

    /**
      * Determines whether the specified callback function returns true for any element of an array.
      * @param callbackfn A function that accepts up to three arguments. The some method calls the
      * callbackfn function for each element in array1 until the callbackfn returns true, or until
      * the end of the array.
      * @param thisArg An object to which the this keyword can refer in the callbackfn function.
      * If thisArg is omitted, undefined is used as the this value.
      */
    some(callbackfn: (value: number, index: number, array: Int16Array) => boolean, thisArg?: any): boolean;

    /**
      * Sorts an array.
      * @param compareFn The name of the function used to determine the order of the elements. If
      * omitted, the elements are sorted in ascending, ASCII character order.
      */
    sort(compareFn?: (a: number, b: number) => number): Int16Array;

    /**
      * Gets a new Int16Array view of the ArrayBuffer store for this array, referencing the elements
      * at begin, inclusive, up to end, exclusive.
      * @param begin The index of the beginning of the array.
      * @param end The index of the end of the array.
      */
    subarray(begin: number, end?: number): Int16Array;

    /**
      * Converts a number to a string by using the current locale.
      */
    toLocaleString(): string;

    /**
      * Returns a string representation of an array.
      */
    toString(): string;

    /**
      * Returns an list of values in the array
      */
    values(): IterableIterator<number>;

    //[index: number]: number;
    [Symbol.iterator](): IterableIterator<number>;
}

interface Int16ArrayConstructor {
    new (length: number): Int16Array;
    new (array: Int16Array): Int16Array;
    new (array: number[]): Int16Array;
    new (buffer: ArrayBuffer, byteOffset?: number, length?: number): Int16Array;

    /**
      * The size in bytes of each element in the array.
      */

    /**
      * Returns a new array from a set of elements.
      * @param items A set of elements to include in the new array object.
      */
    of(...items: number[]): Int16Array;

    /**
      * Creates an array from an array-like or iterable object.
      * @param arrayLike An array-like or iterable object to convert to an array.
      * @param mapfn A mapping function to call on every element of the array.
      * @param thisArg Value of 'this' used to invoke the mapfn.
      */
    from(arrayLike: ArrayLike<number> | Iterable<number>, mapfn?: (v: number, k: number) => number, thisArg?: any): Int16Array;
}
declare var Int16Array: Int16ArrayConstructor;

/**
  * A typed array of 16-bit unsigned integer values. The contents are initialized to 0. If the
  * requested number of bytes could not be allocated an exception is raised.
  */
interface Uint16Array {
    /**
      * The size in bytes of each element in the array.
      */

    /**
      * The ArrayBuffer instance referenced by the array.
      */

    /**
      * The length in bytes of the array.
      */

    /**
      * The offset in bytes of the array.
      */

    /**
      * Returns the this object after copying a section of the array identified by start and end
      * to the same array starting at position target
      * @param target If target is negative, it is treated as length+target where length is the
      * length of the array.
      * @param start If start is negative, it is treated as length+start. If end is negative, it
      * is treated as length+end.
      * @param end If not specified, length of the this object is used as its default value.
      */
    copyWithin(target: number, start: number, end?: number): Uint16Array;

    /**
      * Returns an array of key, value pairs for every entry in the array
      */
    entries(): IterableIterator<[number, number]>;

    /**
      * Determines whether all the members of an array satisfy the specified test.
      * @param callbackfn A function that accepts up to three arguments. The every method calls
      * the callbackfn function for each element in array1 until the callbackfn returns false,
      * or until the end of the array.
      * @param thisArg An object to which the this keyword can refer in the callbackfn function.
      * If thisArg is omitted, undefined is used as the this value.
      */
    every(callbackfn: (value: number, index: number, array: Uint16Array) => boolean, thisArg?: any): boolean;

    /**
        * Returns the this object after filling the section identified by start and end with value
        * @param value value to fill array section with
        * @param start index to start filling the array at. If start is negative, it is treated as
        * length+start where length is the length of the array.
        * @param end index to stop filling the array at. If end is negative, it is treated as
        * length+end.
        */
    fill(value: number, start?: number, end?: number): Uint16Array;

    /**
      * Returns the elements of an array that meet the condition specified in a callback function.
      * @param callbackfn A function that accepts up to three arguments. The filter method calls
      * the callbackfn function one time for each element in the array.
      * @param thisArg An object to which the this keyword can refer in the callbackfn function.
      * If thisArg is omitted, undefined is used as the this value.
      */
    filter(callbackfn: (value: number, index: number, array: Uint16Array) => boolean, thisArg?: any): Uint16Array;

    /**
      * Returns the value of the first element in the array where predicate is true, and undefined
      * otherwise.
      * @param predicate find calls predicate once for each element of the array, in ascending
      * order, until it finds one where predicate returns true. If such an element is found, find
      * immediately returns that element value. Otherwise, find returns undefined.
      * @param thisArg If provided, it will be used as the this value for each invocation of
      * predicate. If it is not provided, undefined is used instead.
      */
    find(predicate: (value: number, index: number, obj: Array<number>) => boolean, thisArg?: any): number;

    /**
      * Returns the index of the first element in the array where predicate is true, and undefined
      * otherwise.
      * @param predicate find calls predicate once for each element of the array, in ascending
      * order, until it finds one where predicate returns true. If such an element is found, find
      * immediately returns that element value. Otherwise, find returns undefined.
      * @param thisArg If provided, it will be used as the this value for each invocation of
      * predicate. If it is not provided, undefined is used instead.
      */
    findIndex(predicate: (value: number) => boolean, thisArg?: any): number;

    /**
      * Performs the specified action for each element in an array.
      * @param callbackfn  A function that accepts up to three arguments. forEach calls the
      * callbackfn function one time for each element in the array.
      * @param thisArg  An object to which the this keyword can refer in the callbackfn function.
      * If thisArg is omitted, undefined is used as the this value.
      */
    forEach(callbackfn: (value: number, index: number, array: Uint16Array) => void, thisArg?: any): void;

    /**
      * Returns the index of the first occurrence of a value in an array.
      * @param searchElement The value to locate in the array.
      * @param fromIndex The array index at which to begin the search. If fromIndex is omitted, the
      *  search starts at index 0.
      */
    indexOf(searchElement: number, fromIndex?: number): number;

    /**
      * Adds all the elements of an array separated by the specified separator string.
      * @param separator A string used to separate one element of an array from the next in the
      * resulting String. If omitted, the array elements are separated with a comma.
      */
    join(separator?: string): string;

    /**
      * Returns an list of keys in the array
      */
    keys(): IterableIterator<number>;

    /**
      * Returns the index of the last occurrence of a value in an array.
      * @param searchElement The value to locate in the array.
      * @param fromIndex The array index at which to begin the search. If fromIndex is omitted, the
      * search starts at index 0.
      */
    lastIndexOf(searchElement: number, fromIndex?: number): number;

    /**
      * The length of the array.
      */

    /**
      * Calls a defined callback function on each element of an array, and returns an array that
      * contains the results.
      * @param callbackfn A function that accepts up to three arguments. The map method calls the
      * callbackfn function one time for each element in the array.
      * @param thisArg An object to which the this keyword can refer in the callbackfn function.
      * If thisArg is omitted, undefined is used as the this value.
      */
    map(callbackfn: (value: number, index: number, array: Uint16Array) => number, thisArg?: any): Uint16Array;

    /**
      * Calls the specified callback function for all the elements in an array. The return value of
      * the callback function is the accumulated result, and is provided as an argument in the next
      * call to the callback function.
      * @param callbackfn A function that accepts up to four arguments. The reduce method calls the
      * callbackfn function one time for each element in the array.
      * @param initialValue If initialValue is specified, it is used as the initial value to start
      * the accumulation. The first call to the callbackfn function provides this value as an argument
      * instead of an array value.
      */
    reduce(callbackfn: (previousValue: number, currentValue: number, currentIndex: number, array: Uint16Array) => number, initialValue?: number): number;

    /**
      * Calls the specified callback function for all the elements in an array. The return value of
      * the callback function is the accumulated result, and is provided as an argument in the next
      * call to the callback function.
      * @param callbackfn A function that accepts up to four arguments. The reduce method calls the
      * callbackfn function one time for each element in the array.
      * @param initialValue If initialValue is specified, it is used as the initial value to start
      * the accumulation. The first call to the callbackfn function provides this value as an argument
      * instead of an array value.
      */
    reduce<U>(callbackfn: (previousValue: U, currentValue: number, currentIndex: number, array: Uint16Array) => U, initialValue: U): U;

    /**
      * Calls the specified callback function for all the elements in an array, in descending order.
      * The return value of the callback function is the accumulated result, and is provided as an
      * argument in the next call to the callback function.
      * @param callbackfn A function that accepts up to four arguments. The reduceRight method calls
      * the callbackfn function one time for each element in the array.
      * @param initialValue If initialValue is specified, it is used as the initial value to start
      * the accumulation. The first call to the callbackfn function provides this value as an
      * argument instead of an array value.
      */
    reduceRight(callbackfn: (previousValue: number, currentValue: number, currentIndex: number, array: Uint16Array) => number, initialValue?: number): number;

    /**
      * Calls the specified callback function for all the elements in an array, in descending order.
      * The return value of the callback function is the accumulated result, and is provided as an
      * argument in the next call to the callback function.
      * @param callbackfn A function that accepts up to four arguments. The reduceRight method calls
      * the callbackfn function one time for each element in the array.
      * @param initialValue If initialValue is specified, it is used as the initial value to start
      * the accumulation. The first call to the callbackfn function provides this value as an argument
      * instead of an array value.
      */
    reduceRight<U>(callbackfn: (previousValue: U, currentValue: number, currentIndex: number, array: Uint16Array) => U, initialValue: U): U;

    /**
      * Reverses the elements in an Array.
      */
    reverse(): Uint16Array;

    /**
      * Sets a value or an array of values.
      * @param index The index of the location to set.
      * @param value The value to set.
      */
    set(index: number, value: number): void;

    /**
      * Sets a value or an array of values.
      * @param array A typed or untyped array of values to set.
      * @param offset The index in the current array at which the values are to be written.
      */
    set(array: Uint16Array, offset?: number): void;

    /**
      * Returns a section of an array.
      * @param start The beginning of the specified portion of the array.
      * @param end The end of the specified portion of the array.
      */
    slice(start?: number, end?: number): Uint16Array;

    /**
      * Determines whether the specified callback function returns true for any element of an array.
      * @param callbackfn A function that accepts up to three arguments. The some method calls the
      * callbackfn function for each element in array1 until the callbackfn returns true, or until
      * the end of the array.
      * @param thisArg An object to which the this keyword can refer in the callbackfn function.
      * If thisArg is omitted, undefined is used as the this value.
      */
    some(callbackfn: (value: number, index: number, array: Uint16Array) => boolean, thisArg?: any): boolean;

    /**
      * Sorts an array.
      * @param compareFn The name of the function used to determine the order of the elements. If
      * omitted, the elements are sorted in ascending, ASCII character order.
      */
    sort(compareFn?: (a: number, b: number) => number): Uint16Array;

    /**
      * Gets a new Uint16Array view of the ArrayBuffer store for this array, referencing the elements
      * at begin, inclusive, up to end, exclusive.
      * @param begin The index of the beginning of the array.
      * @param end The index of the end of the array.
      */
    subarray(begin: number, end?: number): Uint16Array;

    /**
      * Converts a number to a string by using the current locale.
      */
    toLocaleString(): string;

    /**
      * Returns a string representation of an array.
      */
    toString(): string;

    /**
      * Returns an list of values in the array
      */
    values(): IterableIterator<number>;

    //[index: number]: number;
    [Symbol.iterator](): IterableIterator<number>;
}

interface Uint16ArrayConstructor {
    new (length: number): Uint16Array;
    new (array: Uint16Array): Uint16Array;
    new (array: number[]): Uint16Array;
    new (buffer: ArrayBuffer, byteOffset?: number, length?: number): Uint16Array;

    /**
      * The size in bytes of each element in the array.
      */

    /**
      * Returns a new array from a set of elements.
      * @param items A set of elements to include in the new array object.
      */
    of(...items: number[]): Uint16Array;

    /**
      * Creates an array from an array-like or iterable object.
      * @param arrayLike An array-like or iterable object to convert to an array.
      * @param mapfn A mapping function to call on every element of the array.
      * @param thisArg Value of 'this' used to invoke the mapfn.
      */
    from(arrayLike: ArrayLike<number> | Iterable<number>, mapfn?: (v: number, k: number) => number, thisArg?: any): Uint16Array;
}
declare var Uint16Array: Uint16ArrayConstructor;

/**
  * A typed array of 32-bit signed integer values. The contents are initialized to 0. If the
  * requested number of bytes could not be allocated an exception is raised.
  */
interface Int32Array {
    /**
      * The size in bytes of each element in the array.
      */

    /**
      * The ArrayBuffer instance referenced by the array.
      */

    /**
      * The length in bytes of the array.
      */

    /**
      * The offset in bytes of the array.
      */

    /**
      * Returns the this object after copying a section of the array identified by start and end
      * to the same array starting at position target
      * @param target If target is negative, it is treated as length+target where length is the
      * length of the array.
      * @param start If start is negative, it is treated as length+start. If end is negative, it
      * is treated as length+end.
      * @param end If not specified, length of the this object is used as its default value.
      */
    copyWithin(target: number, start: number, end?: number): Int32Array;

    /**
      * Returns an array of key, value pairs for every entry in the array
      */
    entries(): IterableIterator<[number, number]>;

    /**
      * Determines whether all the members of an array satisfy the specified test.
      * @param callbackfn A function that accepts up to three arguments. The every method calls
      * the callbackfn function for each element in array1 until the callbackfn returns false,
      * or until the end of the array.
      * @param thisArg An object to which the this keyword can refer in the callbackfn function.
      * If thisArg is omitted, undefined is used as the this value.
      */
    every(callbackfn: (value: number, index: number, array: Int32Array) => boolean, thisArg?: any): boolean;

    /**
        * Returns the this object after filling the section identified by start and end with value
        * @param value value to fill array section with
        * @param start index to start filling the array at. If start is negative, it is treated as
        * length+start where length is the length of the array.
        * @param end index to stop filling the array at. If end is negative, it is treated as
        * length+end.
        */
    fill(value: number, start?: number, end?: number): Int32Array;

    /**
      * Returns the elements of an array that meet the condition specified in a callback function.
      * @param callbackfn A function that accepts up to three arguments. The filter method calls
      * the callbackfn function one time for each element in the array.
      * @param thisArg An object to which the this keyword can refer in the callbackfn function.
      * If thisArg is omitted, undefined is used as the this value.
      */
    filter(callbackfn: (value: number, index: number, array: Int32Array) => boolean, thisArg?: any): Int32Array;

    /**
      * Returns the value of the first element in the array where predicate is true, and undefined
      * otherwise.
      * @param predicate find calls predicate once for each element of the array, in ascending
      * order, until it finds one where predicate returns true. If such an element is found, find
      * immediately returns that element value. Otherwise, find returns undefined.
      * @param thisArg If provided, it will be used as the this value for each invocation of
      * predicate. If it is not provided, undefined is used instead.
      */
    find(predicate: (value: number, index: number, obj: Array<number>) => boolean, thisArg?: any): number;

    /**
      * Returns the index of the first element in the array where predicate is true, and undefined
      * otherwise.
      * @param predicate find calls predicate once for each element of the array, in ascending
      * order, until it finds one where predicate returns true. If such an element is found, find
      * immediately returns that element value. Otherwise, find returns undefined.
      * @param thisArg If provided, it will be used as the this value for each invocation of
      * predicate. If it is not provided, undefined is used instead.
      */
    findIndex(predicate: (value: number) => boolean, thisArg?: any): number;

    /**
      * Performs the specified action for each element in an array.
      * @param callbackfn  A function that accepts up to three arguments. forEach calls the
      * callbackfn function one time for each element in the array.
      * @param thisArg  An object to which the this keyword can refer in the callbackfn function.
      * If thisArg is omitted, undefined is used as the this value.
      */
    forEach(callbackfn: (value: number, index: number, array: Int32Array) => void, thisArg?: any): void;

    /**
      * Returns the index of the first occurrence of a value in an array.
      * @param searchElement The value to locate in the array.
      * @param fromIndex The array index at which to begin the search. If fromIndex is omitted, the
      *  search starts at index 0.
      */
    indexOf(searchElement: number, fromIndex?: number): number;

    /**
      * Adds all the elements of an array separated by the specified separator string.
      * @param separator A string used to separate one element of an array from the next in the
      * resulting String. If omitted, the array elements are separated with a comma.
      */
    join(separator?: string): string;

    /**
      * Returns an list of keys in the array
      */
    keys(): IterableIterator<number>;

    /**
      * Returns the index of the last occurrence of a value in an array.
      * @param searchElement The value to locate in the array.
      * @param fromIndex The array index at which to begin the search. If fromIndex is omitted, the
      * search starts at index 0.
      */
    lastIndexOf(searchElement: number, fromIndex?: number): number;

    /**
      * The length of the array.
      */

    /**
      * Calls a defined callback function on each element of an array, and returns an array that
      * contains the results.
      * @param callbackfn A function that accepts up to three arguments. The map method calls the
      * callbackfn function one time for each element in the array.
      * @param thisArg An object to which the this keyword can refer in the callbackfn function.
      * If thisArg is omitted, undefined is used as the this value.
      */
    map(callbackfn: (value: number, index: number, array: Int32Array) => number, thisArg?: any): Int32Array;

    /**
      * Calls the specified callback function for all the elements in an array. The return value of
      * the callback function is the accumulated result, and is provided as an argument in the next
      * call to the callback function.
      * @param callbackfn A function that accepts up to four arguments. The reduce method calls the
      * callbackfn function one time for each element in the array.
      * @param initialValue If initialValue is specified, it is used as the initial value to start
      * the accumulation. The first call to the callbackfn function provides this value as an argument
      * instead of an array value.
      */
    reduce(callbackfn: (previousValue: number, currentValue: number, currentIndex: number, array: Int32Array) => number, initialValue?: number): number;

    /**
      * Calls the specified callback function for all the elements in an array. The return value of
      * the callback function is the accumulated result, and is provided as an argument in the next
      * call to the callback function.
      * @param callbackfn A function that accepts up to four arguments. The reduce method calls the
      * callbackfn function one time for each element in the array.
      * @param initialValue If initialValue is specified, it is used as the initial value to start
      * the accumulation. The first call to the callbackfn function provides this value as an argument
      * instead of an array value.
      */
    reduce<U>(callbackfn: (previousValue: U, currentValue: number, currentIndex: number, array: Int32Array) => U, initialValue: U): U;

    /**
      * Calls the specified callback function for all the elements in an array, in descending order.
      * The return value of the callback function is the accumulated result, and is provided as an
      * argument in the next call to the callback function.
      * @param callbackfn A function that accepts up to four arguments. The reduceRight method calls
      * the callbackfn function one time for each element in the array.
      * @param initialValue If initialValue is specified, it is used as the initial value to start
      * the accumulation. The first call to the callbackfn function provides this value as an
      * argument instead of an array value.
      */
    reduceRight(callbackfn: (previousValue: number, currentValue: number, currentIndex: number, array: Int32Array) => number, initialValue?: number): number;

    /**
      * Calls the specified callback function for all the elements in an array, in descending order.
      * The return value of the callback function is the accumulated result, and is provided as an
      * argument in the next call to the callback function.
      * @param callbackfn A function that accepts up to four arguments. The reduceRight method calls
      * the callbackfn function one time for each element in the array.
      * @param initialValue If initialValue is specified, it is used as the initial value to start
      * the accumulation. The first call to the callbackfn function provides this value as an argument
      * instead of an array value.
      */
    reduceRight<U>(callbackfn: (previousValue: U, currentValue: number, currentIndex: number, array: Int32Array) => U, initialValue: U): U;

    /**
      * Reverses the elements in an Array.
      */
    reverse(): Int32Array;

    /**
      * Sets a value or an array of values.
      * @param index The index of the location to set.
      * @param value The value to set.
      */
    set(index: number, value: number): void;

    /**
      * Sets a value or an array of values.
      * @param array A typed or untyped array of values to set.
      * @param offset The index in the current array at which the values are to be written.
      */
    set(array: Int32Array, offset?: number): void;

    /**
      * Returns a section of an array.
      * @param start The beginning of the specified portion of the array.
      * @param end The end of the specified portion of the array.
      */
    slice(start?: number, end?: number): Int32Array;

    /**
      * Determines whether the specified callback function returns true for any element of an array.
      * @param callbackfn A function that accepts up to three arguments. The some method calls the
      * callbackfn function for each element in array1 until the callbackfn returns true, or until
      * the end of the array.
      * @param thisArg An object to which the this keyword can refer in the callbackfn function.
      * If thisArg is omitted, undefined is used as the this value.
      */
    some(callbackfn: (value: number, index: number, array: Int32Array) => boolean, thisArg?: any): boolean;

    /**
      * Sorts an array.
      * @param compareFn The name of the function used to determine the order of the elements. If
      * omitted, the elements are sorted in ascending, ASCII character order.
      */
    sort(compareFn?: (a: number, b: number) => number): Int32Array;

    /**
      * Gets a new Int32Array view of the ArrayBuffer store for this array, referencing the elements
      * at begin, inclusive, up to end, exclusive.
      * @param begin The index of the beginning of the array.
      * @param end The index of the end of the array.
      */
    subarray(begin: number, end?: number): Int32Array;

    /**
      * Converts a number to a string by using the current locale.
      */
    toLocaleString(): string;

    /**
      * Returns a string representation of an array.
      */
    toString(): string;

    /**
      * Returns an list of values in the array
      */
    values(): IterableIterator<number>;

    //[index: number]: number;
    [Symbol.iterator](): IterableIterator<number>;
}

interface Int32ArrayConstructor {
    new (length: number): Int32Array;
    new (array: Int32Array): Int32Array;
    new (array: number[]): Int32Array;
    new (buffer: ArrayBuffer, byteOffset?: number, length?: number): Int32Array;

    /**
      * The size in bytes of each element in the array.
      */

    /**
      * Returns a new array from a set of elements.
      * @param items A set of elements to include in the new array object.
      */
    of(...items: number[]): Int32Array;

    /**
      * Creates an array from an array-like or iterable object.
      * @param arrayLike An array-like or iterable object to convert to an array.
      * @param mapfn A mapping function to call on every element of the array.
      * @param thisArg Value of 'this' used to invoke the mapfn.
      */
    from(arrayLike: ArrayLike<number> | Iterable<number>, mapfn?: (v: number, k: number) => number, thisArg?: any): Int32Array;
}
declare var Int32Array: Int32ArrayConstructor;

/**
  * A typed array of 32-bit unsigned integer values. The contents are initialized to 0. If the
  * requested number of bytes could not be allocated an exception is raised.
  */
interface Uint32Array {
    /**
      * The size in bytes of each element in the array.
      */

    /**
      * The ArrayBuffer instance referenced by the array.
      */

    /**
      * The length in bytes of the array.
      */

    /**
      * The offset in bytes of the array.
      */

    /**
      * Returns the this object after copying a section of the array identified by start and end
      * to the same array starting at position target
      * @param target If target is negative, it is treated as length+target where length is the
      * length of the array.
      * @param start If start is negative, it is treated as length+start. If end is negative, it
      * is treated as length+end.
      * @param end If not specified, length of the this object is used as its default value.
      */
    copyWithin(target: number, start: number, end?: number): Uint32Array;

    /**
      * Returns an array of key, value pairs for every entry in the array
      */
    entries(): IterableIterator<[number, number]>;

    /**
      * Determines whether all the members of an array satisfy the specified test.
      * @param callbackfn A function that accepts up to three arguments. The every method calls
      * the callbackfn function for each element in array1 until the callbackfn returns false,
      * or until the end of the array.
      * @param thisArg An object to which the this keyword can refer in the callbackfn function.
      * If thisArg is omitted, undefined is used as the this value.
      */
    every(callbackfn: (value: number, index: number, array: Uint32Array) => boolean, thisArg?: any): boolean;

    /**
        * Returns the this object after filling the section identified by start and end with value
        * @param value value to fill array section with
        * @param start index to start filling the array at. If start is negative, it is treated as
        * length+start where length is the length of the array.
        * @param end index to stop filling the array at. If end is negative, it is treated as
        * length+end.
        */
    fill(value: number, start?: number, end?: number): Uint32Array;

    /**
      * Returns the elements of an array that meet the condition specified in a callback function.
      * @param callbackfn A function that accepts up to three arguments. The filter method calls
      * the callbackfn function one time for each element in the array.
      * @param thisArg An object to which the this keyword can refer in the callbackfn function.
      * If thisArg is omitted, undefined is used as the this value.
      */
    filter(callbackfn: (value: number, index: number, array: Uint32Array) => boolean, thisArg?: any): Uint32Array;

    /**
      * Returns the value of the first element in the array where predicate is true, and undefined
      * otherwise.
      * @param predicate find calls predicate once for each element of the array, in ascending
      * order, until it finds one where predicate returns true. If such an element is found, find
      * immediately returns that element value. Otherwise, find returns undefined.
      * @param thisArg If provided, it will be used as the this value for each invocation of
      * predicate. If it is not provided, undefined is used instead.
      */
    find(predicate: (value: number, index: number, obj: Array<number>) => boolean, thisArg?: any): number;

    /**
      * Returns the index of the first element in the array where predicate is true, and undefined
      * otherwise.
      * @param predicate find calls predicate once for each element of the array, in ascending
      * order, until it finds one where predicate returns true. If such an element is found, find
      * immediately returns that element value. Otherwise, find returns undefined.
      * @param thisArg If provided, it will be used as the this value for each invocation of
      * predicate. If it is not provided, undefined is used instead.
      */
    findIndex(predicate: (value: number) => boolean, thisArg?: any): number;

    /**
      * Performs the specified action for each element in an array.
      * @param callbackfn  A function that accepts up to three arguments. forEach calls the
      * callbackfn function one time for each element in the array.
      * @param thisArg  An object to which the this keyword can refer in the callbackfn function.
      * If thisArg is omitted, undefined is used as the this value.
      */
    forEach(callbackfn: (value: number, index: number, array: Uint32Array) => void, thisArg?: any): void;

    /**
      * Returns the index of the first occurrence of a value in an array.
      * @param searchElement The value to locate in the array.
      * @param fromIndex The array index at which to begin the search. If fromIndex is omitted, the
      *  search starts at index 0.
      */
    indexOf(searchElement: number, fromIndex?: number): number;

    /**
      * Adds all the elements of an array separated by the specified separator string.
      * @param separator A string used to separate one element of an array from the next in the
      * resulting String. If omitted, the array elements are separated with a comma.
      */
    join(separator?: string): string;

    /**
      * Returns an list of keys in the array
      */
    keys(): IterableIterator<number>;

    /**
      * Returns the index of the last occurrence of a value in an array.
      * @param searchElement The value to locate in the array.
      * @param fromIndex The array index at which to begin the search. If fromIndex is omitted, the
      * search starts at index 0.
      */
    lastIndexOf(searchElement: number, fromIndex?: number): number;

    /**
      * The length of the array.
      */

    /**
      * Calls a defined callback function on each element of an array, and returns an array that
      * contains the results.
      * @param callbackfn A function that accepts up to three arguments. The map method calls the
      * callbackfn function one time for each element in the array.
      * @param thisArg An object to which the this keyword can refer in the callbackfn function.
      * If thisArg is omitted, undefined is used as the this value.
      */
    map(callbackfn: (value: number, index: number, array: Uint32Array) => number, thisArg?: any): Uint32Array;

    /**
      * Calls the specified callback function for all the elements in an array. The return value of
      * the callback function is the accumulated result, and is provided as an argument in the next
      * call to the callback function.
      * @param callbackfn A function that accepts up to four arguments. The reduce method calls the
      * callbackfn function one time for each element in the array.
      * @param initialValue If initialValue is specified, it is used as the initial value to start
      * the accumulation. The first call to the callbackfn function provides this value as an argument
      * instead of an array value.
      */
    reduce(callbackfn: (previousValue: number, currentValue: number, currentIndex: number, array: Uint32Array) => number, initialValue?: number): number;

    /**
      * Calls the specified callback function for all the elements in an array. The return value of
      * the callback function is the accumulated result, and is provided as an argument in the next
      * call to the callback function.
      * @param callbackfn A function that accepts up to four arguments. The reduce method calls the
      * callbackfn function one time for each element in the array.
      * @param initialValue If initialValue is specified, it is used as the initial value to start
      * the accumulation. The first call to the callbackfn function provides this value as an argument
      * instead of an array value.
      */
    reduce<U>(callbackfn: (previousValue: U, currentValue: number, currentIndex: number, array: Uint32Array) => U, initialValue: U): U;

    /**
      * Calls the specified callback function for all the elements in an array, in descending order.
      * The return value of the callback function is the accumulated result, and is provided as an
      * argument in the next call to the callback function.
      * @param callbackfn A function that accepts up to four arguments. The reduceRight method calls
      * the callbackfn function one time for each element in the array.
      * @param initialValue If initialValue is specified, it is used as the initial value to start
      * the accumulation. The first call to the callbackfn function provides this value as an
      * argument instead of an array value.
      */
    reduceRight(callbackfn: (previousValue: number, currentValue: number, currentIndex: number, array: Uint32Array) => number, initialValue?: number): number;

    /**
      * Calls the specified callback function for all the elements in an array, in descending order.
      * The return value of the callback function is the accumulated result, and is provided as an
      * argument in the next call to the callback function.
      * @param callbackfn A function that accepts up to four arguments. The reduceRight method calls
      * the callbackfn function one time for each element in the array.
      * @param initialValue If initialValue is specified, it is used as the initial value to start
      * the accumulation. The first call to the callbackfn function provides this value as an argument
      * instead of an array value.
      */
    reduceRight<U>(callbackfn: (previousValue: U, currentValue: number, currentIndex: number, array: Uint32Array) => U, initialValue: U): U;

    /**
      * Reverses the elements in an Array.
      */
    reverse(): Uint32Array;

    /**
      * Sets a value or an array of values.
      * @param index The index of the location to set.
      * @param value The value to set.
      */
    set(index: number, value: number): void;

    /**
      * Sets a value or an array of values.
      * @param array A typed or untyped array of values to set.
      * @param offset The index in the current array at which the values are to be written.
      */
    set(array: Uint32Array, offset?: number): void;

    /**
      * Returns a section of an array.
      * @param start The beginning of the specified portion of the array.
      * @param end The end of the specified portion of the array.
      */
    slice(start?: number, end?: number): Uint32Array;

    /**
      * Determines whether the specified callback function returns true for any element of an array.
      * @param callbackfn A function that accepts up to three arguments. The some method calls the
      * callbackfn function for each element in array1 until the callbackfn returns true, or until
      * the end of the array.
      * @param thisArg An object to which the this keyword can refer in the callbackfn function.
      * If thisArg is omitted, undefined is used as the this value.
      */
    some(callbackfn: (value: number, index: number, array: Uint32Array) => boolean, thisArg?: any): boolean;

    /**
      * Sorts an array.
      * @param compareFn The name of the function used to determine the order of the elements. If
      * omitted, the elements are sorted in ascending, ASCII character order.
      */
    sort(compareFn?: (a: number, b: number) => number): Uint32Array;

    /**
      * Gets a new Uint32Array view of the ArrayBuffer store for this array, referencing the elements
      * at begin, inclusive, up to end, exclusive.
      * @param begin The index of the beginning of the array.
      * @param end The index of the end of the array.
      */
    subarray(begin: number, end?: number): Uint32Array;

    /**
      * Converts a number to a string by using the current locale.
      */
    toLocaleString(): string;

    /**
      * Returns a string representation of an array.
      */
    toString(): string;

    /**
      * Returns an list of values in the array
      */
    values(): IterableIterator<number>;

    //[index: number]: number;
    [Symbol.iterator](): IterableIterator<number>;
}

interface Uint32ArrayConstructor {
    new (length: number): Uint32Array;
    new (array: Uint32Array): Uint32Array;
    new (array: number[]): Uint32Array;
    new (buffer: ArrayBuffer, byteOffset?: number, length?: number): Uint32Array;

    /**
      * The size in bytes of each element in the array.
      */

    /**
      * Returns a new array from a set of elements.
      * @param items A set of elements to include in the new array object.
      */
    of(...items: number[]): Uint32Array;

    /**
      * Creates an array from an array-like or iterable object.
      * @param arrayLike An array-like or iterable object to convert to an array.
      * @param mapfn A mapping function to call on every element of the array.
      * @param thisArg Value of 'this' used to invoke the mapfn.
      */
    from(arrayLike: ArrayLike<number> | Iterable<number>, mapfn?: (v: number, k: number) => number, thisArg?: any): Uint32Array;
}
declare var Uint32Array: Uint32ArrayConstructor;

/**
  * A typed array of 32-bit float values. The contents are initialized to 0. If the requested number
  * of bytes could not be allocated an exception is raised.
  */
interface Float32Array {
    /**
      * The size in bytes of each element in the array.
      */

    /**
      * The ArrayBuffer instance referenced by the array.
      */

    /**
      * The length in bytes of the array.
      */

    /**
      * The offset in bytes of the array.
      */

    /**
      * Returns the this object after copying a section of the array identified by start and end
      * to the same array starting at position target
      * @param target If target is negative, it is treated as length+target where length is the
      * length of the array.
      * @param start If start is negative, it is treated as length+start. If end is negative, it
      * is treated as length+end.
      * @param end If not specified, length of the this object is used as its default value.
      */
    copyWithin(target: number, start: number, end?: number): Float32Array;

    /**
      * Returns an array of key, value pairs for every entry in the array
      */
    entries(): IterableIterator<[number, number]>;

    /**
      * Determines whether all the members of an array satisfy the specified test.
      * @param callbackfn A function that accepts up to three arguments. The every method calls
      * the callbackfn function for each element in array1 until the callbackfn returns false,
      * or until the end of the array.
      * @param thisArg An object to which the this keyword can refer in the callbackfn function.
      * If thisArg is omitted, undefined is used as the this value.
      */
    every(callbackfn: (value: number, index: number, array: Float32Array) => boolean, thisArg?: any): boolean;

    /**
        * Returns the this object after filling the section identified by start and end with value
        * @param value value to fill array section with
        * @param start index to start filling the array at. If start is negative, it is treated as
        * length+start where length is the length of the array.
        * @param end index to stop filling the array at. If end is negative, it is treated as
        * length+end.
        */
    fill(value: number, start?: number, end?: number): Float32Array;

    /**
      * Returns the elements of an array that meet the condition specified in a callback function.
      * @param callbackfn A function that accepts up to three arguments. The filter method calls
      * the callbackfn function one time for each element in the array.
      * @param thisArg An object to which the this keyword can refer in the callbackfn function.
      * If thisArg is omitted, undefined is used as the this value.
      */
    filter(callbackfn: (value: number, index: number, array: Float32Array) => boolean, thisArg?: any): Float32Array;

    /**
      * Returns the value of the first element in the array where predicate is true, and undefined
      * otherwise.
      * @param predicate find calls predicate once for each element of the array, in ascending
      * order, until it finds one where predicate returns true. If such an element is found, find
      * immediately returns that element value. Otherwise, find returns undefined.
      * @param thisArg If provided, it will be used as the this value for each invocation of
      * predicate. If it is not provided, undefined is used instead.
      */
    find(predicate: (value: number, index: number, obj: Array<number>) => boolean, thisArg?: any): number;

    /**
      * Returns the index of the first element in the array where predicate is true, and undefined
      * otherwise.
      * @param predicate find calls predicate once for each element of the array, in ascending
      * order, until it finds one where predicate returns true. If such an element is found, find
      * immediately returns that element value. Otherwise, find returns undefined.
      * @param thisArg If provided, it will be used as the this value for each invocation of
      * predicate. If it is not provided, undefined is used instead.
      */
    findIndex(predicate: (value: number) => boolean, thisArg?: any): number;

    /**
      * Performs the specified action for each element in an array.
      * @param callbackfn  A function that accepts up to three arguments. forEach calls the
      * callbackfn function one time for each element in the array.
      * @param thisArg  An object to which the this keyword can refer in the callbackfn function.
      * If thisArg is omitted, undefined is used as the this value.
      */
    forEach(callbackfn: (value: number, index: number, array: Float32Array) => void, thisArg?: any): void;

    /**
      * Returns the index of the first occurrence of a value in an array.
      * @param searchElement The value to locate in the array.
      * @param fromIndex The array index at which to begin the search. If fromIndex is omitted, the
      *  search starts at index 0.
      */
    indexOf(searchElement: number, fromIndex?: number): number;

    /**
      * Adds all the elements of an array separated by the specified separator string.
      * @param separator A string used to separate one element of an array from the next in the
      * resulting String. If omitted, the array elements are separated with a comma.
      */
    join(separator?: string): string;

    /**
      * Returns an list of keys in the array
      */
    keys(): IterableIterator<number>;

    /**
      * Returns the index of the last occurrence of a value in an array.
      * @param searchElement The value to locate in the array.
      * @param fromIndex The array index at which to begin the search. If fromIndex is omitted, the
      * search starts at index 0.
      */
    lastIndexOf(searchElement: number, fromIndex?: number): number;

    /**
      * The length of the array.
      */

    /**
      * Calls a defined callback function on each element of an array, and returns an array that
      * contains the results.
      * @param callbackfn A function that accepts up to three arguments. The map method calls the
      * callbackfn function one time for each element in the array.
      * @param thisArg An object to which the this keyword can refer in the callbackfn function.
      * If thisArg is omitted, undefined is used as the this value.
      */
    map(callbackfn: (value: number, index: number, array: Float32Array) => number, thisArg?: any): Float32Array;

    /**
      * Calls the specified callback function for all the elements in an array. The return value of
      * the callback function is the accumulated result, and is provided as an argument in the next
      * call to the callback function.
      * @param callbackfn A function that accepts up to four arguments. The reduce method calls the
      * callbackfn function one time for each element in the array.
      * @param initialValue If initialValue is specified, it is used as the initial value to start
      * the accumulation. The first call to the callbackfn function provides this value as an argument
      * instead of an array value.
      */
    reduce(callbackfn: (previousValue: number, currentValue: number, currentIndex: number, array: Float32Array) => number, initialValue?: number): number;

    /**
      * Calls the specified callback function for all the elements in an array. The return value of
      * the callback function is the accumulated result, and is provided as an argument in the next
      * call to the callback function.
      * @param callbackfn A function that accepts up to four arguments. The reduce method calls the
      * callbackfn function one time for each element in the array.
      * @param initialValue If initialValue is specified, it is used as the initial value to start
      * the accumulation. The first call to the callbackfn function provides this value as an argument
      * instead of an array value.
      */
    reduce<U>(callbackfn: (previousValue: U, currentValue: number, currentIndex: number, array: Float32Array) => U, initialValue: U): U;

    /**
      * Calls the specified callback function for all the elements in an array, in descending order.
      * The return value of the callback function is the accumulated result, and is provided as an
      * argument in the next call to the callback function.
      * @param callbackfn A function that accepts up to four arguments. The reduceRight method calls
      * the callbackfn function one time for each element in the array.
      * @param initialValue If initialValue is specified, it is used as the initial value to start
      * the accumulation. The first call to the callbackfn function provides this value as an
      * argument instead of an array value.
      */
    reduceRight(callbackfn: (previousValue: number, currentValue: number, currentIndex: number, array: Float32Array) => number, initialValue?: number): number;

    /**
      * Calls the specified callback function for all the elements in an array, in descending order.
      * The return value of the callback function is the accumulated result, and is provided as an
      * argument in the next call to the callback function.
      * @param callbackfn A function that accepts up to four arguments. The reduceRight method calls
      * the callbackfn function one time for each element in the array.
      * @param initialValue If initialValue is specified, it is used as the initial value to start
      * the accumulation. The first call to the callbackfn function provides this value as an argument
      * instead of an array value.
      */
    reduceRight<U>(callbackfn: (previousValue: U, currentValue: number, currentIndex: number, array: Float32Array) => U, initialValue: U): U;

    /**
      * Reverses the elements in an Array.
      */
    reverse(): Float32Array;

    /**
      * Sets a value or an array of values.
      * @param index The index of the location to set.
      * @param value The value to set.
      */
    set(index: number, value: number): void;

    /**
      * Sets a value or an array of values.
      * @param array A typed or untyped array of values to set.
      * @param offset The index in the current array at which the values are to be written.
      */
    set(array: Float32Array, offset?: number): void;

    /**
      * Returns a section of an array.
      * @param start The beginning of the specified portion of the array.
      * @param end The end of the specified portion of the array.
      */
    slice(start?: number, end?: number): Float32Array;

    /**
      * Determines whether the specified callback function returns true for any element of an array.
      * @param callbackfn A function that accepts up to three arguments. The some method calls the
      * callbackfn function for each element in array1 until the callbackfn returns true, or until
      * the end of the array.
      * @param thisArg An object to which the this keyword can refer in the callbackfn function.
      * If thisArg is omitted, undefined is used as the this value.
      */
    some(callbackfn: (value: number, index: number, array: Float32Array) => boolean, thisArg?: any): boolean;

    /**
      * Sorts an array.
      * @param compareFn The name of the function used to determine the order of the elements. If
      * omitted, the elements are sorted in ascending, ASCII character order.
      */
    sort(compareFn?: (a: number, b: number) => number): Float32Array;

    /**
      * Gets a new Float32Array view of the ArrayBuffer store for this array, referencing the elements
      * at begin, inclusive, up to end, exclusive.
      * @param begin The index of the beginning of the array.
      * @param end The index of the end of the array.
      */
    subarray(begin: number, end?: number): Float32Array;

    /**
      * Converts a number to a string by using the current locale.
      */
    toLocaleString(): string;

    /**
      * Returns a string representation of an array.
      */
    toString(): string;

    /**
      * Returns an list of values in the array
      */
    values(): IterableIterator<number>;

    //[index: number]: number;
    [Symbol.iterator](): IterableIterator<number>;
}

interface Float32ArrayConstructor {
    new (length: number): Float32Array;
    new (array: Float32Array): Float32Array;
    new (array: number[]): Float32Array;
    new (buffer: ArrayBuffer, byteOffset?: number, length?: number): Float32Array;

    /**
      * The size in bytes of each element in the array.
      */

    /**
      * Returns a new array from a set of elements.
      * @param items A set of elements to include in the new array object.
      */
    of(...items: number[]): Float32Array;

    /**
      * Creates an array from an array-like or iterable object.
      * @param arrayLike An array-like or iterable object to convert to an array.
      * @param mapfn A mapping function to call on every element of the array.
      * @param thisArg Value of 'this' used to invoke the mapfn.
      */
    from(arrayLike: ArrayLike<number> | Iterable<number>, mapfn?: (v: number, k: number) => number, thisArg?: any): Float32Array;
}
declare var Float32Array: Float32ArrayConstructor;

/**
  * A typed array of 64-bit float values. The contents are initialized to 0. If the requested
  * number of bytes could not be allocated an exception is raised.
  */
interface Float64Array {
    /**
      * The size in bytes of each element in the array.
      */

    /**
      * The ArrayBuffer instance referenced by the array.
      */

    /**
      * The length in bytes of the array.
      */

    /**
      * The offset in bytes of the array.
      */

    /**
      * Returns the this object after copying a section of the array identified by start and end
      * to the same array starting at position target
      * @param target If target is negative, it is treated as length+target where length is the
      * length of the array.
      * @param start If start is negative, it is treated as length+start. If end is negative, it
      * is treated as length+end.
      * @param end If not specified, length of the this object is used as its default value.
      */
    copyWithin(target: number, start: number, end?: number): Float64Array;

    /**
      * Returns an array of key, value pairs for every entry in the array
      */
    entries(): IterableIterator<[number, number]>;

    /**
      * Determines whether all the members of an array satisfy the specified test.
      * @param callbackfn A function that accepts up to three arguments. The every method calls
      * the callbackfn function for each element in array1 until the callbackfn returns false,
      * or until the end of the array.
      * @param thisArg An object to which the this keyword can refer in the callbackfn function.
      * If thisArg is omitted, undefined is used as the this value.
      */
    every(callbackfn: (value: number, index: number, array: Float64Array) => boolean, thisArg?: any): boolean;

    /**
        * Returns the this object after filling the section identified by start and end with value
        * @param value value to fill array section with
        * @param start index to start filling the array at. If start is negative, it is treated as
        * length+start where length is the length of the array.
        * @param end index to stop filling the array at. If end is negative, it is treated as
        * length+end.
        */
    fill(value: number, start?: number, end?: number): Float64Array;

    /**
      * Returns the elements of an array that meet the condition specified in a callback function.
      * @param callbackfn A function that accepts up to three arguments. The filter method calls
      * the callbackfn function one time for each element in the array.
      * @param thisArg An object to which the this keyword can refer in the callbackfn function.
      * If thisArg is omitted, undefined is used as the this value.
      */
    filter(callbackfn: (value: number, index: number, array: Float64Array) => boolean, thisArg?: any): Float64Array;

    /**
      * Returns the value of the first element in the array where predicate is true, and undefined
      * otherwise.
      * @param predicate find calls predicate once for each element of the array, in ascending
      * order, until it finds one where predicate returns true. If such an element is found, find
      * immediately returns that element value. Otherwise, find returns undefined.
      * @param thisArg If provided, it will be used as the this value for each invocation of
      * predicate. If it is not provided, undefined is used instead.
      */
    find(predicate: (value: number, index: number, obj: Array<number>) => boolean, thisArg?: any): number;

    /**
      * Returns the index of the first element in the array where predicate is true, and undefined
      * otherwise.
      * @param predicate find calls predicate once for each element of the array, in ascending
      * order, until it finds one where predicate returns true. If such an element is found, find
      * immediately returns that element value. Otherwise, find returns undefined.
      * @param thisArg If provided, it will be used as the this value for each invocation of
      * predicate. If it is not provided, undefined is used instead.
      */
    findIndex(predicate: (value: number) => boolean, thisArg?: any): number;

    /**
      * Performs the specified action for each element in an array.
      * @param callbackfn  A function that accepts up to three arguments. forEach calls the
      * callbackfn function one time for each element in the array.
      * @param thisArg  An object to which the this keyword can refer in the callbackfn function.
      * If thisArg is omitted, undefined is used as the this value.
      */
    forEach(callbackfn: (value: number, index: number, array: Float64Array) => void, thisArg?: any): void;

    /**
      * Returns the index of the first occurrence of a value in an array.
      * @param searchElement The value to locate in the array.
      * @param fromIndex The array index at which to begin the search. If fromIndex is omitted, the
      *  search starts at index 0.
      */
    indexOf(searchElement: number, fromIndex?: number): number;

    /**
      * Adds all the elements of an array separated by the specified separator string.
      * @param separator A string used to separate one element of an array from the next in the
      * resulting String. If omitted, the array elements are separated with a comma.
      */
    join(separator?: string): string;

    /**
      * Returns an list of keys in the array
      */
    keys(): IterableIterator<number>;

    /**
      * Returns the index of the last occurrence of a value in an array.
      * @param searchElement The value to locate in the array.
      * @param fromIndex The array index at which to begin the search. If fromIndex is omitted, the
      * search starts at index 0.
      */
    lastIndexOf(searchElement: number, fromIndex?: number): number;

    /**
      * The length of the array.
      */

    /**
      * Calls a defined callback function on each element of an array, and returns an array that
      * contains the results.
      * @param callbackfn A function that accepts up to three arguments. The map method calls the
      * callbackfn function one time for each element in the array.
      * @param thisArg An object to which the this keyword can refer in the callbackfn function.
      * If thisArg is omitted, undefined is used as the this value.
      */
    map(callbackfn: (value: number, index: number, array: Float64Array) => number, thisArg?: any): Float64Array;

    /**
      * Calls the specified callback function for all the elements in an array. The return value of
      * the callback function is the accumulated result, and is provided as an argument in the next
      * call to the callback function.
      * @param callbackfn A function that accepts up to four arguments. The reduce method calls the
      * callbackfn function one time for each element in the array.
      * @param initialValue If initialValue is specified, it is used as the initial value to start
      * the accumulation. The first call to the callbackfn function provides this value as an argument
      * instead of an array value.
      */
    reduce(callbackfn: (previousValue: number, currentValue: number, currentIndex: number, array: Float64Array) => number, initialValue?: number): number;

    /**
      * Calls the specified callback function for all the elements in an array. The return value of
      * the callback function is the accumulated result, and is provided as an argument in the next
      * call to the callback function.
      * @param callbackfn A function that accepts up to four arguments. The reduce method calls the
      * callbackfn function one time for each element in the array.
      * @param initialValue If initialValue is specified, it is used as the initial value to start
      * the accumulation. The first call to the callbackfn function provides this value as an argument
      * instead of an array value.
      */
    reduce<U>(callbackfn: (previousValue: U, currentValue: number, currentIndex: number, array: Float64Array) => U, initialValue: U): U;

    /**
      * Calls the specified callback function for all the elements in an array, in descending order.
      * The return value of the callback function is the accumulated result, and is provided as an
      * argument in the next call to the callback function.
      * @param callbackfn A function that accepts up to four arguments. The reduceRight method calls
      * the callbackfn function one time for each element in the array.
      * @param initialValue If initialValue is specified, it is used as the initial value to start
      * the accumulation. The first call to the callbackfn function provides this value as an
      * argument instead of an array value.
      */
    reduceRight(callbackfn: (previousValue: number, currentValue: number, currentIndex: number, array: Float64Array) => number, initialValue?: number): number;

    /**
      * Calls the specified callback function for all the elements in an array, in descending order.
      * The return value of the callback function is the accumulated result, and is provided as an
      * argument in the next call to the callback function.
      * @param callbackfn A function that accepts up to four arguments. The reduceRight method calls
      * the callbackfn function one time for each element in the array.
      * @param initialValue If initialValue is specified, it is used as the initial value to start
      * the accumulation. The first call to the callbackfn function provides this value as an argument
      * instead of an array value.
      */
    reduceRight<U>(callbackfn: (previousValue: U, currentValue: number, currentIndex: number, array: Float64Array) => U, initialValue: U): U;

    /**
      * Reverses the elements in an Array.
      */
    reverse(): Float64Array;

    /**
      * Sets a value or an array of values.
      * @param index The index of the location to set.
      * @param value The value to set.
      */
    set(index: number, value: number): void;

    /**
      * Sets a value or an array of values.
      * @param array A typed or untyped array of values to set.
      * @param offset The index in the current array at which the values are to be written.
      */
    set(array: Float64Array, offset?: number): void;

    /**
      * Returns a section of an array.
      * @param start The beginning of the specified portion of the array.
      * @param end The end of the specified portion of the array.
      */
    slice(start?: number, end?: number): Float64Array;

    /**
      * Determines whether the specified callback function returns true for any element of an array.
      * @param callbackfn A function that accepts up to three arguments. The some method calls the
      * callbackfn function for each element in array1 until the callbackfn returns true, or until
      * the end of the array.
      * @param thisArg An object to which the this keyword can refer in the callbackfn function.
      * If thisArg is omitted, undefined is used as the this value.
      */
    some(callbackfn: (value: number, index: number, array: Float64Array) => boolean, thisArg?: any): boolean;

    /**
      * Sorts an array.
      * @param compareFn The name of the function used to determine the order of the elements. If
      * omitted, the elements are sorted in ascending, ASCII character order.
      */
    sort(compareFn?: (a: number, b: number) => number): Float64Array;

    /**
      * Gets a new Float64Array view of the ArrayBuffer store for this array, referencing the elements
      * at begin, inclusive, up to end, exclusive.
      * @param begin The index of the beginning of the array.
      * @param end The index of the end of the array.
      */
    subarray(begin: number, end?: number): Float64Array;

    /**
      * Converts a number to a string by using the current locale.
      */
    toLocaleString(): string;

    /**
      * Returns a string representation of an array.
      */
    toString(): string;

    /**
      * Returns an list of values in the array
      */
    values(): IterableIterator<number>;

    //[index: number]: number;
    [Symbol.iterator](): IterableIterator<number>;
}

interface Float64ArrayConstructor {
    new (length: number): Float64Array;
    new (array: Float64Array): Float64Array;
    new (array: number[]): Float64Array;
    new (buffer: ArrayBuffer, byteOffset?: number, length?: number): Float64Array;

    /**
      * The size in bytes of each element in the array.
      */

    /**
      * Returns a new array from a set of elements.
      * @param items A set of elements to include in the new array object.
      */
    of(...items: number[]): Float64Array;

    /**
      * Creates an array from an array-like or iterable object.
      * @param arrayLike An array-like or iterable object to convert to an array.
      * @param mapfn A mapping function to call on every element of the array.
      * @param thisArg Value of 'this' used to invoke the mapfn.
      */
    from(arrayLike: ArrayLike<number> | Iterable<number>, mapfn?: (v: number, k: number) => number, thisArg?: any): Float64Array;
}
declare var Float64Array: Float64ArrayConstructor;

interface ProxyHandler<T> {
    getPrototypeOf?(target: T): any;
    setPrototypeOf?(target: T, v: any): boolean;
    isExtensible?(target: T): boolean;
    preventExtensions?(target: T): boolean;
    getOwnPropertyDescriptor?(target: T, p: PropertyKey): PropertyDescriptor;
    has?(target: T, p: PropertyKey): boolean;
    get?(target: T, p: PropertyKey, receiver: any): any;
    set?(target: T, p: PropertyKey, value: any, receiver: any): boolean;
    deleteProperty?(target: T, p: PropertyKey): boolean;
    defineProperty?(target: T, p: PropertyKey, attributes: PropertyDescriptor): boolean;
    enumerate?(target: T): PropertyKey[];
    ownKeys?(target: T): PropertyKey[];
    apply?(target: T, thisArg: any, argArray?: any): any;
    construct?(target: T, thisArg: any, argArray?: any): any;
}

interface ProxyConstructor {
    revocable<T>(target: T, handler: ProxyHandler<T>): { proxy: T; revoke: () => void; };
    new <T>(target: T, handler: ProxyHandler<T>): T
}
declare var Proxy: ProxyConstructor;

declare module Reflect {
    function apply(target: Function, thisArgument: any, argumentsList: ArrayLike<any>): any;
    function construct(target: Function, argumentsList: ArrayLike<any>): any;
    function defineProperty(target: any, propertyKey: PropertyKey, attributes: PropertyDescriptor): boolean;
    function deleteProperty(target: any, propertyKey: PropertyKey): boolean;
    function enumerate(target: any): IterableIterator<any>;
    function get(target: any, propertyKey: PropertyKey, receiver?: any): any;
    function getOwnPropertyDescriptor(target: any, propertyKey: PropertyKey): PropertyDescriptor;
    function getPrototypeOf(target: any): any;
    function has(target: any, propertyKey: string): boolean;
    function has(target: any, propertyKey: symbol): boolean;
    function isExtensible(target: any): boolean;
    function ownKeys(target: any): Array<PropertyKey>;
    function preventExtensions(target: any): boolean;
    function set(target: any, propertyKey: PropertyKey, value: any, receiver?: any): boolean;
    function setPrototypeOf(target: any, proto: any): boolean;
}

interface ArrayBufferView {
    /**
      * The ArrayBuffer instance referenced by the array.
      */

    /**
      * The length in bytes of the array.
      */

    /**
      * The offset in bytes of the array.
      */
}
