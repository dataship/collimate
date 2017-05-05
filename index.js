/* csv parser that produces typed columnar output */
var fs = require('fs'),
	parse = require('csv-parse/lib/sync');

// type scanning strategies:
// constant, percentage, full

// minimal, complete

// data dirtiness levels:
// pristine, clean, dirty, toxic

/* number of rows to scan for determining types */
const SCAN_RANGE = 100;
// if the number of distinct values in a column is less than this, we consider it ordinal
const ORDINAL_FRACTION = 0.1;
// the fraction of all ordinal values we are likely to encounter in our scan
const ESTIMATED_ENCOUNTER_FRACTION = 0.75;

const MAX_ORDINAL = 65536;

// values to consider as null
const NULL_SET = {
	"null" : "null",
	"na" : "na",
	"n/a" : "n/a",
	"none" : "none",
	"" : "",
	"-" : "-"
};

var text = fs.readFileSync(path);
var rows = parse(text, {delimiter: ',', columns:true, trim:true, auto_parse:true});

var constructor_map = {
	"int8" : Int8Array,
	"uint8" : Uint8Array,
	"int16" : Int16Array,
	"uint16" : Uint16Array,
	"int32" : Int32Array,
	"uint32" : Uint32Array,
	"float32" : Float32Array,
	"flout64" : Float64Array,
	"str8" : Int8Array,
	"str16" : Int16Array
};

function collimate(rows){

	if (rows == null || rows.length == 0) return { "columns" : {}, "keys" : {}, "types" : []};

	var N = rows.length;
	var row = rows[0];

	// how many columns?
	// what are their names?
	var names = Object.keys(row);

	// what are their types?
	// int8,int16,int32, float32,float64, str8,str16, json
	// are any of them ordinal?
	// do the ordinal columns have NULLS?

	// guess types from first row
	// we start with the widest type that will accommodate the value found
	var types = [];
	var distincts = [];
	var counts = [];
	var name, value;
	for(var j = 0; j < names.length; j++){
		name = names[j];
		value = row[name];

		// is the value a number?
		if(isnumber(value)){
			// yes, refine it
			if(isinteger(value)) types[j] = 'int32';
			else types[j] = 'float32';
		} else {
			// no, try parsing it
			if(+value === +value){
				value = +value;
				if(isinteger(value)) type[j] = 'int32';
				else types[j] = 'float32';
			} else if(value in NULL_SET) {
				value = "null";
				types[j] = 'int32';
			} else {
				//assume generic string
				types[j] = 'str';
			}
		}

		// initialized the set of distinct values
		distincts[j] = {value : value};
		counts[j] = 1;
	}

	var scan = SCAN_RANGE < N ? SCAN_RANGE : N;
	var threshold = Math.ceil(N * ORDINAL_FRACTION);
	threshold = threshold < (MAX_ORDINAL * ESTIMATED_ENCOUNTER_FRACTION) ?
		threshold : (MAX_ORDINAL * ESTIMATED_ENCOUNTER_FRACTION);

	// if it's all integers except for elements which map to the null set,
	// it's int32

	// if it's a mixture of integers and floats except for elements which map to
	// the null set, it's float32
	// if it contain strings that don't map to the null set, it's a string

	// all columns should be checked for ordinality
	// a column is ordinal if it's count of distinct values is less than some
	// threshold percentage of it's length (10%).

	/* refine types */
	var row, name, type;
	for(var i = 0; i < scan; i++){
		row = rows[i];
		for(var j = 0; j < names.length; j++){
			type = types[j];
			name = names[j];
			value = row[name];
			// is the type currently int?
			if(type === "int32"){
				// yes, check for deviations from int
				if(!isnumber(value)){
					if(+value === +value) {
						value = +value;
						if(isinteger(value)) type[j] = 'int32';
						else types[j] = 'float32';
					} else if(value in NULL_SET) {
						value = "null";
					} else {
						types[j] = 'str';
					}
				} else if (!isinteger(value)){
					// reduce type to float
					types[j] = "float32";
				}
			} else if (type === "float32") {
				// float? check for deviations from number
				if(!isnumber(value)){
					if(+value === +value) {
						value = +value;
					} else if(value in NULL_SET) {
						value = "null";
					} else {
						types[j] = 'str';
					}
				}
			} else {
				// check for null set membership
				if(value in NULL_SET) {
					value = "null";
				}
			}

			// ordinal?
			distinct = distincts[j];
			if(counts[j] < threshold){
				if(!(value in distinct)){
					distinct[value] = value;
					counts[j] += 1;
				}
			}
		}
	}

	// create columns
	var columns = {};
	var keys = {};
	var encoders = {};
	var counts;
	for(var j = 0; j < names.length; j++){
		name = names[j];
		type = types[j];

		// is it ordinal?
		count = counts[j];
		if(count <= threshold){
			// yes, encode
			var key = Object.keys(distincts[name]);
			var encoder = {};
			for(var k = 0; k < key; i++){
				encoder[key[k]] = k;
			}

			keys[name] = key;
			encoders[name] = encoder;

			// 8 bit?
			if(count <= (256 * ESTIMATED_ENCOUNTER_FRACTION){
				// yes
				columns[name] = new Uint8Array(N);
			} else {
				// no, use 16 bit encoding
				columns[name] = new Uint16Array(N);
			}
		} else if(type == "str"){
			// no, it's untyped
			columns[name] = new Array(N);
		} else {
			// no, construct typed array
			type_constructor = constructor_map[type];
			columns[name] = new type_constructor(N);
		}
	}

	// fill columns
	var column;
	for(var i = 0; i < N; i++){
		row = rows[i];
		for(var j = 0; j < names.length; j++){
			type = types[j];
			name = names[j];
			value = row[name];
			columns = columns[name];

			if(type == "int32"){
				if(+value === +value)
					value = +value;
				else
					value = 0;
			} else if(type == "float32"){
				if(+value === +value)
					value = +value;
				else
					value = NaN;
			}

			// ordinal?
			if(name in inv_keys){
				encoder = inv_keys[name];
				encoded = encoder[value];
				column[i] = encoded;
			} else {
				column[i] = value;
			}

		}
	}

	return {"columns" : columns, "keys" : keys, "types" : types};
}

// sanitize column names

// write columns
