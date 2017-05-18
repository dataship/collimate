#! /usr/bin/env node
/* csv parser that produces typed columnar output */
var fs = require('fs'),
	path = require('path'),
	parse = require('csv-parse/lib/sync');

function isnumber(obj){ return Object.prototype.toString.call(obj) === "[object Number]";}
function isinteger(num){ return num % 1 === 0;}

// type scanning strategies:
// constant, percentage, full

// minimal, complete

// data dirtiness levels:
// pristine, clean, dirty, toxic

/* number of rows to scan for determining types */
const MIN_SCAN_COUNT = 1000;
const MIN_SCAN_FRACTION = 0.3;

// map of percentage scanned to percentage ordinals likely encountered
// given large enough N
// 20% -> 80%
// 10% -> 75%
// 4% -> 64%
// 1% -> 50%

// if the number of distinct values in a column is less than this, we consider it ordinal
const ORDINAL_FRACTION = 0.3; // this should probably be logarithmic, instead of linear

// the fraction of all ordinal values we are likely to encounter as a function of
// sample percentage.
const SAMPLING_ENCOUNTER_FRACTION_MAP = {
		"1.0" : 1.0,
		"0.8" : 0.7,
		"0.4" : 0.65,
		"0.2" : 0.6,
		"0.1" : 0.5,
		"0.04" : 0.3,
		"0.01" : 0.1
};

// the exponent on the sampling encounter fraction as a function of
// data disorder, higher disorder should produce larger exponents
//const ESTIMATED_ENCOUNTER_FRACTION = 0.75;
//const ESTIMATED_ENCOUNTER_FRACTION = 0.25; // this is low, to account for semi-structured data
// this number is an educated guess and should really be a mapping
// see this entropy calculation answer for a start:
// http://stackoverflow.com/questions/990477/how-to-calculate-the-entropy-of-a-file
// https://en.wikipedia.org/wiki/Entropy_(information_theory)#Data_as_a_Markov_process
const ENTROPIC_ENCOUNTER_EXPONENT = 2; // inverse of entropy

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

var constructor_map = {
	"int8" : Int8Array,
	"uint8" : Uint8Array,
	"int16" : Int16Array,
	"uint16" : Uint16Array,
	"int32" : Int32Array,
	"uint32" : Uint32Array,
	"float32" : Float32Array,
	"flout64" : Float64Array,
	"ord8" : Int8Array,
	"ord16" : Int16Array
};

var ext_map = {
	"int8" : ".i8",
	"uint8" : ".u8",
	"int16" : ".i16",
	"uint16" : ".u16",
	"int32" : ".i32",
	"uint32" : ".u32",
	"float32" : ".f32",
	"float64" : ".f64",
	"ord8" : ".s8",
	"ord16" : ".s16",
	"str" : ".json"
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
	// we start with the narrowest type that will accommodate the value found
	// int32 -> float32 -> str
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
			if(isinteger(value)){
				if(value <= 2147483647) types[j] = 'int32';
				else types[j] = 'str';
			} else{
				types[j] = 'float32';
			}
		} else {
			// no, try parsing it
			var num = +value;
			if(num === +value){
				if(isinteger(num)){
					if(num <= 2147483647) types[j] = 'int32';
					else types[j] = 'str';
				} else{
					types[j] = 'float32';
				}
			} else if(value in NULL_SET) {
				value = null;
				types[j] = 'int32';
			} else {
				//assume generic string
				types[j] = 'str';
			}
		}

		// initialize the set of distinct values
		distincts[j] = {};
		distincts[j][value] = value;
		counts[j] = 1;
	}

	// how many records should we scan, before deciding on types?
	var scan = N < MIN_SCAN_COUNT ?
		N : MIN_SCAN_COUNT > (N * MIN_SCAN_FRACTION) ? MIN_SCAN_COUNT : (N * MIN_SCAN_FRACTION);

	//console.log(N);
	//console.log(scan);

	// what threshold is ordinal?
	// TODO: figure out how to integrate these concepts for determining ordinality:
	// https://en.wikipedia.org/wiki/Diversity_index#True_diversity
	// https://en.wikipedia.org/wiki/Entropy_(information_theory)#Entropy_as_a_measure_of_diversity
	// http://stackoverflow.com/questions/990477/how-to-calculate-the-entropy-of-a-file
	// https://en.wikipedia.org/wiki/Entropy_(information_theory)#Data_as_a_Markov_process
	var threshold = Math.min(Math.ceil(N * ORDINAL_FRACTION), MAX_ORDINAL);

	// adjust ordinal threshold based on how much of the data we're scanning
	var sample_fraction = scan / N;
	var estimated_encounter_fraction;
	for(fraction in SAMPLING_ENCOUNTER_FRACTION_MAP){
		if (sample_fraction >= +fraction){
			//console.log(+fraction);
			estimated_encounter_fraction = Math.pow(SAMPLING_ENCOUNTER_FRACTION_MAP[fraction], ENTROPIC_ENCOUNTER_EXPONENT);
			break;
		}
	}

	//console.log(sample_fraction);
	//console.log(estimated_encounter_fraction);
	threshold *= estimated_encounter_fraction;
	//console.log(threshold);

	// if it's all integers except for elements which map to the null set,
	// it's int32

	// if it's a mixture of integers and floats except for elements which map to
	// the null set, it's float32

	// if it contain strings that don't map to the null set, it's a string

	// all columns should be checked for ordinality
	// a column is ordinal if it's count of distinct values is less than some
	// threshold percentage of it's length (10%).

	//console.log(types);
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
						if(isinteger(+value)){
							if(num <= 2147483647) types[j] = 'int32';
							else types[j] = 'str';
						} else {
							types[j] = 'float32';
						}
					} else if(value in NULL_SET) {
						value = null;
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
						// parses as float: noop
					} else if(value in NULL_SET) {
						value = null;
					} else {
						//console.log(value);
						types[j] = 'str';
					}
				}
			} else {
				// check for null set membership
				if(value in NULL_SET) {
					value = null;
				}
			}

			// ordinal?
			distinct = distincts[j];
			if(counts[j] <= threshold){
				if(!(value in distinct)){
					distinct[value] = value;
					counts[j] += 1;
				}
			}
		}
	}

	//console.log(counts);

	// create columns
	var columns = {};
	var decoders = {};
	var encoders = {};
	var count;
	for(var j = 0; j < names.length; j++){
		name = names[j];
		type = types[j];

		// is it ordinal?
		count = counts[j];
		if(count <= threshold){
			// yes, encode
			var distinct = distincts[j];
			var decoder = [];
			var encoder = {};
			var k = 0;
			for(var s in distinct){
				// map integer to distinct value
				// use distinct[s] (instead of s) to get actual (non-string) value
				if(types[j] == 'str') decoder[k] = distinct[s];
				else decoder[k] = +distinct[s];
				// map distinct value to integer
				encoder[distinct[s]] = k;
				k++;
			}

			decoders[name] = decoder;
			encoders[name] = encoder;

			// 8 bit?
			if(count <= (256 * estimated_encounter_fraction)){
				// yes
				columns[name] = new Uint8Array(N);
				types[j] = 'ord8';
			} else {
				// no, use 16 bit encoding
				columns[name] = new Uint16Array(N);
				types[j] = 'ord16';
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
			count = counts[name];
			column = columns[name];

			// TODO: null interplay with ordinal?
			//if(value in NULL_SET) value = null;

			// ordinal?
			if(name in encoders){
				encoder = encoders[name];
				// is it a null representation?
				if(value in NULL_SET) value = null;

				// value already encountered?
				if(value in encoder){
					// yes, retrieve it
					encoded = encoder[value];
				} else {
					// no, we need to add it to the set of possible values

					// does it expand us beyond our allotted encoding capacity?
					if(count > 256 && type == "ord8"){
						// yes, expand 8 bit encoding to 16 bit
						console.error("alloted encoding size for ordinal exceeded: ord8.");
						console.error("reallocating as ord16.");
						columns[name] = new Uint16Array(column);
						types[j] = "ord16";
					} else if (count > 65536 && type == "ord16"){
						// yes, 16 bit isn't big enough
						console.error("maximum encoding size for ordinal exceeded: ord16.");
						console.error("data loss may occur.");
						// TODO: do something useful?
					}

					decoder = decoders[name];
					encoded = decoder.length;
					decoder.push(value);
					encoder[value] = encoded;

				}
				column[i] = encoded;
			} else {

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

				column[i] = value;
			}

		}
	}

	type_map = {}
	for(var j = 0; j < names.length; j++) type_map[names[j]] = types[j];
	return {"columns" : columns, "keys" : decoders, "types" : type_map};
}

// sanitize column names
function sanitize(str){

	var sane = str.toLowerCase();
	sane = sane.replace(/(^\W+)|(\W+$)/g, '');
	sane = sane.replace(/&/g, 'and');
	sane = sane.replace(/@/g, 'at');
	sane = sane.replace(/%/g, 'percent');
	sane = sane.replace(/-/g, '_');
	sane = sane.replace(/\W+/g, '_');

	return sane;
}


// called directly?
if(require.main === module){
	/* potential options:
		scan options: constant, percentage, full OR minimal, complete

		min scan percentage: minimal percentage to scan
		min scan length: minimal count to scan

		recognize and normalize to date-times to ISO standard

		data dirtiness levels: pristine, clean, dirty, toxic

	 */
	// yes, parse command line args and show something
	var argv = require('yargs')
		.usage('Convert a CSV into typed columns\nUsage: $0 [options] <file>')
		.demand(1)
		.help('h').alias('h', 'help')
		.argv

	var fpath = argv._[0];

	var text = fs.readFileSync(fpath);
	var rows = parse(text, {delimiter: ',', columns:true, trim:true, auto_parse:false});

	var result = collimate(rows);
	//console.log(Object.keys(result.columns));
	//console.log(Object.keys(result.keys));
	//console.log(result.types);
	/*
	for(key in result.keys){
		var vals = result.keys[key];
		console.log(vals.slice(0, 10));
	}*/
	// write columns

	// create directory
	var fext = path.extname(fpath);
	var fname = path.basename(fpath, fext);

	if (!fs.existsSync(fname)){
		console.log("creating directory " + fname);
		fs.mkdirSync(fname);
	}


	// write files
	var dir = fname + "/";
	var ext;
	var sane_name;
	for(var name in result.columns){

		// write columns to file
		sane_name = sanitize(name);

		ext = ext_map[result.types[name]];
		console.log("writing file: " + dir + sane_name + ext);
		if(ext == ".json"){
			fs.writeFileSync(dir + sane_name + ext, JSON.stringify(result.columns[name], null, 1));
		} else {
			fs.writeFileSync(dir + sane_name + ext, new Buffer(result.columns[name].buffer));
		}

		// do we need to write a key file?
		if(name in result.keys){
			// yes
			fs.writeFileSync(dir + sane_name + ".key", JSON.stringify(result.keys[name], null, 1));
		}
	}
}
