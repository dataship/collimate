#! /usr/bin/env node
/* csv parser that produces typed columnar output */
var fs = require('fs'),
	path = require('path'),
	moment = require('moment'),
	parse = require('csv-parse/lib/sync');

function isarray(obj){ return Object.prototype.toString.call(obj) === "[object Array]"; }
function isnumber(obj){ return Object.prototype.toString.call(obj) === "[object Number]"; }
function isinteger(num){ return num % 1 === 0; }
function type(obj){ return Object.prototype.toString.call(obj).slice(8, -1); }

// type scanning strategies:
// constant, percentage, full

// minimal, complete

// data dirtiness levels:
// pristine, clean, dirty, toxic

/* number of rows to scan for determining types */
const MIN_SCAN_COUNT = 1000;
const MIN_SCAN_FRACTION = 0.3;

// map of percentage scanned to percentage categoricals likely encountered
// given large enough N
// 20% -> 80%
// 10% -> 75%
// 4% -> 64%
// 1% -> 50%

// if the number of distinct values in a column is less than this, we consider it categorical
const CATEGORICAL_FRACTION = 0.3; // this should probably be logarithmic, instead of linear

// the fraction of all categorical values we are likely to encounter as a function of
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

const MAX_CATEGORICAL = 65536;

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
	"float64" : Float64Array
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
	"str" : ".json"
};

var ISO_DATE = "YYYY-MM-DD HH:mm:ss.SSSZZ"; // ISO-8601

/* parse date based on
	* is it a string?
	* is the length of that string in the right range?
	* does it parse exactly with the format(s) of matching length?
	* is the format consistent across all samples?
 */
// moment
// all lengths are 8 - 10
var DATE_FORMATS = [
	"YYYY-M-D", // ISO
	"YYYY/M/D",

	"D-M-YYYY", // most common global format
	"D/M/YYYY",

	"M-D-YYYY", // u.s.
	"M/D/YYYY"
];

var TIME_FORMATS = [
	"HH:mm", // 5

	"hh:mm A", // 8
	"hh:mmA", // 7

	"HH:mm:ss", // 8

	"hh:mm:ss A", // 11
	"hh:mm:ssA", // 10

	"HH:mm:ss.S", // 10
	"HH:mm:ss.SS", // 11
	"HH:mm:ss.SSS", // 12
];

var VALID_DATE_LENGTHS = [5, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22];


function collimate(rows, parse_dates, verbose){

	if (rows == null || rows.length == 0) return { "columns" : {}, "keys" : {}, "types" : []};

	var N = rows.length;
	var row = rows[0];

	var t0;
	if(argv.v){
		process.stdout.write("Determining types... ");
		t0 = Date.now();
	}
	// how many columns?
	// what are their names?
	var names = Object.keys(row);

	// what are their types?
	// int8,int16,int32, float32,float64, str8,str16, json
	// are any of them categorical?
	// do the categorical columns have NULLS?

	var types = [];
	var distincts = [];
	var counts = [];
	var date_matches = {};
	// guess types from first row
	// we start with the narrowest type that will accommodate the value found
	// int32 -> float32 -> str
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
			} else if(value.length >= 8 && value.length <= 10){
				// will it parse as a date?
				var m, format;
				for(var k = 0; k < DATE_FORMATS.length; k++){
					format = DATE_FORMATS[k];
					m = moment(value, format, true);
					// did it parse with the given format?
					if(m.isValid()){
						// yes, record it
						if(!date_matches[name]) date_matches[name] = [];

						date_matches[name].push(format);
					}
				}
				/*
				if(name in date_matches)
					console.log("initial match of " + date_matches[name].length + " formats for " + name);
					*/

				types[j] = 'str';
			} else {
				//assume generic string
				types[j] = 'str';
			}
		}

		// initialize the set of distinct values
		distincts[j] = {};
		distincts[j][value] = 0;
		counts[j] = 1;
	}

	// how many records should we scan, before deciding on types?
	var scan = N < MIN_SCAN_COUNT ?
		N : MIN_SCAN_COUNT > (N * MIN_SCAN_FRACTION) ? MIN_SCAN_COUNT : (N * MIN_SCAN_FRACTION);

	//console.log(N);
	//console.log(scan);

	// what threshold is categorical?
	// TODO: figure out how to integrate these concepts for determining categorical:
	// https://en.wikipedia.org/wiki/Diversity_index#True_diversity
	// https://en.wikipedia.org/wiki/Entropy_(information_theory)#Entropy_as_a_measure_of_diversity
	// http://stackoverflow.com/questions/990477/how-to-calculate-the-entropy-of-a-file
	// https://en.wikipedia.org/wiki/Entropy_(information_theory)#Data_as_a_Markov_process
	var threshold = Math.min(Math.ceil(N * CATEGORICAL_FRACTION), MAX_CATEGORICAL);

	// adjust categorical threshold based on how much of the data we're scanning
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

	// all columns should be checked for categoricalness
	// a column is categorical if it's count of distinct values is less than some
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
				} else if(value.length >= 8 && value.length <= 10 && date_matches[name] != null){
					// yes, will it parse as a date?
					var m, matched_formats, format;
					matched_formats = date_matches[name];
					delete date_matches[name];
					for(var k = 0; k < matched_formats.length; k++){
						format = matched_formats[k];
						m = moment(value, format, true);
						// did it parse with the given format?
						if(m.isValid()){
							// yes, persist it
							if(!date_matches[name]) date_matches[name] = [];

							date_matches[name].push(format);
						}
					}
				}
			}

			// is the count for this field below the categorical threshold?
			distinct = distincts[j];
			if(counts[j] <= threshold){
				// yes, have we seen the current value?
				if(!(value in distinct)){
					// no, add and update count
					distinct[value] = counts[j];
					counts[j] += 1;
				}
			}
		}
	}
	if(argv.v) console.log("done! ("+ (Date.now() - t0)+" ms)");

	if(argv.v){
		process.stdout.write("Creating columns... ");
		t0 = Date.now();
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
		/*
		if(name in date_matches){
			console.log("matched " + date_matches[name].length + " date formats for " + name);
		} */

		// is this field categorical?
		count = counts[j];
		if(count <= threshold){
			// yes, encode and set up decoder
			var encoder = distincts[j];
			var decoder = new Array(counts[j]);
			var k;
			for(var s in encoder){
				// get order encountered in data set
				k = encoder[s];

				// parse as number if numeric type
				if(types[j] == 'str'){
					// single valid date format?
					if(name in date_matches && date_matches[name].length == 1 && parse_dates){
						var format = date_matches[name][0];
						var m = moment(s, format, true);
						var normalized_length = 10;
						decoder[k] = m.format(ISO_DATE.slice(0, normalized_length));
					} else {
						decoder[k] = s;
					}
				} else {
					decoder[k] = +s;
				}
			}

			decoders[name] = decoder;
			encoders[name] = encoder;

			// 8 bit?
			if(count <= (256 * estimated_encounter_fraction)){
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
			count = counts[name];
			column = columns[name];

			// categorical?
			if(name in encoders){
				// yes
				encoder = encoders[name];
				// is it a null representation?
				if(value in NULL_SET) value = null;

				// value already encountered?
				if(value in encoder){
					// yes, retrieve it
					encoded = encoder[value];
				} else {
					// no, add it to the set of possible values

					// will it expand the set beyond current encoding capacity?
					if(count == 256 && type(column) == "Uint8Array"){
						// yes, expand 8 bit encoding to 16 bit
						console.error("alloted encoding size for categorical exceeded (8-bit): " + name);
						console.error("reallocating as 16-bit.");
						columns[name] = new Uint16Array(column);
					} else if (count == 65536 && type(column) == "Uint16Array"){
						// yes, 16 bit isn't big enough
						console.error("maximum encoding size for categorical exceeded (16-bit): " + name);
						console.error("data loss may occur.");
						// TODO: do something useful?
					}

					decoder = decoders[name];
					encoded = decoder.length;
					if(type == 'str') {
						// single valid date format?
						if(name in date_matches && date_matches[name].length == 1 && parse_dates){
							var format = date_matches[name][0];
							var m = moment(value, format, true);
							var normalized_length = 10;
							decoder.push(m.format(ISO_DATE.slice(0, normalized_length)));
						} else {
							decoder.push(value);
						}
					} else{
						decoder.push(+value);
					}
					encoder[value] = encoded;
					counts[name]++;

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
				} else if(name in date_matches && date_matches[name].length == 1 && parse_dates){
					var format = date_matches[name][0];
					var m = moment(value, format, true);
					var normalized_length = 10;
					value = m.format(ISO_DATE.slice(0, normalized_length));
				}

				column[i] = value;
			}

		}
	}
	if(argv.v) console.log("done! ("+ (Date.now() - t0)+" ms)");

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

function stringify(obj, type){
	if(type === 'str'){
		return "[" + obj.map(function(str){ return '"'+str+'"'}).join(',\n ') +"]\n";

	}
	return "[" + obj.join(",\n ") +"]\n";
	//return JSON.stringify(obj, null, 1);
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
		.boolean('d').alias('d', 'date')
		.describe('d', 'auto-detect dates and normalize to ISO 8601')
		.boolean('v').describe('v', "print information about what we're doing")
		.help('h').alias('h', 'help')
		.argv

	var t0;
	var fpath = argv._[0];

	if(argv.v){
		process.stdout.write("Parsing CSV... ");
		t0 = Date.now();
	}
	var text = fs.readFileSync(fpath);

	var rows = parse(text, {delimiter: ',', columns:true, trim:true, auto_parse:false});
	if(argv.v) console.log("done! ("+ (Date.now() - t0)+" ms)");

	var result = collimate(rows, argv.d, argv.v);
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

	if(argv.v){
		process.stdout.write("Writing files... ");
		t0 = Date.now();
	}
	if (!fs.existsSync(fname)){
		//console.log("creating directory " + fname);
		fs.mkdirSync(fname);
	}


	// write files
	var dir = fname + "/";
	var ext, column;
	var sane_name;
	for(var name in result.columns){

		// write columns to file
		sane_name = sanitize(name);
		column = result.columns[name];

		// is this a categorical column?
		if(name in result.keys){
			var decoder = result.keys[name];
			// yes
			fs.writeFileSync(dir + sane_name + ".key", stringify(decoder, result.types[name]));

			ext = type(column) == "Uint8Array" ? ".s8" : ".s16";

			fs.writeFileSync(dir + sane_name + ext, new Buffer(column.buffer));
		} else {
			ext = ext_map[result.types[name]];
			//console.log("writing file: " + dir + sane_name + ext);
			//process.stdout.write(".");
			if(ext == ".json"){
				fs.writeFileSync(dir + sane_name + ext, stringify(column, result.types[name]));
			} else {
				fs.writeFileSync(dir + sane_name + ext, new Buffer(column.buffer));
			}
		}

	}
	if(argv.v) console.log("done! ("+ (Date.now() - t0)+" ms)");
}
