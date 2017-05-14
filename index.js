var Chance = require('chance');

const types = [
  'text',
  'number',
  'datetime',
  'logical',
  'uuid'
]

/* Generate a random integer in the range [i,j] */
function randomInt(i, j) {
  let min = Math.ceil(i);
  let max = Math.floor(j);
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function generateCell(type) {
  const chance = new Chance();

  switch(type) {
    case 'text':
      return chance.word();
    case 'number':
      return Math.random() * 10000;
    case 'datetime':
      return chance.timestamp();
    case 'logical':
      return chance.bool();
    case 'uuid':
      return chance.guid();
    }
}

/* Generates a dataset with width columns and depth records */
function generateData(width, depth) {
  // Generate schema with an _id and "width" datatypes
  let schema = ['uuid'];
  const numTypes = types.length;
  for (let i = 0; i < width; ++i) {
    schema.push(types[randomInt(0,numTypes)]);
  }

  // Generate "depth" records using the schema
  let data = [];
  for (let i = 0; i < depth; ++i) {
    let row = schema.map((type) => generateCell(type));
    data.push(row);
  }

  return data;
}

const args = process.argv.slice(2);
const width = Number.parseInt(args.shift());
const depth = Number.parseInt(args.shift());

console.log(`Generating ${depth} rows with ${width} columns`);
const data = generateData(width, depth);
