var Chance = require('chance');
var Sqlite = require('sqlite3').verbose();

const types = [
  'text',
  'number',
  'datetime',
  'logical',
  'uuid'
]

const sqliteTypeMap = {
  'text': 'TEXT',
  'number': 'REAL',
  'datetime': 'DATETIME',
  'logical': 'BOOLEAN',
  'uuid': 'CHARACTER(36)'
}

/* Generate a random integer in the range [i,j] */
function randomInt(i, j) {
  let min = Math.ceil(i);
  let max = Math.floor(j);
  return Math.floor(Math.random(Date.now()) * (max - min)) + min;
}

/* Generate a random value based on schema datatype */
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

/* Generates a dataset with width columns and depth records. Returns an
object {schema: [], data: []} */
function generateData(width, depth) {
  console.log(`>> Generating ${depth} rows with ${width} columns`);

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

  return {schema, data};
}

/* Writes a dataset into a sqlite database */
function writeData(dataset, callback) {
  let db = new Sqlite.Database("");
  let startTime = 0;
  let endTime = 0;

  const dbSchema = dataset.schema.map((type, idx) => {
    const columnName = `${type}${idx}`;
    const sqlType = sqliteTypeMap[type];
    return `${columnName} ${sqlType}`;
  }).join(',');

  db.serialize(() => {
    console.log(">> Creating table");
    db.run("PRAGMA temp_store = FILE;")
    db.run(`CREATE TABLE tbl (${dbSchema});`);

    const colVars = dataset.schema.map((x) => {return "?"}).join(',');
    var stmt = db.prepare(`INSERT INTO tbl VALUES (${colVars});`);

    console.log(">> Writing rows");
    endTime = Date.now();
    startTime = Date.now();

    db.run("BEGIN TRANSACTION;");
    for (let i = 0; i < dataset.data.length; ++i) {
      stmt.run(dataset.data[i]);
    }
    db.run("COMMIT;", (err) => {
      endTime = Date.now();
    });

    stmt.finalize();
  });

  db.close((err) => {
    callback({startTime, endTime});
  });
}

/* Runs n trials */
function executeTrial(ntrials, trialnum, results) {
  if (trialnum == null || trialnum < ntrials) {
    // Initial call or still have trials to run

    let thisTrialNum = trialnum || 0;
    let allResults = results || [];
    console.log(`> Running trial ${thisTrialNum}`);

    const dataset = generateData(width, depth);
    writeData(dataset, (metrics) => {
      const elapsedTime = (metrics.endTime - metrics.startTime);
      writeRate = Math.floor(dataset.data.length / (elapsedTime / 1000.0));
      allResults.push(writeRate);
      console.log(`>> Wrote ${dataset.data.length} rows in ${elapsedTime}ms (${writeRate} rows/sec)`);

      executeTrial(ntrials, thisTrialNum + 1, allResults);
    });
  } else {
    // No more trials to run, compute and output results
    const averageRate = results.reduce((rate, sum) => {return sum += rate}) / ntrials;
    console.log(`> Average rate over ${ntrials} trials is ${averageRate} rows/sec`);
  }
}

const args = process.argv.slice(2);
const width = Number.parseInt(args.shift());
const depth = Number.parseInt(args.shift());
const ntrials = Number.parseInt(args.shift());

executeTrial(ntrials);
