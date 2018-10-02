const { Client } = require('pg');
const Json2csvParser = require('json2csv').Parser;
const fs = require('fs');
const Promise = require('bluebird');
const fields = ["home_id", "images"];
require('dotenv').config();

const json2csvParser = new Json2csvParser({ fields });
const readFileAsync = Promise.promisify(fs.readFile);
const writeFileAsync = Promise.promisify(fs.writeFile);
const deleteFileAsync = Promise.promisify(fs.unlink);

const client = new Client({
  host: 'localhost',
  database: 'treypurnell',
}) 

const mainTableQuery = 
`CREATE TABLE homes (
  home_id integer NOT NULL,
  images jsonb) PARTITION BY RANGE (home_id)`;

client.connect()

async function createPartitionedTable() {
  await client.query(mainTableQuery);
  for (let i = 0; i < 10000000; i += 100000) {
    rangeStart = i;
    rangeEnd = i + 100000;

    const partitionQuery = 
    `CREATE TABLE homes_${rangeStart}_${rangeEnd}
    PARTITION OF homes (PRIMARY KEY (home_id))
    FOR VALUES FROM (${rangeStart}) to (${rangeEnd})`;

    await client.query(partitionQuery);
  }
}

async function deletePartitionedTable() {
  await client.query(`DROP TABLE IF EXISTS homes`);
  for (let i = 0; i < 10000000; i += 100000) {
    rangeStart = i;
    rangeEnd = i + 100000;
    const partitionQuery = 
    `DROP TABLE IF EXISTS homes_${rangeStart}_${rangeEnd}`;

    await client.query(partitionQuery)
  }
}

async function seedFile(fileNum, offset) {
  const csv = await readJSON(fileNum, offset);
  const seedQuery = `COPY homes
  FROM '${__dirname + `/csv/${fileNum}`}' DELIMITER ',' CSV HEADER;` 

  writeFileAsync(`./csv/${fileNum}`, csv)
  .then(async (err) => {
    if(err) console.log(err);
    await client.query(seedQuery);
    return deleteFileAsync(`./csv/${fileNum}`);
  })
  .then((result) => {
  })
  .catch((err) => {
    console.log('failure', err);
  })
}

function readJSON(fileNum, offset) {
  return readFileAsync(`./data/${fileNum}`, "utf8")
    .then((result) => {
      let objArr = [];
      let data = JSON.parse(result);
      for(let listing in data) {
        objArr.push({"home_id": parseInt(listing) + offset, images: data[listing]})
      }
      const csv = json2csvParser.parse(objArr);
      return csv;
    })
    .catch((err) => {
      console.log(err);
    })
}

async function seedDB() {
  await deletePartitionedTable();
  await createPartitionedTable();

  for (let i = 0; i < 100; i++) {
    let chunk = [];
    for (let j = 0; j < 50; j++) {
      chunk.push(j + (i * 50));
    }
    await Promise.all(chunk.map(fileNum => seedFile(fileNum, fileNum * 2000)));
    console.log(`chunked from: ${i * 50} to ${49 + (i * 50)}`);  
  }
  console.timeEnd('timer');
}

console.time('timer');
seedDB();