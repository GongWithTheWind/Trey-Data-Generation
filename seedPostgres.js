const { Client } = require('pg');
const Json2csvParser = require('json2csv').Parser;
const fs = require('fs');
const Promise = require('bluebird');
const fields = ["id", "home_id", "image", "image_id", "caption"];
require('dotenv').config();

const json2csvParser = new Json2csvParser({ fields });
const readFileAsync = Promise.promisify(fs.readFile);
const writeFileAsync = Promise.promisify(fs.writeFile);
const deleteFileAsync = Promise.promisify(fs.unlink);

let ID_NUM = 0;
const client = new Client({
  host: 'localhost',
  database: 'treypurnell',
}) 

const mainTableQuery = 
`CREATE TABLE images (
  id integer NOT NULL,
  home_id integer NOT NULL,
  image varchar(255),
  image_id integer NOT NULL,
  caption character varying)`; //  PARTITION BY RANGE (home_id)

client.connect()

async function createPartitionedTable() {
  await client.query(mainTableQuery);
  // for (let i = 0; i < 10000000; i += 100000) {
  //   rangeStart = i;
  //   rangeEnd = i + 100000;

  //   const partitionQuery = 
  //   `CREATE TABLE images_${rangeStart}_${rangeEnd}
  //   PARTITION OF images (home_id)
  //   FOR VALUES FROM (${rangeStart}) to (${rangeEnd})`;

  //   await client.query(partitionQuery);
  // }
}

async function deletePartitionedTable() {
  await client.query(`DROP TABLE IF EXISTS images`);
  // for (let i = 0; i < 10000000; i += 100000) {
  //   rangeStart = i;
  //   rangeEnd = i + 100000;
  //   const partitionQuery = 
  //   `DROP TABLE IF EXISTS images_${rangeStart}_${rangeEnd}`;
  //   await client.query(partitionQuery)
  // }
}

async function indexPartitionedTables() {
  for (let i = 0; i < 10000000; i += 100000) {
    rangeStart = i;
    rangeEnd = i + 100000;
    const indexQuery = 
    `CREATE INDEX images_id_index_${rangeStart} ON images_${rangeStart}_${rangeEnd} (home_id)`;
    await client.query(indexQuery);
    console.log(`indexed at: ${rangeStart}`)
  }
  console.timeEnd('timer');
}

async function seedFile(fileNum, offset) {
  const csv = await readJSON(fileNum, offset);
  const seedQuery = `COPY images
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
        for (let image in data[listing]) {
          objArr.push({
            "id": ID_NUM++,
            "home_id": parseInt(listing) + offset, 
            "image": image,
            "image_id": parseInt(image.split('=')[1]),
            "caption": data[listing][image]
          })
        }
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
  // indexPartitionedTables();
  console.timeEnd('timer');
}

console.time('timer');
seedDB();