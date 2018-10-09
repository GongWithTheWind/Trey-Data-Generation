const { Client } = require('pg');
const Json2csvParser = require('json2csv').Parser;
const fs = require('fs');
const Promise = require('bluebird');
const fields = ["home_id", "image", "image_id", "caption"];

const json2csvParser = new Json2csvParser({ fields });
const readFileAsync = Promise.promisify(fs.readFile);
const writeFileAsync = Promise.promisify(fs.writeFile);
const deleteFileAsync = Promise.promisify(fs.unlink);
const chmodAsync = Promise.promisify(fs.chmod);

const client = new Client({
  host: process.env.PG_PORT || 'localhost',
  user: process.env.PG_USER || 'postgres',
  password: process.env.PG_PASSWORD || 'soundcloud',
  database: process.env.PG_DB || 'treypurnell',
})

const mainTableQuery = 
`CREATE TABLE images (
  id SERIAL,
  home_id integer NOT NULL,
  image varchar(255),
  image_id integer NOT NULL,
  caption character varying)`;

client.connect()

async function seedFile(fileNum, offset) {
  const csv = await readJSON(fileNum, offset);
  const seedQuery = `COPY images (home_id, image, image_id, caption)
  FROM '${__dirname + `/csv/${fileNum}`}' DELIMITER ',' CSV HEADER;` 

  writeFileAsync(`./csv/${fileNum}`, csv)
  .then(async (err) => {
    if(err) console.log(err);
    await chmodAsync(`./csv/${fileNum}`, '755');
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
  await client.query(`DROP TABLE IF EXISTS images`);
  await client.query(mainTableQuery);

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