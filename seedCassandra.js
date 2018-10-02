const cassandra = require('cassandra-driver');
const async = require('async');
const fs = require('fs');
const Promise = require('bluebird');

const client = new cassandra.Client({contactPoints: ['127.0.0.1:9042']});
const readFileAsync = Promise.promisify(fs.readFile);

console.time('timer');

client.execute("DROP TABLE IF EXISTS demo.homes")
  .then((result) => {
    return client.execute("DROP KEYSPACE IF EXISTS demo");
  })
  .then((result) => {
    return client.execute("CREATE KEYSPACE demo with replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
  })
  .then((result) => {
    return client.execute("CREATE TABLE demo.homes (home_id int PRIMARY KEY, image map<text, text>)");
  })
  .then((result) => {
    return seedDB();
  })
  .then((result) => {
    client.shutdown();
    console.timeEnd('timer')
    console.log('success');
    console.log(result);
  })
  .catch((err) => {
    client.shutdown();
    console.log('failure');
    console.log(err);
  })

async function seedDB (factor /* TODO - Refactor to take factor as ENV variable */) {
  for (let i = 0; i < 100; i++) {
    let chunk = [];
    for (let j = 0; j < 50; j++) {
      chunk.push(j + (i * 50));
    }
    await Promise.all(chunk.map(fileNum => seedFile(fileNum)))
    console.log(`chunked from: ${i * 50} to ${49 + (i * 50)}`);
  }
}

function seedFile (fileNum) {
  return readFileAsync(`./data/${fileNum}`, "utf8")
  .then(async (result) => {
    let data = JSON.parse(result);
    for (let home_id in data) {
      var homeQuery = `INSERT INTO demo.homes (home_id, image) values (?, ?)`;
      var id = parseInt(home_id) + (fileNum * 2000);
      var homeValues = [id, data[home_id]];
      
      await client.execute(homeQuery, homeValues, {prepare: true})
        .then((response) => {
        })
        .catch((err) => {
          console.log(err);
        });
      }
    })
} 