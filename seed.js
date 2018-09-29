const cassandra = require('cassandra-driver');
const async = require('async');
const fs = require('fs');
const client = new cassandra.Client({contactPoints: ['127.0.0.1:9042']});
const _ = require('underscore');
const Promise = require('bluebird');
// const eachAsync = Promise.promisify(_.each);
const forEachAsync = Promise.promisify(async.forEachOf);
console.log(forEachAsync);

const readFileAsync = Promise.promisify(fs.readFile);
//TODO after interview - research and test a way of inserting multiple columns where necessary.

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
    seedDB();
  })
  .then((result) => {
    // client.shutdown();
    // console.log('success');
    // console.log(result);
  })
  .catch((err) => {
    // client.shutdown();
    console.log('failure');
    console.log(err);
  })

async function seedDB (factor /* TODO - Refactor to take factor as ENV variable */) {
  for (let i = 0; i < 100; i++) {
    let chunk = [];
    for (let j = 0; j < 50; j++) {
      chunk.push(j + (i * 50)); //REMEMBER TO REFACTOR THE MATHEMATICALS HERE
    }
    console.log('yoooo');
    await Promise.all(chunk.map(fileNum => seedFile(fileNum)))
    // .then((response) => {
    //   console.log('promise completed at: ', i)
    // })
    // .catch((err) => {
    //   console.log(err);
    // })
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
      // console.log(id, home_id, fileNum);
      var homeValues = [id, data[home_id]];
      await client.execute(homeQuery, homeValues, {prepare: true})
        .then((response) => {
          // console.log('successfully seeded at: ', id, home_id, fileNum);
        })
        .catch((err) => {
          console.log(err);
        });
      }
    })
} 