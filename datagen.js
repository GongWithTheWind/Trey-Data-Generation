const fs = require('fs');
const faker = require('faker');
const Promise = require('bluebird');
writeFileAsync = Promise.promisify(fs.writeFile);

function generateListing() {
  let listing = {};
  for (let i = 0; i < Math.random() * 15; i++) {
    let randId = Math.floor(Math.random() * 35000);
    listing[`https://loremflickr.com/320/240?lock=${randId}`] = faker.lorem.sentence();
  }
  return listing;
}

function genListingGroupJSON (groupSize) {
  let listingGroup = {}
  for (let i = 0; i < groupSize; i++) {
    listingGroup[`${i}`] = generateListing()
  }
  return JSON.stringify(listingGroup); //This one may be costly: 
}

async function genFile(index) {
  let path = `./data/${index}`;
  fs.writeFile(path, genListingGroupJSON(2000), (err) => {
    if (err) console.log(err);
  }) 
}

async function genFilesParallel (numFiles) {
  for (let i = 0; i < 4751; i += 250) {
    let chunk = [];
    for (let j = 0; j < 250; j++) {
      chunk.push(i + j);
    }
    console.log(i, chunk);
    await Promise.all(chunk.map(num => genFile(num)));
  }
  console.timeEnd('timer');
}
console.time('timer');


    
    // async.parallel(chunk, async (err, results) => {
    //   console.log(err, results);
    // })

    // async.each(chunk, async.ensureAsync(genFile), async function(err, response) {
    //    if(err) console.log(err);
    //    console.log(response);
    //  })

      // await Promise.all(chunk).then((err, result) => {
      //   console.log(`${i} written!`);
      // })

      // await writeFileAsync(`./data/test${i}`, genListingGroupJSON(100000)).then((err, result) => {
      //   console.log(err, result, `${i} written!`)
      // })

// / 1st para in async.each() is the array of items
// async.each(items,
//   // 2nd param is the function that each item is passed to
//   function(item, callback){
//     // Call an asynchronous function, often a save() to DB
//     item.someAsyncCall(function (){
//       // Async call is done, alert via callback
//       callback();
//     });
//   },
//   // 3rd param is the function to call when everything's done
//   function(err){
//     // All tasks are done now
//     doSomethingOnceAllAreDone();
//   }
// );



genFilesParallel();