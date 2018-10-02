const fs = require('fs');
const faker = require('faker');
const Promise = require('bluebird');

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


//This one too, can be abstracted away. 
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

genFilesParallel();