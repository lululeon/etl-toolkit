#!/usr/bin/env node
const etlconf = require('../.etlconf.js');
const commandLineArgs = require('command-line-args'); //TODO: replace with commander
const commandLineUsage = require('command-line-usage');
//const w = require('../src/utils/logger'); //winston logger config
const unzipper = require('unzipper');
const fs = require('fs');
const stream = require('stream');
const path = require('path');
const cron = require('node-cron');
const moment = require('moment');
// cos fetch api isn't actually impld in node
// main differences from vanilla, client-side fetch:
// - client-side concerns like cookies, Cross-Origin, Content Security Policy, Mixed Content, Service Workers ignored.
// - res.body is a Node.js Readable stream, and req.body can be too, if u like.
// - res.text(), res.json(), res.blob(), res.arraybuffer(), res.buffer() only.
// - richer error-handling.
const fetch = require('node-fetch');
const storage = require('@google-cloud/storage')(etlconf.gcpConf);
const ftype = require('./utils/fileTyping');
//const crc32 = require('fast-crc32c'); //for resumable uploads
//const GoogleCloudPlatform = require('./utils/gcp'); //not used yet

function filenameTimestamp() {
  return moment().format().slice(0, -6).replace(/T(.+):(.+):(.+)/gi, '\_$1-$2-$3');
}

function pull() {
  //determine dataSink - TODO: config sanity-checks
  const timestamp = filenameTimestamp();
  const dataSink = path.join(__dirname, '..', 'localstore', path.sep, 'pull-to', etlconf.pull.dataSinkName + '-' + timestamp + '.' + etlconf.pull.dataSinkSuffix);
  console.log(dataSink);

  //fetch data (@/localstore presumed writeable!)
  const wstream = fs.createWriteStream(dataSink, {flags: 'wx', encoding: 'utf-8', mode: 0666});
  wstream.on('error', err => {
    console.log('!!!!!!!!!! error creating file for writestream !!!!! Error:', err);
  });
  wstream.on('open', () =>  { //you have to wait for the write stream b4 proceeding else file not guaranteed to exist by when you need it.
    console.log('write stream ready. Beginning Fetch...');
    fetch(etlconf.pull.dataSourceUrl).then( res => {
      console.log(res.headers.raw);
      return new Promise((resolve, reject) => {
        res.body.pipe(wstream);
        res.body.on('error', err => {
          reject(err);
        });
        wstream.on('finish', () => {
          resolve(dataSink);
        });
        wstream.on('error', err => {
          reject(err);
        });
      });
    })
    .then( filepath => {
      console.log(`fetch complete - data written to [${filepath}]`);
    })
    .catch( err => {
      console.log(`Failed to fetch with error type ${err.type} and message ${err.message}`);
    });
  });
}

function push() {
  return new Promise((resolve, reject) => {
    //const gcp = new GoogleCloudPlatform(storage);
    const dataSourcePath = path.join(__dirname, '..', 'localstore', path.sep, 'push-from', etlconf.push.dataPushFile);
    const timestamp = filenameTimestamp();
    const dataSinkName = `${etlconf.push.datasetName}-${timestamp}.${etlconf.push.targetFileExt}`;
    console.log(`pushing [${dataSourcePath}] to bucket [${etlconf.push.bucket}/${dataSinkName}]`);
    const fileType = ftype.getFileTypeFromName(etlconf.push.dataPushFile);
    const rstream = fs.createReadStream(dataSourcePath);
    const theBucket = storage.bucket(etlconf.push.bucket);
    const wstream = theBucket.file(dataSinkName).createWriteStream({validation:'crc32c'});
    console.log('********** getting ready to stream ************');
    if (fileType === 'zip') {
      console.log('********** unzipping ************');
      rstream.pipe(unzipper.Parse())
      .on('entry', entry => {
        const fileName = entry.path;
        const fileType = entry.type; // 'Directory' or 'File'
        const fileSize = entry.size;
        console.log('processing ', fileType, ' - ', fileName, 'with size', fileSize, '\r');
        //entry.pipe(process.stdout);
        entry.pipe(wstream);
      });
    } else {
      //could disambig further types here..
      console.log('********** uploading as-is... no unzip ************');
      rstream.pipe(wstream);
    }
    wstream.on('error', (err) => {
      return reject(err);
    });
    wstream.on('finish', () => {
      console.log("Uploaded successfully!!");
      wstream.getMetadata()
      .then(metadata => {
          console.log("File's metadata: ", metadata);
          return resolve(metadata.mediaLink);
      })
      .catch(err => {
        console.log("Error getting metadata from uploaded file:", err);
        return reject(err);
      });
    });
  });
}


/*
// fetch('https://www.kaggle.com/kiva/data-science-for-good-kiva-crowdfunding/downloads/kiva_loans.csv/5', {method: 'HEAD'})
// .then( res => {
//   console.log(res.headers.raw);
// });


//simulate data changing at some interval
// cron.schedule('* * * * *', function(){
//   console.log('running a task every minute');
// });
*/



function main() {
  // info about the commandline for this server.
  // command-line-arg entries must have name at minimum
  const optionList = [
    {
      name: 'help',
      type: Boolean,
      description: 'Print this usage guide / help.'
    },
    {
      name: 'pull',
      type: Boolean,
      description: 'pull from pre-defined data source'
    },
    {
      name: 'push',
      type: Boolean,
      description: 'push from localstore to predefined bucket storage in cloud'
    }
  ];

  // command-line-usage entries must be {name, header,content|optionList}
  const optionDefinitions = [
    {
      name: 'ETL toolkit',
      header: 'ETL toolkit',
      content: 'Very basic start to an etl pipeline.'
    },
    {
      name: 'Options',
      header: 'Options',
      optionList: optionList
    }
  ];
  
  const usage = commandLineUsage(optionDefinitions);
  const options = commandLineArgs(optionDefinitions[1].optionList);

  if (options.help) {
    console.log(usage);
    return;
  } else {
    //set commandline overrides
    if(options.pull) {
      pull();
    } else if (options.push) {
      push()
      .then(meta => {
        console.log('DONE!!', meta);
      })
      .catch(err => {
        console.log('sigh. oh dear...');
      })
    }
  }
}

main();