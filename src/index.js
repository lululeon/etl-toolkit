#!/usr/bin/env node
const commandLineArgs = require('command-line-args'); //TODO: replace with commander
const commandLineUsage = require('command-line-usage');
const exec = require('child_process').exec;
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
const etlconf = require('../.etlconf.js');
const storage = require('@google-cloud/storage')(etlconf.gcpConf);
const ftype = require('./utils/fileTyping');
//const crc32 = require('fast-crc32c'); //for resumable uploads... you also need a recent npm global install of node-gyp

function filenameTimestamp() {
  return moment().format().slice(0, -6).replace(/T(.+):(.+):(.+)/gi, '\_$1-$2-$3');
}

function pull() {
  return new Promise((resolve, reject) => {
    //determine dataSink - TODO: config sanity-checks
    const timestamp = filenameTimestamp();
    const dataSink = path.join(__dirname, '..', 'localstore', path.sep, 'pull-to', etlconf.pull.dataSinkName + '-' + timestamp + '.' + etlconf.pull.dataSinkSuffix);

    //fetch data (@/localstore presumed writeable!)
    const wstream = fs.createWriteStream(dataSink, {flags: 'wx', encoding: 'utf-8', mode: 0666});
    wstream.on('error', err => {
      console.log('!!!!!!!!!! error creating file for writestream !!!!! Error:');
      reject(err);
    });
    wstream.on('open', () =>  { //you have to wait for the write stream b4 proceeding else file not guaranteed to exist by when you need it.
      console.log('write stream ready. Beginning Fetch...');
      fetch(etlconf.pull.dataSourceUrl).then( res => {
        res.body.pipe(wstream);
        res.body.on('error', err => {
          reject(err);
        });
        wstream.on('finish', () => {
          console.log('finished writes...');
          resolve(dataSink);
        });
        wstream.on('error', err => {
          reject(err);
        });
      })
      .then( () => {
        console.log('finished fetch...');
      })
      .catch( err => {
        console.log(`Failed to fetch with error type ${err.type} and message ${err.message}`);
        reject(err);
      });
    });
  });
}

function push() {
  return new Promise((resolve, reject) => {
    const dataSourcePath = path.join(__dirname, '..', 'localstore', path.sep, 'push-from', etlconf.push.dataPushFile);
    const timestamp = filenameTimestamp();
    const dataSinkName = `${etlconf.push.datasetName}-${timestamp}.${etlconf.push.targetFileExt}`;
    const fileType = ftype.getFileTypeFromName(etlconf.push.dataPushFile);
    const rstream = fs.createReadStream(dataSourcePath);
    const theBucket = storage.bucket(etlconf.push.bucket);
    // const wstream = theBucket.file(dataSinkName).createWriteStream({validation:'crc32c'});
    const wstream = theBucket.file(dataSinkName).createWriteStream();
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
      resolve(`${etlconf.push.bucket}/${dataSinkName}`);
    });
  });
}

function bounce() {
  return new Promise((resolve, reject) => {
    const timestamp = filenameTimestamp();
    const dataSinkName = `${etlconf.bounce.datasetName}-${timestamp}.${etlconf.bounce.targetFileExt}`;
    console.log(`bouncing [${etlconf.bounce.dataSourceUrl}] to bucket [${etlconf.bounce.bucket}/${dataSinkName}]`);
    const theBucket = storage.bucket(etlconf.bounce.bucket);
    const wstream = theBucket.file(dataSinkName).createWriteStream();
    console.log('********** prepping bucket for writing ************');
    wstream.on('error', (err) => {
      return reject(err);
    });
    wstream.on('finish', () => {
      console.log("Uploaded successfully!!");
      return resolve(`${etlconf.bounce.bucket}/${dataSinkName}`);
    });

    console.log('********** getting ready to stream ************');
    fetch(etlconf.pull.dataSourceUrl)
    //.then(res => res.body.pipe(process.stdout)) //tho res.body is synchronous, it actually rtns a readable stream. Also rem this is node-fetch, not fetch.
    .then(res => res.body.pipe(wstream)) //tho res.body is synchronous, it actually rtns a readable stream. Also rem this is node-fetch, not fetch.
    .then(() => {
      console.log("reads complete");
    })
    .catch(err => {
      return reject(err);
    });
  });
}

function cfdeploy(cfname, init=false) {
  return new Promise((resolve, reject) => {
    //build env keys for cloudfuncs
    console.log('process cloud func ', cfname);
    let envstring;
    let gcloudCmd;
    const argsForFirstEverExecution = '';
  
    //TODO: stop doing this and use Cloud KMS instead...
    try {
      let tmparr =[];
      Object.keys(etlconf.cloudSQL).forEach(k => {
        const key = k.toUpperCase();
        const value = etlconf.cloudSQL[k];
        tmparr.push(`${key}=${value}`);
      });
      envstring = tmparr.join(',');
    
      if(init) {
        if (etlconf.CloudFuncs[cfname].type === 'onStore') {
          argsForFirstEverExecution += `--trigger-event google.storage.object.finalize --trigger-resource ${etlconf.push.bucket}`;
        } else {
          //none others supported at the mo.
        }
      }
      
      gcloudCmd = `gcloud beta functions deploy ${cfname} --runtime nodejs8 --source ${etlconf.cloudFuncsRemote} --set-env-vars ${envstring} ${argsForFirstEverExecution}`;
    } catch (err) {
      console.log('woops... snafu parsing cloudfuncs info!')
      reject(err);
    }
  
    const child = exec(gcloudCmd, (error, stdout, stderr) => {
      if (error) {
          console.error(error);
          reject(error);
      }
      console.log(stdout);
      resolve('done');
    });
  });
}

function pushsql(sqlfile) {
  return new Promise((resolve, reject) => {
    if(!sqlfile) {
      reject(new Error('no sql file specified!'));
    }
    const dataSourcePath = path.join(__dirname, '..', 'sql', path.sep, sqlfile);
    const dataSinkName = sqlfile;
    storage.bucket(etlconf.cloudSQL.sqlbucket)
    .upload(dataSourcePath)
    .then(()=>{
      resolve(`${etlconf.cloudSQL.sqlbucket}/${dataSinkName}`);
    })
    .catch(err => {
      reject(err);
    });
  });
}

/*
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
    },
    {
      name: 'bounce',
      type: Boolean,
      description: 'stream from predefined data source to predefined bucket storage in cloud'
    },
    {
      name: 'cfdeploy',
      type: String,
      description: 'provide the name of a cloud functions to deploy from gcp remote repo'
    },
    {
      name: 'pushsql',
      type: String,
      description: 'push sql file to cloud bucket'
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
    let asyncAction;
    if(options.pull) {
      asyncAction = pull();
    } else if (options.push) {
      asyncAction = push();
    } else if (options.bounce) {
      asyncAction = bounce();
    } else if (options.cfdeploy) {
      asyncAction = cfdeploy(options.cfdeploy);
    } else if (options.pushsql) {
      asyncAction = pushsql(options.pushsql);
    } else {
      console.log('no such command!');
      console.log(usage);
      return;
    }

    asyncAction.then(meta => {
      console.log('DONE!!', meta);
    })
    .catch(err => {
      console.log('sigh. oh dear...');
      console.log(err);
    });
  }
}

main();