/* CREATE A COPY OF THIS, RENAME TO '.etlconf.js' */

const etlconf = {
  pull : {
    dataSourceUrl : 'https://foo/bar',
    dataSinkName : 'mydataset',
    dataSinkSuffix : 'json'
  },
  bounce : {
    bucket : 'mybucketname',
    dataSourceUrl : 'https://blah/bloog',
    datasetName : 'stockprices',
    targetFileExt : 'txt'
  },
  push : {
    bucket : 'mybucketname',
    dataPushFile : 'fancyfile.zip',
    datasetName : 'weatherreports',
    targetFileExt : 'txt',
    transforms : { //not impld yet
      rename: '',
      unzip: true
    }
  },
  cloudFuncsRemote : 'https://source.developers.google.com/projects/<projectid>/repos/<googleRepo>/moveable-aliases/master/paths/<parentFolderOfFuncs>',
  cloudFuncs : {
    myCloudFuncName1: { type: 'onStorage' }, //triggered when something happens on the bucket
    myCloudFuncName2: { type: 'onPubSub' } //triggered directly
  },
  pubsubTopic : 'topicname',
  cloudSQL : {
    connectionName : 'your-db-connection-name-from-gcp-console',
    dbUser : 'username',
    dbPass : 'password',
    dbName : 'dbname',
    sqlbucket : 'mybucket'
  },
  gcpConf : {
    projectId: 'myprojectid',
    keyFilename: 'path/to/myserviceaccountkeyfile.json' //eg. keys/
  }
};

module.exports = etlconf;