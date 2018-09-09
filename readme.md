# GCP ETL Toolkit
in alpha / experimental.

## Prerequisites
Project folder must include:
- keys/
- localstore/
- sql/
- .etlconf.js

Additionally you must have google cloud sdk incl beta components installed on your machine.

## How to use:
1. Ensure you have populated the .etlconf.js file with your own settings
1. Ensure your cloud functions have been written and either synced to a google repo or has a google remote for updates. see [here](https://github.com/lululeon/gcp-cloudfuncs) for a very rough example.
1. Run the specific tasks you need to check / setup the pipeline and deploy your cloud functions.
```
node src/index.js --help
```
4. when all the parts of the pipeline are configured and ready, set off the cron job and it should run itself:
```
npm run start
``` 
Horrible - needs to evolve into a cli at min proper ui at best.
