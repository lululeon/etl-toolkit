#!/usr/bin/env node
const cron = require('node-cron');
const path = require('path');
const exec = require('child_process').exec;

/*
slot 1  MIN     Minute field    0 to 59
slot 2  HOUR    Hour field      0 to 23
slot 3  DOM     Day of Month    1-31
slot 4  MON     Month field     1-12
slot 5  DOW     Day Of Week     0-6
CMD     Command   
*/

// simulate data changing at some interval (5 mins past the hour every hour for now)
cron.schedule('5 * * * *', () => {
  const sep = path.sep;
  cmdline = `node src${sep}index.js --bounce`; //bounce so silly; rename later.
  const child = exec(`node src/index.js --bounce`, (error, stdout, stderr) => {
    if (error) {
      console.error(error);
    } else {
      console.log(stdout); //change to proper logging later.
    }
  });
});

