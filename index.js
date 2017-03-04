
const es = require('event-stream');
const fs = require('fs');
const R = require('ramda')
const moment = require('moment')
const records = [];


fs.createReadStream('./work-order-history.csv')
  .pipe(es.split('\n'))
  .pipe(es.mapSync((line) => {
    if (!line || line.indexOf('WO#,') >= 0) {
      return;
    }
    const split = line.split(',');
    let assPartNum = split[1];
    if (!assPartNum) {
      return;
    } else {
      assPartNum = assPartNum.split('-')[4] || 'OTHER';
    }
    const assNum = parseInt(split[2]);

    const issueDate = split[9]
    const priority = parseInt(split[27]);
    if (!assNum || !priority) {
      return; // invalid line
    }
    records.push({
      assPartNum, assNum, issueDate, priority
    })
    return records.length;
  }))
  .on('error', e => console.error(e))
  .on('end', () => {
    const partNumToServiceDates = R.map(l => l.sort().reverse(), records.reduce((result, record) => {
      const time = moment(record.issueDate, 'DD-MMM-YY').toDate().getTime();
      if (!isNaN(time)) {
        if (!result[record.assPartNum]) {
          result[record.assPartNum] = [];
        }
        result[record.assPartNum].push(time);
      } else {
        // ignore invalid date
      }
      return result;
    }, {}));

    const f = R.map(serviceDateList => serviceDateList.map((d, index) => {
      if (index === 0) {
        return 0
      } else {
        return serviceDateList[index - 1] - d
      }
    }).reduce((result, t) => result + t, 0), partNumToServiceDates)
    const avgTimeBetweenService = R.mapObjIndexed((v, k) => {
      return Math.round(v / partNumToServiceDates[k].length / 8.64e+7)
    }, f)
    //  const f = partNumToServiceDates
    R.forEachObjIndexed((v, k) => console.log(`part ${k} runs for ~${v} days between service`), avgTimeBetweenService)
  });
