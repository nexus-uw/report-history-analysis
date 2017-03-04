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
    const assPartNum = split[1];
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
    const groupedByAssetNumber = R.groupBy(i => i.assNum, records);
    const reduced = Object.keys(groupedByAssetNumber).reduce((result, assetNumber) => {
      console.log('key', assetNumber, 'issues', groupedByAssetNumber[assetNumber].length)

      const workDateListPerPart = groupedByAssetNumber[assetNumber].reduce((result, record) => {
        if (!result[record.assPartNum]) {
          result[record.assPartNum] = [];
        }
        result[record.assPartNum].push(record.issueDate);
        return result;
      }, {})

      result[assetNumber] = Object.keys(workDateListPerPart).map(partName => {
        if (workDateListPerPart[partName].length <= 1) {
          return `not enough data for ${partName} in ${assetNumber} `;
        } else {
          console.log(workDateListPerPart[partName].map(s => moment(s, 'DD-MMM-YY').toDate().getTime()).sort().reverse())
          const timeList = workDateListPerPart[partName].map(s => moment(s, 'DD-MMM-YY').toDate().getTime()).sort().reverse()

          const totalUpTime = timeList
            .map((d, index) => {
              if (index === 0) {
                return 0
              } else {
                return timeList[index - 1] - d
              }
            }).reduce((result, t) => result + t, 0)
          console.log(timeList)
          console.log(totalUpTime)
          // / workDateListPerPart[partName].length;
          return `${partName} in ${assetNumber} requires service every ${Math.round(totalUpTime / timeList.length / 8.64e+7)} days`;
        }

      })

      return result
    }, {})
    console.log(reduced)
  });
