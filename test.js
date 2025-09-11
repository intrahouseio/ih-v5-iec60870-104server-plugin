const util = require('util');
const { IEC104Server } = require('ih-lib60870-node');

const server = new IEC104Server((event, data) => {
    console.log("event " + util.inspect(event));
    console.log("data " + util.inspect(data));
});

for(i=0; i< 1000; i++) {
    j++;
    const event = {
              typeId: 11,
              ioa: i,
              value: i,
              asduAddress: 1,
              timestamp: Date.now(),
              quality: 0,
              cot: 3
            };
    if (j == 10) server.sendCommand(event)
}