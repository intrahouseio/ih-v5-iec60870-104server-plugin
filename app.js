const util = require('util');
const { IEC104Server } = require('ih-lib60870-node');
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

module.exports = function (plugin) {
  const params = plugin.params;
  let extraChannels = plugin.extraChannels;
  let curData = {};
  let curCmd = {};
  let curDataArr = [];
  let clients = {};
  let periodicTasks = {};
  let filter = filterExtraChannels();
  subExtraChannels(filter);

  const eventBuffer = [];
  const clientPointers = {};
  const sboSelections = {};
  const SBO_TIMEOUT = 30000;
  const MAX_BATCH_SIZE = 10; // Максимальный размер пакета для отправки

  const validCmdExec = ['direct', 'select'];
  const cmdExec = validCmdExec.includes(params.cmdExec) ? params.cmdExec : 'direct';
  if (!validCmdExec.includes(params.cmdExec)) {
    plugin.log(`Invalid cmdExec value: ${params.cmdExec}, defaulting to 'direct'`, 2);
  }

  if (cmdExec === 'direct') {
    Object.keys(sboSelections).forEach(clientId => delete sboSelections[clientId]);
  }

  const execDefault = Number(params.execdefault) || 2000;
  const execShort = Number(params.execshort) || 500;
  const execLong = Number(params.execlong) || 5000;
  plugin.log(`Execution times: QOC=0=${execDefault}ms, QOC=1=${execShort}ms, QOC=2=${execLong}ms, QOC=3=persistent`, 2);

  // функция chunkArray с группировкой по asduAddress и typeId
  function chunkArray(array, size) {
  // Группировка объектов по asduAddress и typeId
  const grouped = {};
  array.forEach(item => {
    const key = `${item.asduAddress}_${item.typeId}`;
    if (!grouped[key]) {
      grouped[key] = [];
    }
    grouped[key].push(item);
  });

  // Разбиение каждой группы на пакеты размером не более size
  const result = [];
  Object.values(grouped).forEach(group => {
    for (let i = 0; i < group.length; i += size) {
      result.push(group.slice(i, i + size));
    }
  });

  return result;
}

  function subExtraChannels(filter) {
    plugin.onSub('devices', filter, data => {
      curDataArr = [];
      data.forEach(item => {
        const curitem = filter[item.did + "." + item.prop];
        if (curitem) {
          if (curitem.extype === 'pub') {
            let value;
            if (curitem.ioObjMtype == 1 || curitem.ioObjMtype == 30) {
              value = item.value == 1 ? true : false;
            } else {
              value = Number(item.value);
            }
            const event = {
              typeId: Number(curitem.ioObjMtype),
              ioa: Number(curitem.address),
              value,
              asduAddress: Number(curitem.asdu),
              timestamp: Date.now(),
              quality: item.chstatus != undefined ? Number(item.chstatus) : 0,
              cot: 3
            };
            curData[curitem.did + '.' + curitem.prop] = event;
            curDataArr.push(event);
            addToEventBuffer(event);
          } else if (curitem.extype === 'set') {
            let value;
            if (curitem.ioObjCtype == 45 || curitem.ioObjCtype == 58) {
              value = item.value == 1 ? true : false;
            } else {
              value = Number(item.value);
            }
            const cmd = {
              typeId: Number(curitem.ioObjCtype),
              ioa: Number(curitem.address),
              value,
              asduAddress: Number(curitem.asdu),
              timestamp: Date.now(),
              quality: item.chstatus != undefined ? Number(item.chstatus) : 0,
              cot: 3
            };
            curCmd[curitem.did + '.' + curitem.prop] = cmd;
          }
        }
      });

      const status = server.getStatus();
      if (status.connectedClients.length > 0) {
        status.connectedClients.forEach(clientId => {
          if (clients[clientId]?.activated === 1) {
            try {
              // Разбиваем curDataArr на части по MAX_BATCH_SIZE
              const batches = chunkArray(curDataArr, MAX_BATCH_SIZE);
              batches.forEach(batch => {
                const success = server.sendCommands(clientId, batch);
                if (success) {
                  plugin.log(`Sent ${batch.length} events to client ${clientId}: ${util.inspect(batch)}`, 2);
                } else {
                  plugin.log(`Failed to send ${batch.length} events to client ${clientId}`, 2);
                }
              });
              updateClientPointer(clientId, eventBuffer.length);
            } catch (e) {
              plugin.log(`Command ERROR for client ${clientId}: ${util.inspect(e)}`, 2);
            }
          }
        });
      }
    });
  }

  plugin.onChange('extra', async (recs) => {
    Object.values(periodicTasks).forEach(task => clearInterval(task.timerId));
    extraChannels = await plugin.extra.get();
    curData = {};
    curCmd = {};
    periodicTasks = {};
    filter = filterExtraChannels();
    subExtraChannels(filter);
  });

  function addToEventBuffer(event) {
    const maxBufferSize = Number(params.maxBufferSize) || 1000;
    if (eventBuffer.length >= maxBufferSize) {
      eventBuffer.shift();
      for (const clientId in clientPointers) {
        if (clientPointers[clientId] > 0) {
          clientPointers[clientId]--;
        }
      }
      plugin.log('Event buffer overflow, oldest event removed', 2);
    }
    eventBuffer.push(event);
  }

  function updateClientPointer(clientId, position) {
    clientPointers[clientId] = position;
  }

  function sendBufferedEvents(clientId) {
    const pointer = clientPointers[clientId] || 0;
    const eventsToSend = eventBuffer.slice(pointer).map(event => ({
      ...event,
      cot: 48
    }));
    if (eventsToSend.length > 0) {
      try {
        // Разбиваем eventsToSend на части по MAX_BATCH_SIZE
        const batches = chunkArray(eventsToSend, MAX_BATCH_SIZE);
        batches.forEach(batch => {
          const success = server.sendCommands(clientId, batch);
          if (success) {
            plugin.log(`Sent ${batch.length} buffered events to client ${clientId}: ${util.inspect(batch)}`, 2);
          } else {
            plugin.log(`Failed to send ${batch.length} buffered events to client ${clientId}`, 2);
          }
        });
        updateClientPointer(clientId, eventBuffer.length);
      } catch (e) {
        plugin.log(`Error sending buffered events to client ${clientId}: ${util.inspect(e)}`, 2);
      }
    } else {
      plugin.log(`No buffered events to send to client ${clientId}`, 2);
    }
  }

  async function sendCurrentValues(clientId) {
    const currentValues = Object.values(curData).map(item => ({
      ...item,
      cot: 20
    }));
    if (currentValues.length > 0) {
      try {
        // Разбиваем currentValues на части по MAX_BATCH_SIZE
        const batches = chunkArray(currentValues, MAX_BATCH_SIZE);
        for (let i=0; i<batches.length; i++) {
          const batch = batches[i];
          const success = server.sendCommands(clientId, batch);
          await sleep(100);
          if (success) {
            plugin.log(`Sent ${batch.length} current values to client ${clientId}: ${util.inspect(batch)}`, 2);
          } else {
            plugin.log(`Failed to send ${batch.length} current values to client ${clientId}`, 2);
          }
        }
      } catch (e) {
        plugin.log(`Error sending current values to client ${clientId}: ${util.inspect(e)}`, 2);
      }
    } else {
      plugin.log(`No current values to send to client ${clientId}`, 2);
    }
  }

  function sendPeriodicDataByInterval(intervalMs) {
    const task = periodicTasks[intervalMs];
    if (!task) return;

    const dataToSend = [];
    task.didProps.forEach(didProp => {
      const item = filter[didProp];
      if (item && item.extype === 'pub') {
        const data = curData[didProp];
        if (data) {
          dataToSend.push({ ...data, cot: 20 });
        }
      }
    });

    if (dataToSend.length > 0) {
      const status = server.getStatus();
      if (status.connectedClients.length > 0) {
        status.connectedClients.forEach(clientId => {
          if (clients[clientId]?.activated === 1) {
            try {
              // Разбиваем dataToSend на части по MAX_BATCH_SIZE
              const batches = chunkArray(dataToSend, MAX_BATCH_SIZE);
              batches.forEach(batch => {
                const success = server.sendCommands(clientId, batch);
                if (success) {
                  plugin.log(`Sent ${batch.length} periodic data items for interval ${intervalMs / 60000} minutes to client ${clientId}: ${util.inspect(batch)}`, 2);
                } else {
                  plugin.log(`Failed to send ${batch.length} periodic data items for interval ${intervalMs / 60000} minutes to client ${clientId}`, 2);
                }
              });
            } catch (e) {
              plugin.log(`Error sending periodic data for interval ${intervalMs / 60000} minutes to client ${clientId}: ${util.inspect(e)}`, 2);
            }
          }
        });
      } else {
        plugin.log(`No clients connected to send periodic data for interval ${intervalMs / 60000} minutes`, 2);
      }
    }
  }

  function sendCounterValues(clientId, qcc) {
    const counterValues = Object.values(curData)
      .filter(item => item.typeId === 15)
      .map(item => ({
        ...item,
        cot: qcc >= 2 && qcc <= 5 ? 37 : 20
      }));
    if (counterValues.length > 0) {
      try {
        server.sendCommands(clientId, [{
          typeId: 101,
          ioa: 0,
          value: qcc,
          asduAddress: counterValues[0].asduAddress,
          cot: 7
        }]);
        // Разбиваем counterValues на части по MAX_BATCH_SIZE
        const batches = chunkArray(counterValues, MAX_BATCH_SIZE);
        batches.forEach(batch => {
          const success = server.sendCommands(clientId, batch);
          if (success) {
            plugin.log(`Sent ${batch.length} counter values to client ${clientId}: ${util.inspect(batch)}`, 2);
          } else {
            plugin.log(`Failed to send ${batch.length} counter values to client ${clientId}`, 2);
          }
        });
      } catch (e) {
        plugin.log(`Error sending counter values to client ${clientId}: ${util.inspect(e)}`, 2);
        server.sendCommands(clientId, [{
          typeId: 101,
          ioa: 0,
          value: qcc,
          asduAddress: counterValues[0]?.asduAddress || 0,
          cot: 10
        }]);
      }
    } else {
      plugin.log(`No counter values to send to client ${clientId}`, 2);
      server.sendCommands(clientId, [{
        typeId: 101,
        ioa: 0,
        value: qcc,
        asduAddress: 0,
        cot: 10
      }]);
    }
  }

  function cleanupSboSelections() {
    const now = Date.now();
    for (const clientId in sboSelections) {
      for (const key in sboSelections[clientId]) {
        if (now - sboSelections[clientId][key].timestamp > SBO_TIMEOUT) {
          delete sboSelections[clientId][key];
          plugin.log(`Cleared expired SBO selection for client ${clientId}, key=${key}`, 2);
        }
      }
      if (Object.keys(sboSelections[clientId]).length === 0) {
        delete sboSelections[clientId];
      }
    }
  }

  function validateCommandValue(typeId, val) {
    if (typeId === 46) return [0, 1, 2, 3].includes(val);
    if (typeId === 47) return [0, 1, 2].includes(val);
    if (typeId === 49) return Number.isInteger(val) && val >= -32768 && val <= 32767;
    if (typeId === 51) return Number.isInteger(val) && val >= 0 && val <= 0xFFFFFFFF;
    return true;
  }

  function handleControlCommand(clientId, typeId, ioa, val, ql, timestamp, asduAddress, item, cmdItem, bselCmd, sboClient, sboKey, responseTypeId) {
    if (!item) {
      try {
        server.sendCommands(clientId, [{
          typeId,
          ioa,
          value: val,
          timestamp,
          asduAddress,
          cot: 10
        }]);
      } catch (e) {
        plugin.log(`Error in server.sendCommands: ${util.inspect(e)}`, 2);
      }
      plugin.log(`Error for command typeId=${typeId}: Unknown object, ioa=${ioa}, asduAddress=${asduAddress}`, 2);
      return;
    }

    if (!cmdItem || cmdItem.quality !== 0) {
      try {
        server.sendCommands(clientId, [{
          typeId,
          ioa,
          value: val,
          timestamp,
          asduAddress,
          cot: 10
        }]);
      } catch (e) {
        plugin.log(`Error in server.sendCommands: ${util.inspect(e)}`, 2);
      }
      plugin.log(`Error for command typeId=${typeId}: ${!cmdItem ? 'No command in curCmd' : `Invalid quality (${cmdItem.quality})`}, ioa=${ioa}, asduAddress=${asduAddress}`, 2);
      return;
    }

    if (!validateCommandValue(typeId, val)) {
      try {
        server.sendCommands(clientId, [{
          typeId,
          ioa,
          value: val,
          timestamp,
          asduAddress,
          cot: 10
        }]);
      } catch (e) {
        plugin.log(`Error in server.sendCommands: ${util.inspect(e)}`, 2);
      }
      plugin.log(`Invalid value ${val} for typeId=${typeId}, ioa=${ioa}, asduAddress=${asduAddress}`, 2);
      return;
    }

    const processedVal = (typeId === 45 || typeId === 58) ? val === 1 :
                        [46, 47, 49, 51, 59, 60, 62, 64].includes(typeId) ? Math.round(val) : val;

    let execTime = null;
    let commandType = 'none';
    if ([45, 46, 58, 59].includes(typeId)) {
      switch (ql) {
        case 0:
          execTime = execDefault;
          commandType = 'default';
          break;
        case 1:
          execTime = execShort;
          commandType = 'short';
          break;
        case 2:
          execTime = execLong;
          commandType = 'long';
          break;
        case 3:
          execTime = null;
          commandType = 'persistent';
          break;
        default:
          try {
            server.sendCommands(clientId, [{
              typeId,
              ioa,
              value: val,
              timestamp,
              asduAddress,
              cot: 10
            }]);
          } catch (e) {
            plugin.log(`Error in server.sendCommands: ${util.inspect(e)}`, 2);
          }
          plugin.log(`Invalid QOC ${ql} for typeId=${typeId}, ioa=${ioa}, asduAddress=${asduAddress}`, 2);
          return;
      }
      plugin.log(`Command typeId=${typeId}, ioa=${ioa}, asduAddress=${asduAddress}: QOC=${ql} (${commandType}), execTime=${execTime === null ? 'none' : execTime + 'ms'}`, 2);
    }

    if (cmdExec === 'direct' && bselCmd) {
      try {
        server.sendCommands(clientId, [{
          typeId,
          ioa,
          value: processedVal,
          timestamp,
          asduAddress,
          cot: 10
        }]);
      } catch (e) {
        plugin.log(`Error in server.sendCommands: ${util.inspect(e)}`, 2);
      }
      plugin.log(`Select not allowed in direct mode for typeId=${typeId}: ioa=${ioa}, asduAddress=${asduAddress}`, 2);
    } else if (cmdExec === 'select' && !bselCmd && !sboClient[sboKey]) {
      try {
        server.sendCommands(clientId, [{
          typeId,
          ioa,
          value: processedVal,
          timestamp,
          asduAddress,
          cot: 10
        }]);
      } catch (e) {
        plugin.log(`Error in server.sendCommands: ${util.inspect(e)}`, 2);
      }
      plugin.log(`Direct command not allowed in SBO mode for typeId=${typeId}: ioa=${ioa}, asduAddress=${asduAddress}`, 2);
    } else if (cmdExec === 'select' && bselCmd) {
      sboSelections[clientId] = sboSelections[clientId] || {};
      sboSelections[clientId][sboKey] = { ioa, asduAddress, timestamp: Date.now() };
      try {
        server.sendCommands(clientId, [{
          typeId,
          ioa,
          value: processedVal,
          timestamp,
          asduAddress,
          cot: 7
        }]);
      } catch (e) {
        plugin.log(`Error in server.sendCommands: ${util.inspect(e)}`, 2);
      }
      plugin.log(`SBO Select successful for client ${clientId}, ioa=${ioa}, asduAddress=${asduAddress}`, 2);
      cleanupSboSelections();
    } else {
      if (cmdExec === 'direct' || (cmdExec === 'select' && sboClient[sboKey] && sboClient[sboKey].ioa === ioa && sboClient[sboKey].asduAddress === asduAddress)) {
        try {
          server.sendCommands(clientId, [{
            typeId,
            ioa,
            value: processedVal,
            timestamp,
            asduAddress,
            cot: 7
          }]);

          const itemKey = `${asduAddress}.${ioa}`;
          const filterItem = filter[itemKey];
          if (filterItem) {
            const { did, prop } = filterItem;
            try {
              plugin.send({
                type: 'command',
                command: 'setval',
                did,
                prop,
                value: processedVal
              });
              plugin.log(`Sent setval command: did=${did}, prop=${prop}, value=${processedVal}`, 2);
            } catch (e) {
              plugin.log(`Error in plugin.send: ${util.inspect(e)}`, 2);
            }

            if ([45, 46, 58, 59].includes(typeId) && execTime !== null && execTime > 0) {
              setTimeout(() => {
                try {
                  plugin.send({
                    type: 'command',
                    command: 'setval',
                    did,
                    prop,
                    value: 0
                  });
                  plugin.log(`Sent reset command: did=${did}, prop=${prop}, value=0 after ${execTime}ms`, 2);
                } catch (e) {
                  plugin.log(`Error in plugin.send (reset): ${util.inspect(e)}`, 2);
                }
              }, execTime);
            }
          } else {
            plugin.log(`No filter item found for asduAddress=${asduAddress}, ioa=${ioa}, cannot send setval`, 2);
          }

          server.sendCommands(clientId, [{
            typeId: responseTypeId,
            ioa,
            value: processedVal,
            timestamp,
            asduAddress,
            cot: 20
          }]);

          if (cmdExec === 'select') {
            delete sboSelections[clientId][sboKey];
            plugin.log(`SBO Operate successful for client ${clientId}, ioa=${ioa}, asduAddress=${asduAddress}`, 2);
          } else {
            plugin.log(`Direct command successful for client ${clientId}, ioa=${ioa}, asduAddress=${asduAddress}`, 2);
          }
        } catch (e) {
          plugin.log(`Error sending command for client ${clientId}: ${util.inspect(e)}`, 2);
          try {
            server.sendCommands(clientId, [{
              typeId,
              ioa,
              value: processedVal,
              timestamp,
              asduAddress,
              cot: 10
            }]);
          } catch (err) {
            plugin.log(`Error in server.sendCommands: ${util.inspect(err)}`, 2);
          }
        }
      } else {
        try {
          server.sendCommands(clientId, [{
            typeId,
            ioa,
            value: processedVal,
            timestamp,
            asduAddress,
            cot: 10
          }]);
        } catch (e) {
          plugin.log(`Error in server.sendCommands: ${util.inspect(e)}`, 2);
        }
        plugin.log(`SBO Operate failed: No prior Select for client ${clientId}, ioa=${ioa}, asduAddress=${asduAddress}`, 2);
      }
    }
  }

  const server = new IEC104Server((event, data) => {
    plugin.log(`Server Event: ${event}, Data: ${JSON.stringify(data, null, 2)}`, 2);
    if (event === 'data') {
      if (data.type === 'control') {
        clients[data.clientId] = { 
          activated: data.event === "activated" ? 1 : 0
        };
        if (data.event === 'activated') {
          //sendCurrentValues(data.clientId);
          //sendBufferedEvents(data.clientId);
        } else if (data.event === 'disconnected') {
          delete sboSelections[data.clientId];
          delete clients[data.clientId];
        }
      }
      if (Array.isArray(data)) {
        data.forEach(cmd => {
          try {
            const { clientId, typeId, ioa, val, quality, bselCmd, ql, timestamp, asduAddress } = cmd;
            const itemKey = `${asduAddress}.${ioa}`;
            const item = filter[itemKey];
            const cmdItem = item && item.did && item.prop ? curCmd[item.did + "." + item.prop] : null;

            const sboKey = `${ioa}_${asduAddress}`;
            const sboClient = sboSelections[clientId] || {};

            switch (typeId) {
              case 45:
                handleControlCommand(clientId, 45, ioa, val, ql, undefined, asduAddress, item, cmdItem, bselCmd, sboClient, sboKey, 1);
                break;
              case 46:
                handleControlCommand(clientId, 46, ioa, val, ql, undefined, asduAddress, item, cmdItem, bselCmd, sboClient, sboKey, 3);
                break;
              case 47:
                handleControlCommand(clientId, 47, ioa, val, ql, undefined, asduAddress, item, cmdItem, bselCmd, sboClient, sboKey, 5);
                break;
              case 48:
                handleControlCommand(clientId, 48, ioa, val, ql, undefined, asduAddress, item, cmdItem, bselCmd, sboClient, sboKey, 9);
                break;
              case 49:
                handleControlCommand(clientId, 49, ioa, val, ql, undefined, asduAddress, item, cmdItem, bselCmd, sboClient, sboKey, 11);
                break;
              case 50:
                handleControlCommand(clientId, 50, ioa, val, ql, undefined, asduAddress, item, cmdItem, bselCmd, sboClient, sboKey, 13);
                break;
              case 51:
                handleControlCommand(clientId, 51, ioa, val, ql, undefined, asduAddress, item, cmdItem, bselCmd, sboClient, sboKey, 7);
                break;
              case 58:
                handleControlCommand(clientId, 58, ioa, val, ql, timestamp, asduAddress, item, cmdItem, bselCmd, sboClient, sboKey, 30);
                break;
              case 59:
                handleControlCommand(clientId, 59, ioa, val, ql, timestamp, asduAddress, item, cmdItem, bselCmd, sboClient, sboKey, 31);
                break;
              case 60:
                handleControlCommand(clientId, 60, ioa, val, ql, timestamp, asduAddress, item, cmdItem, bselCmd, sboClient, sboKey, 32);
                break;
              case 61:
                handleControlCommand(clientId, 61, ioa, val, ql, timestamp, asduAddress, item, cmdItem, bselCmd, sboClient, sboKey, 34);
                break;
              case 62:
                handleControlCommand(clientId, 62, ioa, val, ql, timestamp, asduAddress, item, cmdItem, bselCmd, sboClient, sboKey, 35);
                break;
              case 63:
                handleControlCommand(clientId, 63, ioa, val, ql, timestamp, asduAddress, item, cmdItem, bselCmd, sboClient, sboKey, 36);
                break;
              case 64:
                handleControlCommand(clientId, 64, ioa, val, ql, timestamp, asduAddress, item, cmdItem, bselCmd, sboClient, sboKey, 33);
                break;
              case 100:
                plugin.log(`Received Interrogation Command: clientId=${clientId}, ioa=${ioa}, qoi=${val}, asduAddress=${asduAddress}`, 2);
                server.sendCommands(clientId, [{
                  typeId: 100,
                  ioa: 0,
                  value: val,
                  asduAddress,
                  cot: 7
                }]);
                sendCurrentValues(clientId);
                //sendBufferedEvents(clientId);
                break;
              case 101:
                plugin.log(`Received Counter Interrogation Command: clientId=${clientId}, ioa=${ioa}, qcc=${val}, asduAddress=${asduAddress}`, 2);
                sendCounterValues(clientId, val);
                break;
              case 102:
                plugin.log(`Received Read Command: clientId=${clientId}, ioa=${ioa}, asduAddress=${asduAddress}`, 2);
                const curitem = item && item.did && item.prop ? curData[item.did + "." + item.prop] : null;
                if (!item || !curitem || curitem.quality !== 0) {
                    server.sendCommands(clientId, [{
                        typeId: item?.ioObjMtype || 1,
                        ioa,
                        value: null,
                        asduAddress,
                        quality: 0x40,
                        cot: 47
                    }]);
                    plugin.log(`Error for Read Command: ${!item || !curitem ? "Unknown object" : `Invalid quality (${curitem.quality})`}, ioa=${ioa}, asduAddress=${asduAddress}`, 2);
                } else {
                    server.sendCommands(clientId, [{
                        ...curitem,
                        cot: 5
                    }]);
                }
                break;
              case 103:
                plugin.log(`Received Clock Sync Command: clientId=${clientId}, ioa=${ioa}, timestamp=${timestamp}, asduAddress=${asduAddress}`, 2);
                server.sendCommands(clientId, [{
                    typeId: 103,
                    ioa: 0,
                    timestamp,
                    asduAddress,
                    cot: 7
                }]);
                break;
              default:
                plugin.log(`Unknown command typeId=${typeId}, clientId=${clientId}, ioa=${ioa}, value=${val}, asduAddress=${asduAddress}`, 2);
                server.sendCommands(clientId, [{
                    typeId,
                    ioa,
                    value: val,
                    asduAddress,
                    cot: 10
                }]);
            }
          } catch (e) {
            plugin.log(`ERROR Command: ${util.inspect(e)}`, 2);
            server.sendCommands(clientId, [{
              typeId: cmd.typeId,
              ioa: cmd.ioa,
              value: cmd.val,
              asduAddress: cmd.asduAddress,
              cot: 10
            }]);
          }
        });
      }
    }
    
  });

  server.start({
    port: Number(params.port),
    serverID: "server IntraSCADA",
    ipAddress: "0.0.0.0",
    mode: "multi",
    params: {
      originatorAddress: Number(params.originatorAddress),
      k: Number(params.k),
      w: Number(params.w),
      t0: Number(params.t0),
      t1: Number(params.t1),
      t2: Number(params.t2),
      t3: Number(params.t3),
      maxClients: Number(params.maxClients)
    }
  });

  process.on('SIGTERM', () => {
    terminate();
  });

  function terminate() {
    Object.values(periodicTasks).forEach(task => clearInterval(task.timerId));
    plugin.log('TERMINATE PLUGIN', 2);
    plugin.exit();
  }

  function filterExtraChannels() {
    let res = { did_prop: [] };
    if (!Array.isArray(extraChannels)) {
      plugin.log('Error: extraChannels is not an array', 2);
      return res;
    }

    extraChannels.forEach(item => {
      const didProp = item.did + '.' + item.prop;
      res.did_prop.push(didProp);
      res[didProp] = item;
      res[item.asdu + '.' + item.address] = item;

      if (item.extype === 'pub' && item.interval !== undefined && !isNaN(Number(item.interval)) && Number(item.interval) > 0) {
        const intervalMs = Number(item.interval) * 60 * 1000;
        if (!periodicTasks[intervalMs]) {
          periodicTasks[intervalMs] = {
            didProps: [],
            timerId: setInterval(() => {
              sendPeriodicDataByInterval(intervalMs);
            }, intervalMs)
          };
          plugin.log(`Set periodic task for interval ${intervalMs / 60000} minutes`, 2);
        }
        periodicTasks[intervalMs].didProps.push(didProp);
      }
    });
    return res;
  }
};