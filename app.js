const util = require('util');
const { IEC104Server } = require('ih-lib60870-node');

module.exports = function (plugin) {
  const params = plugin.params;
  let extraChannels = plugin.extraChannels;
  let curData = {};
  let curCmd = {}; // Хранилище для команд с extype == 'set'
  let curDataArr = [];
  let clients = {};
  let filter = filterExtraChannels();
  subExtraChannels(filter);

  // Буфер событий и указатели для клиентов
  const eventBuffer = [];
  const clientPointers = {};

  // Хранилище для SBO
  const sboSelections = {};
  const SBO_TIMEOUT = 30000;

  // Валидация cmdExec
  const validCmdExec = ['direct', 'select'];
  const cmdExec = validCmdExec.includes(params.cmdExec) ? params.cmdExec : 'direct';
  if (!validCmdExec.includes(params.cmdExec)) {
    plugin.log(`Invalid cmdExec value: ${params.cmdExec}, defaulting to 'direct'`, 2);
  }

  // Очистка sboSelections в режиме direct
  if (cmdExec === 'direct') {
    Object.keys(sboSelections).forEach(clientId => delete sboSelections[clientId]);
  }

  // Установка параметров времени исполнения
  const execDefault = Number(params.execdefault) || 2000; // QOC == 0
  const execShort = Number(params.execshort) || 500; // QOC == 1
  const execLong = Number(params.execlong) || 5000; // QOC == 2
  plugin.log(`Execution times: QOC=0=${execDefault}ms, QOC=1=${execShort}ms, QOC=2=${execLong}ms, QOC=3=persistent`, 2);

  function subExtraChannels(filter) {
    plugin.onSub('devices', filter, data => {
      curDataArr = [];
      data.forEach(item => {
        const curitem = filter[item.did + "." + item.prop];
        if (curitem) {
          if (curitem.extype === 'pub') {
            const event = {
              typeId: Number(curitem.ioObjMtype),
              ioa: Number(curitem.address),
              value: Number(item.value),
              asduAddress: Number(curitem.asdu),
              timestamp: Date.now(),
              quality: item.chstatus != undefined ? Number(item.chstatus) : 0,
              cot: 3 // CoT=3 для spontaneous
            };
            curData[curitem.did + '.' + curitem.prop] = event;
            curDataArr.push(event);
            // Добавление события в буфер
            addToEventBuffer(event);
          } else if (curitem.extype === 'set') {
            const cmd = {
              typeId: Number(curitem.ioObjCtype),
              ioa: Number(curitem.address),
              value: Number(item.value),
              asduAddress: Number(curitem.asdu),
              timestamp: Date.now(),
              quality: item.chstatus != undefined ? Number(item.chstatus) : 0,
              cot: 3 // CoT=3 для spontaneous
            };
            curCmd[curitem.did + '.' + curitem.prop] = cmd;
            plugin.log(`Added command to curCmd: ${curitem.did + '.' + curitem.prop}, value=${cmd.value}`, 2);
          }
        }
      });

      // Немедленная отправка данных подключенным и активированным клиентам
      const status = server.getStatus();
      if (status.connectedClients.length > 0) {
        status.connectedClients.forEach(clientId => {
          if (clients[clientId]?.activated == 1) {
            try {
              const success = server.sendCommands(Number(clientId), curDataArr);
              if (success) {
                updateClientPointer(clientId, eventBuffer.length);
                plugin.log(`Sent ${curDataArr.length} events to client ${clientId}: ${util.inspect(curDataArr)}`, 2);
              } else {
                plugin.log(`Failed to send ${curDataArr.length} events to client ${clientId}`, 2);
              }
            } catch (e) {
              plugin.log(`Command ERROR for client ${clientId}: ${util.inspect(e)}`, 2);
            }
          }
        });
      } else {
        plugin.log('No clients connected to send spontaneous data', 2);
      }
    });
  }

  plugin.onChange('extra', async () => {
    plugin.log('onChange extra', 2);
    extraChannels = await plugin.extra.get();
    curData = {}; // Очистка curData
    curCmd = {}; // Очистка curCmd
    filter = filterExtraChannels();
    subExtraChannels(filter);
  });

  // Функция добавления события в буфер
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

  // Функция обновления указателя клиента
  function updateClientPointer(clientId, position) {
    clientPointers[clientId] = position;
  }

  // Функция отправки буферизованных данных клиенту
  function sendBufferedEvents(clientId) {
    const pointer = clientPointers[clientId] || 0;
    const eventsToSend = eventBuffer.slice(pointer).map(event => ({
      ...event,
      cot: 48 // CoT=48 для архивных данных
    }));
    if (eventsToSend.length > 0) {
      try {
        const success = server.sendCommands(Number(clientId), eventsToSend);
        if (success) {
          updateClientPointer(clientId, eventBuffer.length);
          plugin.log(`Sent ${eventsToSend.length} buffered events to client ${clientId}: ${util.inspect(eventsToSend)}`, 2);
        } else {
          plugin.log(`Failed to send ${eventsToSend.length} buffered events to client ${clientId}`, 2);
        }
      } catch (e) {
        plugin.log(`Error sending buffered events to client ${clientId}: ${util.inspect(e)}`, 2);
      }
    } else {
      plugin.log(`No buffered events to send to client ${clientId}`, 2);
    }
  }

  // Функция отправки актуальных значений
  function sendCurrentValues(clientId) {
    const currentValues = Object.values(curData).map(item => ({
      ...item,
      cot: 20 // CoT=20 для interrogated
    }));
    if (currentValues.length > 0) {
      try {
        const success = server.sendCommands(Number(clientId), currentValues);
        if (success) {
          plugin.log(`Sent ${currentValues.length} current values to client ${clientId}: ${util.inspect(currentValues)}`, 2);
        } else {
          plugin.log(`Failed to send ${currentValues.length} current values to client ${clientId}`, 2);
        }
      } catch (e) {
        plugin.log(`Error sending current values to client ${clientId}: ${util.inspect(e)}`, 2);
      }
    } else {
      plugin.log(`No current values to send to client ${clientId}`, 2);
    }
  }

  // Функция для отправки данных счетчиков
  function sendCounterValues(clientId, qcc) {
    const counterValues = Object.values(curData)
      .filter(item => item.typeId === 15)
      .map(item => ({
        ...item,
        cot: qcc >= 2 && qcc <= 5 ? 37 : 20
      }));
    if (counterValues.length > 0) {
      try {
        server.sendCommands(Number(clientId), [{
          typeId: 101,
          ioa: 0,
          value: qcc,
          asduAddress: counterValues[0].asduAddress,
          cot: 7
        }]);
        const success = server.sendCommands(Number(clientId), counterValues);
        if (success) {
          plugin.log(`Sent ${counterValues.length} counter values to client ${clientId}: ${util.inspect(counterValues)}`, 2);
        } else {
          plugin.log(`Failed to send ${counterValues.length} counter values to client ${clientId}`, 2);
        }
      } catch (e) {
        plugin.log(`Error sending counter values to client ${clientId}: ${util.inspect(e)}`, 2);
        server.sendCommands(Number(clientId), [{
          typeId: 101,
          ioa: 0,
          value: qcc,
          asduAddress: counterValues[0]?.asduAddress || 0,
          cot: 10
        }]);
      }
    } else {
      plugin.log(`No counter values to send to client ${clientId}`, 2);
      server.sendCommands(Number(clientId), [{
        typeId: 101,
        ioa: 0,
        value: qcc,
        asduAddress: 0,
        cot: 10
      }]);
    }
  }

  // Функция для проверки и очистки устаревших SBO выборов
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

  // Валидация значений команд
  function validateCommandValue(typeId, val) {
    if (typeId === 46) return [0, 1, 2, 3].includes(val);
    if (typeId === 47) return [0, 1, 2].includes(val);
    if (typeId === 49) return Number.isInteger(val) && val >= -32768 && val <= 32767;
    if (typeId === 51) return Number.isInteger(val) && val >= 0 && val <= 0xFFFFFFFF;
    return true;
  }

  // Универсальная функция обработки управляющих команд
  function handleControlCommand(clientId, typeId, ioa, val, ql, timestamp, asduAddress, item, cmdItem, bselCmd, sboClient, sboKey, responseTypeId) {
    if (!item) {
      server.sendCommands(clientId, [{
        typeId,
        ioa,
        value: val,
        timestamp,
        asduAddress,
        cot: 10
      }]);
      plugin.log(`Error for command typeId=${typeId}: Unknown object, ioa=${ioa}, asduAddress=${asduAddress}`, 2);
      return;
    }

    // Проверка команды в curCmd
    if (!cmdItem || cmdItem.quality !== 0) {
      server.sendCommands(clientId, [{
        typeId,
        ioa,
        value: val,
        timestamp,
        asduAddress,
        cot: 10
      }]);
      plugin.log(`Error for command typeId=${typeId}: ${!cmdItem ? 'No command in curCmd' : `Invalid quality (${cmdItem.quality})`}, ioa=${ioa}, asduAddress=${asduAddress}`, 2);
      return;
    }

    // Валидация значения команды
    if (!validateCommandValue(typeId, val)) {
      server.sendCommands(clientId, [{
        typeId,
        ioa,
        value: val,
        timestamp,
        asduAddress,
        cot: 10
      }]);
      plugin.log(`Invalid value ${val} for typeId=${typeId}, ioa=${ioa}, asduAddress=${asduAddress}`, 2);
      return;
    }

    const processedVal = (typeId === 45 || typeId === 58) ? val === 1 :
                        [46, 47, 49, 51, 59, 60, 62, 64].includes(typeId) ? Math.round(val) : val;

    // Определение времени исполнения и типа команды для TypeID 45, 46, 58, 59
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
          execTime = null; // Persistent output
          commandType = 'persistent';
          break;
        default:
          server.sendCommands(clientId, [{
            typeId,
            ioa,
            value: val,
            timestamp,
            asduAddress,
            cot: 10
          }]);
          plugin.log(`Invalid QOC ${ql} for typeId=${typeId}, ioa=${ioa}, asduAddress=${asduAddress}`, 2);
          return;
      }
      plugin.log(`Command typeId=${typeId}, ioa=${ioa}, asduAddress=${asduAddress}: QOC=${ql} (${commandType}), execTime=${execTime === null ? 'none' : execTime + 'ms'}`, 2);
    }

    if (cmdExec === 'direct' && bselCmd) {
      server.sendCommands(clientId, [{
        typeId,
        ioa,
        value: processedVal,
        timestamp,
        asduAddress,
        cot: 10
      }]);
      plugin.log(`Select not allowed in direct mode for typeId=${typeId}: ioa=${ioa}, asduAddress=${asduAddress}`, 2);
    } else if (cmdExec === 'select' && !bselCmd && !sboClient[sboKey]) {
      server.sendCommands(clientId, [{
        typeId,
        ioa,
        value: processedVal,
        timestamp,
        asduAddress,
        cot: 10
      }]);
      plugin.log(`Direct command not allowed in SBO mode for typeId=${typeId}: ioa=${ioa}, asduAddress=${asduAddress}`, 2);
    } else if (cmdExec === 'select' && bselCmd) {
      sboSelections[clientId] = sboSelections[clientId] || {};
      sboSelections[clientId][sboKey] = { ioa, asduAddress, timestamp: Date.now() };
      server.sendCommands(clientId, [{
        typeId,
        ioa,
        value: processedVal,
        timestamp,
        asduAddress,
        cot: 7
      }]);
      plugin.log(`SBO Select successful for client ${clientId}, ioa=${ioa}, asduAddress=${asduAddress}`, 2);
      cleanupSboSelections();
    } else {
      if (cmdExec === 'direct' || (cmdExec === 'select' && sboClient[sboKey] && sboClient[sboKey].ioa === ioa && sboClient[sboKey].asduAddress === asduAddress)) {
        try {
          // Отправка подтверждения активации
          server.sendCommands(clientId, [{
            typeId,
            ioa,
            value: processedVal,
            timestamp,
            asduAddress,
            cot: 7
          }]);

          // Отправка значения в систему для всех TypeID
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

            // Сброс для TypeID 45, 46, 58, 59 с QOC = 0, 1, 2
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

          // Отправка ответа с соответствующим TypeID
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
          server.sendCommands(clientId, [{
            typeId,
            ioa,
            value: processedVal,
            timestamp,
            asduAddress,
            cot: 10
          }]);
        }
      } else {
        server.sendCommands(clientId, [{
          typeId,
          ioa,
          value: processedVal,
          timestamp,
          asduAddress,
          cot: 10
        }]);
        plugin.log(`SBO Operate failed: No prior Select for client ${clientId}, ioa=${ioa}, asduAddress=${asduAddress}`, 2);
      }
    }
  }

  const server = new IEC104Server((event, data) => {
    plugin.log(`Server Event: ${event}, Data: ${JSON.stringify(data, null, 2)}`, 2);
    if (event === 'data') {
      if (data.type === 'control') {
        clients[data.clientId] = { 
          activated: data.event === "activated" ? 1 : 0, 
          clientIdStr: data.event === "connected" ? data.clientIdStr : "" 
        };
        if (data.event === 'activated') {
          sendCurrentValues(data.clientId);
          sendBufferedEvents(data.clientId);
        } else if (data.event === 'disconnected') {
          plugin.log(`Client ${data.clientId} disconnected, buffering events`, 2);
          delete sboSelections[data.clientId];
        }
      }
      if (Array.isArray(data)) {
        data.forEach(cmd => {
          try {
            const { clientId, typeId, ioa, val, quality, bselCmd, ql, timestamp, asduAddress } = cmd;
            plugin.log("Received " + util.inspect(cmd), 2);
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
                server.sendCommands(Number(clientId), [{
                  typeId: 100,
                  ioa: 0,
                  value: val,
                  asduAddress: asduAddress,
                  cot: 7
                }]);
                sendCurrentValues(clientId);
                sendBufferedEvents(clientId);
                break;
              case 101:
                plugin.log(`Received Counter Interrogation Command: clientId=${clientId}, ioa=${ioa}, qcc=${val}, asduAddress=${asduAddress}`, 2);
                sendCounterValues(clientId, val);
                break;
              case 102:
                plugin.log(`Received Read Command: clientId=${clientId}, ioa=${ioa}, asduAddress=${asduAddress}`, 2);
                const curitem = item && item.did && item.prop ? curData[item.did + "." + item.prop] : null;
                if (!item || !curitem || curitem.quality !== 0) {
                  server.sendCommands(Number(clientId), [{
                    typeId: item?.ioObjMtype || 1,
                    ioa: ioa,
                    value: null,
                    asduAddress: asduAddress,
                    quality: 0x40,
                    cot: 47
                  }]);
                  plugin.log(`Error for Read Command: ${!item || !curitem ? "Unknown object" : `Invalid quality (${curitem.quality})`}, ioa=${ioa}, asduAddress=${asduAddress}`, 2);
                } else {
                  server.sendCommands(Number(clientId), [{
                    ...curitem,
                    cot: 5
                  }]);
                }
                break;
              case 103:
                plugin.log(`Received Clock Sync Command: clientId=${clientId}, ioa=${ioa}, timestamp=${timestamp}, asduAddress=${asduAddress}`, 2);
                server.sendCommands(Number(clientId), [{
                  typeId: 103,
                  ioa: 0,
                  timestamp: timestamp,
                  asduAddress: asduAddress,
                  cot: 7
                }]);
                break;
              default:
                plugin.log(`Unhandled command typeId=${typeId}, clientId=${clientId}, ioa=${ioa}, value=${val}, asduAddress=${asduAddress}`, 2);
                server.sendCommands(Number(clientId), [{
                  typeId: typeId,
                  ioa: ioa,
                  value: val,
                  asduAddress: asduAddress,
                  cot: 10
                }]);
            }
          } catch (e) {
            plugin.log(`ERROR Command: ${util.inspect(e)}`, 2);
            server.sendCommands(Number(clientId), [{
              typeId: cmd.typeId,
              ioa: cmd.ioa,
              value: cmd.val,
              asduAddress: cmd.asduAddress,
              cot: 10
            }]);
          }
        });
      } else {
        plugin.log(`Control Event: ${data.event}, Client ID: ${data.clientId}, Reason=${data.reason}`, 2);
      }
    }
  });

  server.start({
    port: Number(params.port),
    serverID: "server IntraSCADA",
    ipReserve: "0.0.0.0",
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
      res.did_prop.push(item.did + "." + item.prop);
      res[item.did + '.' + item.prop] = item;
      res[item.asdu + '.' + item.address] = item;
    });
    return res;
  }
};