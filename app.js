const util = require('util');
const { IEC104Server } = require('ih-lib60870-node');
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

module.exports = function (plugin) {
  const params = plugin.params;
  let extraChannels = plugin.extraChannels;
  let curData = {};
  let curCmd = {};
  let curDataArr = [];
  let clients = {}; // { clientId: { activated, asdu } }
  let ipAsduToClientId = {}; // { ipAsdu: clientId }
  let periodicTasks = {};
  let filter = filterExtraChannels();
  subExtraChannels(filter);

  const eventBuffer = [];
  const clientPointers = {};
  const sboSelections = {};
  const SBO_TIMEOUT = 30000;
  const MAX_BATCH_SIZE = 50;

  const validCmdExec = ['direct', 'select'];
  const cmdExec = validCmdExec.includes(params.cmdExec) ? params.cmdExec : 'direct';
  if (!validCmdExec.includes(params.cmdExec)) {
    plugin.log(`Недопустимое значение cmdExec: ${params.cmdExec}, используется по умолчанию 'direct'`, 2);
  }

  const useBuffer = params.useBuffer;
  plugin.log(`Режим буфера: ${useBuffer ? 'включён' : 'выключен'}`, 2);

  if (cmdExec === 'direct') {
    Object.keys(sboSelections).forEach(clientId => delete sboSelections[clientId]);
  }

  const execDefault = Number(params.execdefault) || 2000;
  const execShort = Number(params.execshort) || 500;
  const execLong = Number(params.execlong) || 5000;
  plugin.log(`Времена выполнения: QOC=0=${execDefault}ms, QOC=1=${execShort}ms, QOC=2=${execLong}ms, QOC=3=persistent`, 2);

  function chunkArray(array, size) {
    const grouped = {};
    array.forEach(item => {
      const key = `${item.asduAddress}_${item.typeId}`;
      if (!grouped[key]) {
        grouped[key] = [];
      }
      grouped[key].push(item);
    });

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
              quality: item.chstatus != undefined ? 128 : 0,
              cot: 3
            };
            curData[curitem.did + '.' + curitem.prop] = event;
            curDataArr.push(event);
            if (useBuffer) {
              addToEventBuffer(event);
            }
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
              quality: item.chstatus != undefined ? 128 : 0,
              cot: 3
            };
            curCmd[curitem.did + '.' + curitem.prop] = cmd;
          }
        }
      });

      const status = server.getStatus();
      if (status.connectedClients.length > 0 ) {
        status.connectedClients.forEach(clientId => {
          const client = clients[clientId];
          if (client?.activated === 1) {
            try {
              const batches = chunkArray(curDataArr, MAX_BATCH_SIZE);
              for (let i = 0; i < batches.length; i++) {
                const batch = batches[i];
                const success = server.sendCommands(clientId, batch);
                sleep(10).then(() => {
                  if (success) {
                    plugin.log(`Отправлено ${batch.length} событий клиенту ${clientId} (asdu=${client.asdu || 'unknown'}): ${util.inspect(batch)}`, 2);
                  } else {
                    plugin.log(`Не удалось отправить ${batch.length} событий клиенту ${clientId} (asdu=${client.asdu || 'unknown'})`, 2);
                  }
                });
              }
              if (client.asdu) {
                const ipAsdu = `${clientId.split(':')[0]}:${client.asdu}`;
                updateClientPointer(ipAsdu, eventBuffer.length);
              }
            } catch (e) {
              plugin.log(`Ошибка команды для клиента ${clientId}: ${util.inspect(e)}`, 2);
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
    const maxBufferSize = Number(params.maxBufferSize) || 1000; // Размер буфера в количестве сообщений
    if (eventBuffer.length >= maxBufferSize) {
      eventBuffer.shift();
      for (const ipAsdu in clientPointers) {
        if (clientPointers[ipAsdu] > 0) {
          clientPointers[ipAsdu]--;
        }
      }
      plugin.log(`Переполнение буфера событий, удалено самое старое сообщение (максимум ${maxBufferSize} сообщений)`, 2);
    }
    eventBuffer.push(event);
  }

  function updateClientPointer(ipAsdu, position) {
    clientPointers[ipAsdu] = position;
  }

  async function sendBufferedEvents(clientId, startFromPointer = null) {
    const client = clients[clientId];
    if (!client) {
      plugin.log(`Клиент ${clientId} не найден, невозможно отправить буферизованные события`, 2);
      return;
    }

    const ip = clientId.split(':')[0];
    const ipAsdu = client.asdu ? `${ip}:${client.asdu}` : `${clientId}:unknown`;
    const pointer = startFromPointer !== null ? startFromPointer : (clientPointers[ipAsdu] || 0);

    const eventsToSend = eventBuffer.slice(pointer).map(event => ({
      ...event,
      cot: 48
    }));

    if (eventsToSend.length > 0) {
      try {
        const batches = chunkArray(eventsToSend, MAX_BATCH_SIZE);
        for (let i = 0; i < batches.length; i++) {
          const batch = batches[i];
          const success = server.sendCommands(clientId, batch);
          await sleep(10);
          if (success) {
            plugin.log(`Отправлено ${batch.length} буферизованных событий клиенту ${clientId} (asdu=${client.asdu || 'unknown'}): ${util.inspect(batch)}`, 2);
          } else {
            plugin.log(`Не удалось отправить ${batch.length} буферизованных событий клиенту ${clientId} (asdu=${client.asdu || 'unknown'})`, 2);
          }
        }
        updateClientPointer(ipAsdu, eventBuffer.length);
      } catch (e) {
        plugin.log(`Ошибка отправки буферизованных событий клиенту ${clientId} (asdu=${client.asdu || 'unknown'}): ${util.inspect(e)}`, 2);
      }
    } else {
      plugin.log(`Нет буферизованных событий для отправки клиенту ${clientId} (asdu=${client.asdu || 'unknown'})`, 2);
    }
  }

  async function sendCurrentValues(clientId) {
    const client = clients[clientId];
    if (!client) {
      plugin.log(`Клиент ${clientId} не найден, невозможно отправить текущие значения`, 2);
      return;
    }

    const currentValues = Object.values(curData).map(item => ({
      ...item,
      cot: 20
    }));

    if (currentValues.length > 0) {
      try {
        const batches = chunkArray(currentValues, MAX_BATCH_SIZE);
        for (let i = 0; i < batches.length; i++) {
          const batch = batches[i];
          const success = server.sendCommands(clientId, batch);
          await sleep(10);
          if (success) {
            plugin.log(`Отправлено ${batch.length} текущих значений клиенту ${clientId} (asdu=${client.asdu || 'unknown'}): ${util.inspect(batch)}`, 2);
          } else {
            plugin.log(`Не удалось отправить ${batch.length} текущих значений клиенту ${clientId} (asdu=${client.asdu || 'unknown'})`, 2);
          }
        }
      } catch (e) {
        plugin.log(`Ошибка отправки текущих значений клиенту ${clientId}: ${util.inspect(e)}`, 2);
      }
    } else {
      plugin.log(`Нет текущих значений для отправки клиенту ${clientId} (asdu=${client.asdu || 'unknown'})`, 2);
    }
  }

  async function sendCurrentChanges(clientId) {
    const client = clients[clientId];
    if (!client) {
      plugin.log(`Клиент ${clientId} не найден, невозможно отправить текущие изменения`, 2);
      return;
    }

    if (curDataArr.length > 0) {
      try {
        const batches = chunkArray(curDataArr, MAX_BATCH_SIZE);
        for (let i = 0; i < batches.length; i++) {
          const batch = batches[i];
          const success = server.sendCommands(clientId, batch);
          await sleep(10);
          if (success) {
            plugin.log(`Отправлено ${batch.length} текущих изменений клиенту ${clientId} (asdu=${client.asdu || 'unknown'}): ${util.inspect(batch)}`, 2);
          } else {
            plugin.log(`Не удалось отправить ${batch.length} текущих изменений клиенту ${clientId} (asdu=${client.asdu || 'unknown'})`, 2);
          }
        }
      } catch (e) {
        plugin.log(`Ошибка отправки текущих изменений клиенту ${clientId}: ${util.inspect(e)}`, 2);
      }
    } else {
      plugin.log(`Нет текущих изменений для отправки клиенту ${clientId} (asdu=${client.asdu || 'unknown'})`, 2);
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
          dataToSend.push({ ...data, cot: 1 });
        }
      }
    });

    if (dataToSend.length > 0) {
      const status = server.getStatus();
      if (status.connectedClients.length > 0) {
        status.connectedClients.forEach(clientId => {
          const client = clients[clientId];
          if (client?.activated === 1) {
            try {
              const batches = chunkArray(dataToSend, MAX_BATCH_SIZE);
              for (let i = 0; i < batches.length; i++) {
                const batch = batches[i];
                const success = server.sendCommands(clientId, batch);
                sleep(10).then(() => {
                  if (success) {
                    plugin.log(`Отправлено ${batch.length} периодических данных за интервал ${intervalMs / 60000} минут клиенту ${clientId} (asdu=${client.asdu || 'unknown'}): ${util.inspect(batch)}`, 2);
                  } else {
                    plugin.log(`Не удалось отправить ${batch.length} периодических данных за интервал ${intervalMs / 60000} минут клиенту ${clientId} (asdu=${client.asdu || 'unknown'})`, 2);
                  }
                });
              }
            } catch (e) {
              plugin.log(`Ошибка отправки периодических данных за интервал ${intervalMs / 60000} минут клиенту ${clientId}: ${util.inspect(e)}`, 2);
            }
          }
        });
      } else {
        plugin.log(`Нет подключённых клиентов для отправки периодических данных за интервал ${intervalMs / 60000} минут`, 2);
      }
    }
  }

  function sendCounterValues(clientId, qcc) {
    const client = clients[clientId];
    if (!client) {
      plugin.log(`Клиент ${clientId} не найден, невозможно отправить значения счётчиков`, 2);
      return;
    }

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
          asduAddress: client.asdu || 0,
          cot: 7
        }]);
        const batches = chunkArray(counterValues, MAX_BATCH_SIZE);
        for (let i = 0; i < batches.length; i++) {
          const batch = batches[i];
          const success = server.sendCommands(clientId, batch);
          sleep(10).then(() => {
            if (success) {
              plugin.log(`Отправлено ${batch.length} значений счётчиков клиенту ${clientId} (asdu=${client.asdu || 'unknown'}): ${util.inspect(batch)}`, 2);
            } else {
              plugin.log(`Не удалось отправить ${batch.length} значений счётчиков клиенту ${clientId} (asdu=${client.asdu || 'unknown'})`, 2);
            }
          });
        }
      } catch (e) {
        plugin.log(`Ошибка отправки значений счётчиков клиенту ${clientId}: ${util.inspect(e)}`, 2);
        server.sendCommands(clientId, [{
          typeId: 101,
          ioa: 0,
          value: qcc,
          asduAddress: client.asdu || 0,
          cot: 10
        }]);
      }
    } else {
      plugin.log(`Нет значений счётчиков для отправки клиенту ${clientId} (asdu=${client.asdu || 'unknown'})`, 2);
      server.sendCommands(clientId, [{
        typeId: 101,
        ioa: 0,
        value: qcc,
        asduAddress: client.asdu || 0,
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
          plugin.log(`Очищена просроченная SBO выборка для клиента ${clientId}, ключ=${key}`, 2);
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
        plugin.log(`Ошибка в server.sendCommands: ${util.inspect(e)}`, 2);
      }
      plugin.log(`Ошибка для команды typeId=${typeId}: Неизвестный объект, ioa=${ioa}, asduAddress=${asduAddress}`, 2);
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
        plugin.log(`Ошибка в server.sendCommands: ${util.inspect(e)}`, 2);
      }
      plugin.log(`Ошибка для команды typeId=${typeId}: ${!cmdItem ? 'Нет команды в curCmd' : `Недопустимое качество (${cmdItem.quality})`}, ioa=${ioa}, asduAddress=${asduAddress}`, 2);
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
        plugin.log(`Ошибка в server.sendCommands: ${util.inspect(e)}`, 2);
      }
      plugin.log(`Недопустимое значение ${val} для typeId=${typeId}, ioa=${ioa}, asduAddress=${asduAddress}`, 2);
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
            plugin.log(`Ошибка в server.sendCommands: ${util.inspect(e)}`, 2);
          }
          plugin.log(`Недопустимое QOC ${ql} для typeId=${typeId}, ioa=${ioa}, asduAddress=${asduAddress}`, 2);
          return;
      }
      plugin.log(`Команда typeId=${typeId}, ioa=${ioa}, asduAddress=${asduAddress}: QOC=${ql} (${commandType}), execTime=${execTime === null ? 'нет' : execTime + 'ms'}`, 2);
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
        plugin.log(`Ошибка в server.sendCommands: ${util.inspect(e)}`, 2);
      }
      plugin.log(`Выборка не разрешена в прямом режиме для typeId=${typeId}: ioa=${ioa}, asduAddress=${asduAddress}`, 2);
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
        plugin.log(`Ошибка в server.sendCommands: ${util.inspect(e)}`, 2);
      }
      plugin.log(`Прямая команда не разрешена в режиме SBO для typeId=${typeId}: ioa=${ioa}, asduAddress=${asduAddress}`, 2);
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
        plugin.log(`Ошибка в server.sendCommands: ${util.inspect(e)}`, 2);
      }
      plugin.log(`SBO выборка успешна для клиента ${clientId}, ioa=${ioa}, asduAddress=${asduAddress}`, 2);
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
              plugin.log(`Отправлена команда setval: did=${did}, prop=${prop}, value=${processedVal}`, 2);
            } catch (e) {
              plugin.log(`Ошибка в plugin.send: ${util.inspect(e)}`, 2);
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
                  plugin.log(`Отправлена команда сброса: did=${did}, prop=${prop}, value=0 после ${execTime}ms`, 2);
                } catch (e) {
                  plugin.log(`Ошибка в plugin.send (сброс): ${util.inspect(e)}`, 2);
                }
              }, execTime);
            }
          } else {
            plugin.log(`Не найден элемент фильтра для asduAddress=${asduAddress}, ioa=${ioa}, невозможно отправить setval`, 2);
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
            plugin.log(`SBO операция успешна для клиента ${clientId}, ioa=${ioa}, asduAddress=${asduAddress}`, 2);
          } else {
            plugin.log(`Прямая команда успешна для клиента ${clientId}, ioa=${ioa}, asduAddress=${asduAddress}`, 2);
          }
        } catch (e) {
          plugin.log(`Ошибка отправки команды для клиента ${clientId}: ${util.inspect(e)}`, 2);
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
            plugin.log(`Ошибка в server.sendCommands: ${util.inspect(err)}`, 2);
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
          plugin.log(`Ошибка в server.sendCommands: ${util.inspect(e)}`, 2);
        }
        plugin.log(`SBO операция не удалась: Нет предварительной выборки для клиента ${clientId}, ioa=${ioa}, asduAddress=${asduAddress}`, 2);
      }
    }
  }

  const server = new IEC104Server((event, data) => {
    plugin.log(`Событие сервера: ${event}, Данные: ${JSON.stringify(data, null, 2)}`, 2);
    if (event === 'data') {
      if (data.type === 'control') {
        clients[data.clientId] = {
          activated: data.event === "activated" ? 1 : 0,
          asdu: clients[data.clientId]?.asdu || null
        };
        if (data.event === 'activated') {
          //sendCurrentChanges(data.clientId);
        } else if (data.event === 'disconnected') {
          delete sboSelections[data.clientId];
          delete clients[data.clientId];
          plugin.log(`Клиент ${data.clientId} отключён, сохранён в ipAsduToClientId для возможного восстановления`, 2);
        }
      }
      if (Array.isArray(data)) {
        data.forEach(cmd => {
          try {
            const { clientId, typeId, ioa, val, quality, bselCmd, ql, timestamp, asduAddress } = cmd;

            if (clients[clientId] && asduAddress && !clients[clientId].asdu) {
              clients[clientId].asdu = asduAddress;
              const ip = clientId.split(':')[0];
              const ipAsdu = `${ip}:${asduAddress}`;
              ipAsduToClientId[ipAsdu] = clientId;
              plugin.log(`Клиент ${clientId} привязан к ip:ASDU=${ipAsdu}`, 2);
            }

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
                handleControlCommand(clientId, 47, ioa, val, ql, undefined, asduAddress, item, cmdItem, bselCmd, sboClient, sboKey, HX5);
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
                plugin.log(`Получена команда опроса: clientId=${clientId}, ioa=${ioa}, qoi=${val}, asduAddress=${asduAddress}`, 2);
                server.sendCommands(clientId, [{
                  typeId: 100,
                  ioa: 0,
                  value: val,
                  asduAddress,
                  cot: 7
                }]);
                if (useBuffer && asduAddress) {
                  const ip = clientId.split(':')[0];
                  const ipAsdu = `${ip}:${asduAddress}`;
                  const mappedClientId = ipAsduToClientId[ipAsdu];
                  if (mappedClientId) {
                    const pointer = clientPointers[ipAsdu] || 0;
                    plugin.log(`Клиент ${clientId} найден в ipAsduToClientId, отправка буферизованных данных с позиции ${pointer}`, 2);
                    sendBufferedEvents(mappedClientId, pointer).then(() => {
                      sendCurrentValues(clientId);
                    });
                    
                  } else {
                    plugin.log(`Клиент ${clientId} не найден в ipAsduToClientId, отправка всех текущих значений`, 2);
                    sendCurrentValues(clientId);
                  }
                } else {
                  plugin.log(`Буфер выключен или нет asduAddress, отправка всех текущих значений клиенту ${clientId}`, 2);
                  sendCurrentValues(clientId);
                }
                break;
              case 101:
                plugin.log(`Получена команда опроса счётчиков: clientId=${clientId}, ioa=${ioa}, qcc=${val}, asduAddress=${asduAddress}`, 2);
                sendCounterValues(clientId, val);
                break;
              case 102:
                plugin.log(`Получена команда чтения: clientId=${clientId}, ioa=${ioa}, asduAddress=${asduAddress}`, 2);
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
                  plugin.log(`Ошибка для команды чтения: ${!item || !curitem ? "Неизвестный объект" : `Недопустимое качество (${curitem.quality})`}, ioa=${ioa}, asduAddress=${asduAddress}`, 2);
                } else {
                  server.sendCommands(clientId, [{
                    ...curitem,
                    cot: 5
                  }]);
                }
                break;
              case 103:
                plugin.log(`Получена команда синхронизации времени: clientId=${clientId}, ioa=${ioa}, timestamp=${timestamp}, asduAddress=${asduAddress}`, 2);
                server.sendCommands(clientId, [{
                  typeId: 103,
                  ioa: 0,
                  timestamp,
                  asduAddress,
                  cot: 7
                }]);
                break;
              default:
                plugin.log(`Неизвестный тип команды typeId=${typeId}, clientId=${clientId}, ioa=${ioa}, value=${val}, asduAddress=${asduAddress}`, 2);
                server.sendCommands(clientId, [{
                  typeId,
                  ioa,
                  value: val,
                  asduAddress,
                  cot: 10
                }]);
            }
          } catch (e) {
            plugin.log(`ОШИБКА команды: ${util.inspect(e)}`, 2);
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
    plugin.log('ЗАВЕРШЕНИЕ ПЛАГИНА', 2);
    plugin.exit();
  }

  function filterExtraChannels() {
    let res = { did_prop: [] };
    if (!Array.isArray(extraChannels)) {
      plugin.log('Ошибка: extraChannels не является массивом', 2);
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
          plugin.log(`Установлена периодическая задача на интервал ${intervalMs / 60000} минут`, 2);
        }
        periodicTasks[intervalMs].didProps.push(didProp);
      }
    });
    return res;
  }
};