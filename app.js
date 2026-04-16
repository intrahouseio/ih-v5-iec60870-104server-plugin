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
  const execTimers = {};
  let filter = filterExtraChannels();

  subExtraChannels(filter);

  const eventBuffer = [];
  const clientPointers = {};
  const sboSelections = {};
  const SBO_TIMEOUT = 30000;
  const MAX_BATCH_SIZE = 50; // увеличено для эффективности

  // 🚚 Приоритетные очереди отправки
  const PRIORITY_HIGH = 0;      // Подтверждения, команды управления
  const PRIORITY_MEDIUM = 1;    // Ответы на запросы
  const PRIORITY_LOW = 2;       // Текущие значения
  const PRIORITY_BACKGROUND = 3; // Телеметрия, периодика

  let priorityQueues = {
    0: [], // high
    1: [], // medium
    2: [], // low
    3: []  // background
  };

  let isProcessing = false;
  const BATCHES_PER_TICK = 5;
  const MAX_TOTAL_QUEUE_SIZE = 500;
  const PROCESS_INTERVAL_MS = 10;

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

  // 🎯 Определяем приоритет батча по его содержимому
  function getBatchPriority(batch) {
    const firstCmd = batch[0];
    if (!firstCmd) return PRIORITY_BACKGROUND;

    // HIGH: подтверждения, команды управления
    if ([7, 10].includes(firstCmd.cot)) {
      return PRIORITY_HIGH;
    }
    if ([45, 46, 47, 48, 49, 50, 51, 58, 59, 60, 61, 62, 63, 64].includes(firstCmd.typeId)) {
      return PRIORITY_HIGH;
    }

    // MEDIUM: ответы на запросы
    if ([5, 20, 37].includes(firstCmd.cot)) {
      return PRIORITY_MEDIUM;
    }
    if ([100, 101, 102, 103].includes(firstCmd.typeId)) {
      return PRIORITY_MEDIUM;
    }

    // LOW: текущие значения
    if (firstCmd.cot === 20) {
      return PRIORITY_LOW;
    }

    // BACKGROUND: всё остальное (телеметрия)
    return PRIORITY_BACKGROUND;
  }

  // 📥 Добавляет команды в приоритетную очередь
  function enqueueCommands(clientId, commands) {
    if (!Array.isArray(commands)) {
      commands = [commands];
    }

    const client = clients[clientId];
    if (!client || !client.activated) {
      plugin.log(`Клиент ${clientId} не активен — команды не добавлены в очередь`, 3);
      return;
    }

    // Группируем в батчи
    const batches = chunkArray(commands, MAX_BATCH_SIZE);

    const priorityCounts = {};

    batches.forEach(batch => {
      const priority = getBatchPriority(batch);
      priorityCounts[priority] = (priorityCounts[priority] || 0) + 1;

      priorityQueues[priority].push({
        clientId,
        batch,
        attempts: 0,
        firstAttemptTime: Date.now(),
        priority
      });
    });

    // Логируем распределение по приоритетам
    let logMsg = `📥 Добавлено ${batches.length} батчей для ${clientId}: `;
    for (let p of [0, 1, 2, 3]) {
      if (priorityCounts[p]) logMsg += `P${p}:${priorityCounts[p]} `;
    }
    plugin.log(logMsg, 3);

    // Запускаем обработчик, если не запущен
    if (!isProcessing) {
      processQueue();
    }
  }

  // 🔄 Обрабатывает очередь по приоритетам
  async function processQueue() {
    if (isProcessing) return;
    isProcessing = true;

    // Защита от переполнения
    const totalSize = Object.values(priorityQueues).reduce((sum, q) => sum + q.length, 0);
    if (totalSize > MAX_TOTAL_QUEUE_SIZE) {
      let toDrop = totalSize - MAX_TOTAL_QUEUE_SIZE;
      for (let p = PRIORITY_BACKGROUND; p >= PRIORITY_HIGH; p--) {
        if (toDrop <= 0) break;
        const dropFromThis = Math.min(toDrop, priorityQueues[p].length);
        if (dropFromThis > 0) {
          priorityQueues[p].splice(0, dropFromThis);
          toDrop -= dropFromThis;
          plugin.log(`⚠️ Переполнение: удалено ${dropFromThis} батчей с приоритетом ${p}`, 2);
        }
      }
    }

    let processed = 0;

    // Обрабатываем от высшего приоритета к низшему
    for (let priority = PRIORITY_HIGH; priority <= PRIORITY_BACKGROUND; priority++) {
      while (processed < BATCHES_PER_TICK && priorityQueues[priority].length > 0) {
        const task = priorityQueues[priority].shift();
        if (!task) break;

        const { clientId, batch, attempts, firstAttemptTime } = task;

        const client = clients[clientId];
        if (!client || !client.activated) {
          plugin.log(`🔌 Клиент ${clientId} отключён — батч P${priority} отброшен (попыток: ${attempts})`, 3);
          continue;
        }

        // Таймаут жизни батча
        if (Date.now() - firstAttemptTime > 30000) {
          plugin.log(`⏳ Батч P${priority} для ${clientId} устарел (>30сек) — отбрасываем после ${attempts} попыток`, 3);
          continue;
        }

        try {
          const newAttempts = attempts + 1;
          const success = server.sendCommands(clientId, batch);

          if (success) {
            plugin.log(`✅ Отправлен батч (${batch.length} команд, P${priority}) клиенту ${clientId} (попытка ${newAttempts})`, 3);
          } else {
            if (newAttempts >= 3) {
              plugin.log(`❌ Батч P${priority} для ${clientId} не отправлен после ${newAttempts} попыток — отбрасываем`, 3);
            } else {
              priorityQueues[priority].unshift({ ...task, attempts: newAttempts });
              plugin.log(`⚠️ Ошибка отправки P${priority} — возвращаем в очередь (попытка ${newAttempts})`, 4);
            }
          }
        } catch (e) {
          const newAttempts = attempts + 1;
          if (newAttempts >= 3) {
            plugin.log(`❌ Исключение при отправке P${priority} — отбрасываем после ${newAttempts} попыток: ${e.message}`, 3);
          } else {
            priorityQueues[priority].unshift({ ...task, attempts: newAttempts });
            plugin.log(`⚠️ Исключение P${priority} (попытка ${newAttempts}): ${e.message}`, 4);
          }
        } finally {
          // Обновляем указатель буфера для событий
          if (client.asdu && batch.some(cmd => cmd.cot === 3 || cmd.cot === 48)) {
            const ipAsdu = `${clientId.split(':')[0]}:${client.asdu}`;
            updateClientPointer(ipAsdu, eventBuffer.length);
          }
        }

        processed++;
      }
      if (processed >= BATCHES_PER_TICK) break;
    }

    isProcessing = false;
    setTimeout(processQueue, PROCESS_INTERVAL_MS);
  }

  function subExtraChannels(filter) {
    plugin.onSub('devices', filter, data => {
      const newEvents = [];
      data.forEach(item => {
        const curitem = filter[item.did + "." + item.prop];
        if (!curitem) return;

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
            timestamp: Date.now() + Number(params.tzondevice.slice(3)) * (3600000),
            quality: item.chstatus > 0 ? 128 : 0,
            cot: 3
          };
          curData[curitem.did + '.' + curitem.prop] = event;
          newEvents.push(event);
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
            timestamp: Date.now() + Number(params.tzondevice.slice(3)) * (3600000),
            cot: 3
          };
          curCmd[curitem.did + '.' + curitem.prop] = cmd;
        }
      });

      if (newEvents.length > 0) {
        curDataArr = [...curDataArr, ...newEvents];

        const status = server.getStatus();
        if (status.connectedClients.length > 0) {
          status.connectedClients.forEach(clientId => {
            const client = clients[clientId];
            if (client?.activated === 1) {
              enqueueCommands(clientId, newEvents);
            }
          });
        }
      }
    });
  }

  plugin.onChange('extra', async (recs) => {
    Object.values(periodicTasks).forEach(task => clearInterval(task.timerId));

    extraChannels = await plugin.extra.get();
    curData = {};
    curCmd = {};
    curDataArr = [];
    priorityQueues = { 0: [], 1: [], 2: [], 3: [] };
    isProcessing = false;
    periodicTasks = {};
    filter = filterExtraChannels();
    subExtraChannels(filter);
  });

  function addToEventBuffer(event) {
    const maxBufferSize = Number(params.maxBufferSize) || 1000;
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
        enqueueCommands(clientId, eventsToSend);
        updateClientPointer(ipAsdu, eventBuffer.length);
      } catch (e) {
        plugin.log(`Ошибка отправки буферизованных событий клиенту ${clientId}: ${util.inspect(e)}`, 2);
      }
    } else {
      plugin.log(`Нет буферизованных событий для отправки клиенту ${clientId}`, 2);
    }
  }

  async function sendCurrentValues(clientId) {
    const client = clients[clientId];
    if (!client) {
      plugin.log(`Клиент ${clientId} не найден`, 2);
      return;
    }

    const currentValues = Object.values(curData).map(item => ({
      ...item,
      cot: 20
    }));

    if (currentValues.length > 0) {
      try {
        enqueueCommands(clientId, currentValues);
      } catch (e) {
        plugin.log(`Ошибка отправки текущих значений клиенту ${clientId}: ${util.inspect(e)}`, 2);
      }
    } else {
      plugin.log(`Нет текущих значений для отправки клиенту ${clientId}`, 2);
    }
  }

  async function sendCurrentChanges(clientId) {
    const client = clients[clientId];
    if (!client) {
      plugin.log(`Клиент ${clientId} не найден`, 2);
      return;
    }

    if (curDataArr.length > 0) {
      try {
        enqueueCommands(clientId, curDataArr);
      } catch (e) {
        plugin.log(`Ошибка отправки текущих изменений клиенту ${clientId}: ${util.inspect(e)}`, 2);
      }
    } else {
      plugin.log(`Нет текущих изменений для отправки клиенту ${clientId}`, 2);
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
              enqueueCommands(clientId, dataToSend);
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
      plugin.log(`Клиент ${clientId} не найден`, 2);
      return;
    }

    enqueueCommands(clientId, [{
      typeId: 101,
      ioa: 0,
      value: qcc,
      asduAddress: client.asdu || 0,
      cot: 7
    }]);

    const counterValues = Object.values(curData)
      .filter(item => item.typeId === 15)
      .map(item => ({
        ...item,
        cot: qcc >= 2 && qcc <= 5 ? 37 : 20
      }));

    if (counterValues.length > 0) {
      try {
        enqueueCommands(clientId, counterValues);
      } catch (e) {
        plugin.log(`Ошибка отправки значений счётчиков клиенту ${clientId}: ${util.inspect(e)}`, 2);
        enqueueCommands(clientId, [{
          typeId: 101,
          ioa: 0,
          value: qcc,
          asduAddress: client.asdu || 0,
          cot: 10
        }]);
      }
    } else {
      plugin.log(`Нет значений счётчиков для отправки клиенту ${clientId}`, 2);
      enqueueCommands(clientId, [{
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


  function handleControlCommand(
    clientId,
    typeId,
    ioa,
    val,
    ql,
    timestamp,
    asduAddress,
    item,
    cmdItem,
    bselCmd,
    sboClient,
    sboKey,
    responseTypeId
  ) {

    // -----------------------------
    // 1. Проверка объекта
    // -----------------------------
    if (!item) {
      enqueueCommands(clientId, [{
        typeId,
        ioa,
        value: val,
        timestamp,
        asduAddress,
        cot: 10
      }]);
      plugin.log(`Неизвестный объект ioa=${ioa}`, 2);
      return;
    }

    if (!cmdItem) {
      enqueueCommands(clientId, [{
        typeId,
        ioa,
        value: val,
        timestamp,
        asduAddress,
        cot: 10
      }]);
      plugin.log(`Нет команды в curCmd ioa=${ioa}`, 2);
      return;
    }

    // -----------------------------
    // 2. Валидация
    // -----------------------------
    if (!validateCommandValue(typeId, val)) {
      enqueueCommands(clientId, [{
        typeId,
        ioa,
        value: val,
        timestamp,
        asduAddress,
        cot: 10
      }]);
      plugin.log(`Недопустимое значение ${val}`, 2);
      return;
    }

    // -----------------------------
    // 3. Нормализация значения
    // -----------------------------
    let processedVal;

    if (typeId === 45 || typeId === 58) {
      processedVal = val === 1;
    } else if ([46, 47, 49, 51, 59, 60, 62, 64].includes(typeId)) {
      processedVal = Math.round(val);
    } else if ([50, 61, 63].includes(typeId)) {
      processedVal = String(val);
    } else {
      processedVal = val;
    }

    // -----------------------------
    // 4. Определение фазы SBO
    // -----------------------------
    const isExecutePhase = (cmdExec === 'direct') ||
      (cmdExec === 'select' && !bselCmd && sboClient?.[sboKey]);

    // -----------------------------
    // 5. QOC обработка (только логика)
    // -----------------------------
    let execTime = null;

    if ([45, 46, 58, 59].includes(typeId)) {
      switch (ql) {
        case 0: execTime = execDefault; break;
        case 1: execTime = execShort; break;
        case 2: execTime = execLong; break;
        case 3: execTime = null; break;
        default:
          enqueueCommands(clientId, [{
            typeId,
            ioa,
            value: val,
            timestamp,
            asduAddress,
            cot: 10
          }]);
          plugin.log(`Недопустимый QOC=${ql}`, 2);
          return;
      }
    }

    plugin.log(
      `Команда ioa=${ioa}, QOC=${ql}, execTime=${execTime}`,
      2
    );

    // =====================================================
    // 6. SELECT PHASE (только регистрация SBO)
    // =====================================================
    if (cmdExec === 'select' && bselCmd) {
      sboSelections[clientId] = sboSelections[clientId] || {};

      sboSelections[clientId][sboKey] = {
        ioa,
        asduAddress,
        timestamp: Date.now(),
        phase: 'selected'
      };

      enqueueCommands(clientId, [{
        typeId,
        ioa,
        value: processedVal,
        timestamp,
        asduAddress,
        cot: 7
      }]);

      cleanupSboSelections();

      plugin.log(`SBO SELECT OK ioa=${ioa}`, 2);
      return;
    }

    // =====================================================
    // 7. EXECUTE PHASE
    // =====================================================
    if (!isExecutePhase) {
      enqueueCommands(clientId, [{
        typeId,
        ioa,
        value: processedVal,
        timestamp,
        asduAddress,
        cot: 10
      }]);

      plugin.log(`EXECUTE без SBO ioa=${ioa}`, 2);
      return;
    }

    // -----------------------------
    // 8. Проверка SBO соответствия
    // -----------------------------
    if (cmdExec === 'select') {
      const sbo = sboSelections?.[clientId]?.[sboKey];

      if (!sbo || sbo.ioa !== ioa || sbo.asduAddress !== asduAddress) {
        enqueueCommands(clientId, [{
          typeId,
          ioa,
          value: processedVal,
          timestamp,
          asduAddress,
          cot: 10
        }]);

        plugin.log(`SBO mismatch ioa=${ioa}`, 2);
        return;
      }

      delete sboSelections[clientId][sboKey];
    }

    // =====================================================
    // 9. EXECUTION (основная логика)
    // =====================================================

    try {
      // --- отправка в IEC ---
      enqueueCommands(clientId, [{
        typeId,
        ioa,
        value: processedVal,
        timestamp,
        asduAddress,
        cot: 7
      }]);

      // --- IH side-effect ---
      const itemKey = `${asduAddress}.${ioa}`;
      const filterItem = filter[itemKey];

      if (filterItem) {
        const { did, prop } = filterItem;

        plugin.send({
          type: 'command',
          command: 'setval',
          did,
          prop,
          value: val
        });

        plugin.log(`IH setval ioa=${ioa}`, 2);

        // =================================================
        // 10. RESET (ТОЛЬКО НА EXECUTE)
        // =================================================
        if (
          execTime !== null &&
          execTime > 0
        ) {
          const timerKey = `${clientId}_${asduAddress}_${ioa}_${typeId}`;

          if (execTimers[timerKey]) {
            clearTimeout(execTimers[timerKey]);
          }

          execTimers[timerKey] = setTimeout(() => {
            try {
              plugin.send({
                type: 'command',
                command: 'setval',
                did,
                prop,
                value: 0
              });

              plugin.log(`RESET ioa=${ioa}`, 2);
            } finally {
              delete execTimers[timerKey];
            }
          }, execTime);
        }

      } else {
        plugin.log(`filterItem not found ioa=${ioa}`, 2);
      }

    } catch (e) {
      plugin.log(`ERROR command ioa=${ioa}: ${util.inspect(e)}`, 2);

      enqueueCommands(clientId, [{
        typeId,
        ioa,
        value: processedVal,
        timestamp,
        asduAddress,
        cot: 10
      }]);
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
                handleControlCommand(clientId, 47, ioa, val, ql, undefined, asduAddress, item, cmdItem, bselCmd, sboClient, sboKey, 5); // ✅ Исправлено HX5 → 5
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
                enqueueCommands(clientId, [{
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
                  enqueueCommands(clientId, [{
                    typeId: item?.ioObjMtype || 1,
                    ioa,
                    value: null,
                    asduAddress,
                    quality: 0x40,
                    cot: 47
                  }]);
                  plugin.log(`Ошибка для команды чтения: ${!item || !curitem ? "Неизвестный объект" : `Недопустимое качество (${curitem.quality})`}, ioa=${ioa}, asduAddress=${asduAddress}`, 2);
                } else {
                  enqueueCommands(clientId, [{
                    ...curitem,
                    cot: 5
                  }]);
                }
                break;
              case 103:
                plugin.log(`Получена команда синхронизации времени: clientId=${clientId}, ioa=${ioa}, timestamp=${timestamp}, asduAddress=${asduAddress}`, 2);
                enqueueCommands(clientId, [{
                  typeId: 103,
                  ioa: 0,
                  timestamp,
                  asduAddress,
                  cot: 7
                }]);
                break;
              default:
                plugin.log(`Неизвестный тип команды typeId=${typeId}, clientId=${clientId}, ioa=${ioa}, value=${val}, asduAddress=${asduAddress}`, 2);
                enqueueCommands(clientId, [{
                  typeId,
                  ioa,
                  value: val,
                  asduAddress,
                  cot: 10
                }]);
            }
          } catch (e) {
            plugin.log(`ОШИБКА команды: ${util.inspect(e)}`, 2);
            enqueueCommands(clientId, [{
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