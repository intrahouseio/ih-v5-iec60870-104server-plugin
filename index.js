const util = require("util");

//const plugin = require("ih-plugin-api")();
const modbus = require("./app");

(async () => {  
  let plugin;
  try {
    
    const opt = getOptFromArgs();
    const pluginapi = opt && opt.pluginapi ? opt.pluginapi : 'ih-plugin-api';
    plugin = require(pluginapi+'/index.js')();
    plugin.log("IEC 60870-5-104 server plugin has started.");
    plugin.params = await plugin.params.get();
    plugin.log('Received params '+ util.inspect(plugin.params));
     // Получить каналы для публикации
     plugin.extraChannels = await plugin.extra.get();
     plugin.log('Received extra channels...');
    if (plugin.extraChannels.length > 0) {
      plugin.log(`Received ${plugin.extraChannels.length} extra channels...`);
    } else {
      plugin.log('Empty extra channels list!');
      process.exit(2);
    }

    modbus(plugin);
  } catch (err) {
    plugin.exit(8, `Error! Message: ${util.inspect(err)}`);
  }
})();

function getOptFromArgs() {
  let opt;
  try {
    opt = JSON.parse(process.argv[2]); //
  } catch (e) {
    opt = {};
  }
  return opt;
}