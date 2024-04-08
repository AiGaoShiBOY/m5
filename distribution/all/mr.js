const distribution = require('../../distribution');
const {id, serialize, deserialize, log} = require('../util/util');

const mr = function(config) {
  let context = {};
  context.gid = config.gid || 'all';
  let distribution = global.distribution;

  return {
    exec: (configuration, callback) => {
      /* Change this with your own exciting Map Reduce code! */
      const keys = configuration.keys;
      if(keys === undefined){
        callback(new Error('Configuration invalid'), null);
      }
      const mapper = configuration.map;
      if(!mapper){
        callback(new Error('Configuration invalid', null));
      }
      const reducer = configuration.reduce;
      if(!reducer){
        callback(new Error('Configuration invalid', null));
      }
      const mrId = 'mr' + id.getSID(configuration);
      
      const mrService = {
        map: mapWrapper,
      }

      distribution[context.gid].routes.put(mrService, mrId, (e, v)=>{
        const message = [keys, context.gid, mapper];
        const remote = {
          service: mrId,
          method: 'map',
        };
        log(JSON.stringify(e) + JSON.stringify(v));
        distribution[context.gid].comm.send(message, remote, (e, v) =>{
          log(JSON.stringify(e) + JSON.stringify(v));
          callback(null, []);
        })
      });
    },
  };
};

const mapWrapper = function(keys, gid, mapper, callback){
  let cnt = keys.length;
  // for every key 
  keys.forEach(key => {
    // get the key from local storage
    global.distribution.local.store.get({key: key, gid: gid}, (e, v) => {
      // if the key stores in the local storage
      if(v){
        // apply the mapper on the data
        const mappedData = mapper(key, v);
        // store the data 
        global.distribution.local.store.put(mappedData, {key: key, gid: gid}, (e, v)=>{
          cnt--;
          if(cnt === 0){
            callback(null, 1);
          }
        })
      }else{
        cnt--;
        if(cnt === 0){
          callback(null, 1);
        }
      }
    })
  });
};

module.exports = mr;
