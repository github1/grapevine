var Gossiper = require('../lib/gossiper')

var seed1 = new Gossiper({ port: 9000, seeds: [] });
seed1.start();

var seed2 = new Gossiper({ port: 9001, seeds: [] });
seed2.start();

var n = 0;
var gs = [];
var count = 100;
var peers_done = 0;
var setup_peer = function(this_peer) {
  var n = 0;
  this_peer.on('new_peer', function() { 
    n++;
    if(n == 100) {
      console.log('peer done');
      peers_done++;
      if(peers_done == 100) {
        console.log("all peers know about each other");
        process.exit();
      }
    }
  });
}

for(var i = 9101; i <= 9101+count;i++) {
  var g = gs[i] = new Gossiper({ port: i, seeds: ['127.0.0.1:9000', '127.0.0.1:9001'] });
  setup_peer(g);
  g.start();
}
