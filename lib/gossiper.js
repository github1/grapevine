var PeerState = require('./peer_state');
var Scuttle = require('./scuttle');
var EventEmitter = require('events').EventEmitter;
var net = require('net');
var util = require('util');
var child_process = require('child_process');
var debug = require('debug')('grapevine_Gossiper');
var nssocket = require('nssocket');
var ServerAdapter = require('./ServerAdapter')
var SocketAdapter = require('./SocketAdapter')

module.exports = Gossiper

/*
  *	TODO: complete documentation
  *	
  *	the flow of data is as follows:
  *	1. every peer is a server
  *	2. a peer randomly connects to another peer with a message of type REQUEST
  *	3. the other peer responds with a message of type FIRST_RESPONSE
  *	4. the initiating peer responds with a message of type SECOND_RESPONSE
  *
  *	@class
  */
util.inherits(Gossiper, EventEmitter);
function Gossiper(options) {
	EventEmitter.call(this);

	if (typeof options === 'number') {
		options = {
			port: options
		};
	}

	options = options || {};

	if (typeof options.port !== 'number')
		throw new Error('must specify a port');

	// TODO eek refactor:
	this._newServerAdapter = options.newServerAdapter || function () { return new ServerAdapter(options); };
	this._newSocketAdapter = options.newSocketAdapter || function () { return new SocketAdapter(options); };
	
	this.address = options.address || '127.0.0.1';
	this.port = options.port;
	this.secure = options.secure;

	// TODO my peer state and peer name are modified on start()
	// need to refactor so the assignment and declaration is only in one place

	this.peer_name = this._generatePeerName();

	this.my_state = new PeerState(this.port, this.address);
	this.my_state.address = this.address;
	this.my_state.port = this.port;
	this.listenToExpiredKeys(this.my_state);

	this.peers = {};	

	// TODO: consider removing my_state from peers
	//	thus eliminating checks for self in various peer iterations
	this.peers[this.peer_name] = this.my_state;

	this.beatHeart = true;
	this.emitUpdateOnHeartBeat = options.emitUpdateOnHeartBeat || false;
	this.scuttle = new Scuttle(this.peers);

	this.seeds = options.seeds || [];

	for (var i = 0; i < this.seeds.length; i++) {
		if (this.seeds[i] === this.peer_name)
			throw new Error('cannot specify self as seed')
	}

	// TODO: another ugly hack :(
	var seeds = {};

	for (var i = 0; i < this.seeds.length; i++)
		seeds[this.seeds[i]] = undefined;

	this.handleNewPeers(seeds);

	// hook these two to socket events on both incoming and outgoing connections:
	var self = this;

	this._onSocketData = function (message, socket) {
		var reply = self.handleMessage(message);
		
		if (reply) {
			socket.write(reply);
		}

		if (message.type !== Gossiper.REQUEST)
			socket.end();
	};
 
	this._onSocketError = function (e) {
		debug('socket error: %s', e);
	};
}

Gossiper.prototype._generatePeerName = function() {
	if (net.isIPv6(this.address)) {
		return '[' + this.address + ']' + ':' + this.port;
	}

	return this.address + ':' + this.port;
}

Gossiper.prototype.start = function(callback) {

	var self = this;

	this.server = this._newServerAdapter();

	this.server.on('listening', start);

	this.server.on('connection', onConnection);

	this.server.on('error', onError);

	this.server.listen(this.port, this.address);
	
	if (this.beatHeart) {
		this.heartBeatTimer = setInterval(function() {
			self.my_state.beatHeart();
		}, 1000);
	}

	this.gossipTimer = setInterval(function() {
		self.gossip();
	}, 1000);

	function onConnection(socket) {
		socket.on('data', self._onSocketData);
		socket.on('error', self._onSocketError);
	}

	function onError(e) {
		debug(e)
	}

	function start(err) {
		debug('%s started', self.peer_name);

		if (callback)
			callback(err, self);

		self.started = true;
		self.emit('started', self);
	}
}

Gossiper.prototype.stop = function(callback) {
	clearInterval(this.heartBeatTimer);
	clearInterval(this.gossipTimer);
	var self = this;
	if (this.server) {
		
		this.server.once('close', function () {
			self.server = undefined
			self.started = false;
			callback();
		});

		this.server.close();	
	} else {
		this.started = false;
		setImmediate(callback);
	}
}

// The method of choosing which peer(s) to gossip to is borrowed from Cassandra.
// They seemed to have worked out all of the edge cases
// http://wiki.apache.org/cassandra/ArchitectureGossip
Gossiper.prototype.gossip = function() {
	this.emit('gossip start');

	var now = Date.now();

	for (var p in this.peers)
		this.peers[p].expireLocalKeys(now);

	var livePeers = this.livePeers();

	// Find a live peer to gossip to
	var livePeer;

	if (livePeers.length > 0) {
		livePeer = this.chooseRandom(livePeers);
		this.gossipToPeer(livePeer);
	}

	var deadPeers = this.deadPeers();

	// Possilby gossip to a dead peer
	var prob = deadPeers.length / (livePeers.length + 1)
	if (Math.random() < prob) {
		var deadPeer = this.chooseRandom(deadPeers);
		this.gossipToPeer(deadPeer);
	}

	//TODO this following comment is from the original fork, i dont understand
	//why it says "gossip to seed" but chooses a peer from all the peers
	// Gossip to seed under certain conditions
	if (livePeer && !this.seeds[livePeer] && livePeers.length < this.seeds.length) {
		if (Math.random() < (this.seeds / this.peers.length)) {
			var p = this.chooseRandom(this.allPeers())
			this.gossipToPeer(p);
		}
	}

	// Check health of peers
	for (var i in this.peers) {
		var peer = this.peers[i];
		if (peer !== this.my_state) {
			peer.isSuspect();
		}
	}
}

Gossiper.prototype.chooseRandom = function(peers) {
	// Choose random peer to gossip to
	var i = Math.floor(Math.random() * 1000000) % peers.length;
	return this.peers[peers[i]];
}

Gossiper.prototype.gossipToPeer = function(peer) {
	
	if (debug.enabled) {
		debug('%s => %s', this.peer_name, peer.name);
	}

	var socket = this._newSocketAdapter();

	socket.on('data', this._onSocketData);

	var self = this
	socket.on('connect', function() {
		socket.write(self.requestMessage());
		self.emit('gossip', peer);
	})

	socket.on('error', this._onSocketError);

	socket.connect(peer.port, peer.address);
}

Gossiper.REQUEST = 0;
Gossiper.FIRST_RESPONSE = 1;
Gossiper.SECOND_RESPONSE = 2;

Gossiper.prototype.handleMessage = function(msg) {

	switch (msg.type) {	 
		// the request message is from the connecting client and is handled at the server peer
		case Gossiper.REQUEST:
			return this.firstResponseMessage(msg.digest, msg.psk);
		
		// the first response message is from the server peer and is handled by the client peer	
		case Gossiper.FIRST_RESPONSE:
			this.scuttle.updateKnownState(msg.updates);
			return this.secondResponseMessage(msg.request_digest);
			
		// the second response message is from the connecting client and is handled at the server peer
		case Gossiper.SECOND_RESPONSE:
			this.scuttle.updateKnownState(msg.updates);
			break;

		default:
			debug('unknown message type', msg.type)
			break;
	}
}

// MESSSAGES
Gossiper.prototype.handleNewPeers = function(newPeers) {
	var self = this;
	for (var p in newPeers) {
		var peer_info;
		// TODO can this be done without regex?
		var m = p.match(/\[(.+)\]:([0-9]+)/);
		var address;
		var port;

		if (m) {
			address = m[1];
			port = m[2];
		} else {
			m = p.split(':');
			address = m[0];
			port = m[1];
		}

		var tp = new PeerState(parseInt(port), address);

		tp.metadata = newPeers[p]

		this.peers[tp.name] = tp;

		this.emit('new_peer', tp);

		this.listenToPeer(tp);
	}
}

Gossiper.prototype.listenToPeer = function(peer) {
	var self = this;

	if (peer.name === this.peer_name)
		throw new Error('cannot listen to itself')

	var peerName = peer.name;

	this.listenToExpiredKeys(peer)

	peer.on('update', function(k, v, expires) {

		if (k !== '__heartbeat__')
			self.emit('update', peerName, k, v, expires);
		else if (self.emitUpdateOnHeartBeat)
			self.emit('update', peerName, k, v, expires); // heartbeats are disabled by default but it can be changed so this takes care of that
	});

	peer.on('peer_alive', function() {
		self.emit('peer_alive', peerName);
	});

	peer.on('peer_failed', function() {
		self.emit('peer_failed', peerName);
	});
}

Gossiper.prototype.listenToExpiredKeys = function(peer) {

	var self = this;
	peer.on('expire', function(k, v, expires) {
		self.emit('expire', peer.name, k, v, expires);
	});
}

Gossiper.prototype.requestMessage = function() {
	var m = {
		type: Gossiper.REQUEST,
		digest: this.scuttle.digest()
	};

	if (this.secure && this.presharedKey) {
		debug('send request message with preshared key')
		m.psk = this.presharedKey;
	}

	return m;
};

Gossiper.prototype.firstResponseMessage = function(peer_digest, psk) {
	// if we are secure and the psk is not the same as our, return an empty message
	// do not discover new peers from this peer and so on
	if (this.secure && this.presharedKey !== psk) {
		debug('Unauthorized peer!')
		return {}
	}

	var sc = this.scuttle.scuttle(peer_digest)

	this.handleNewPeers(sc.new_peers)

	var m = {
		type: Gossiper.FIRST_RESPONSE,
		request_digest: sc.requests,
		updates: sc.deltas
	};

	return m;
};

Gossiper.prototype.secondResponseMessage = function(requests) {
	var m = {
		type: Gossiper.SECOND_RESPONSE,
		updates: this.scuttle.fetchDeltas(requests)
	};
	return m;
};

Gossiper.prototype.setLocalState = function(k, v, expires) {
	this.my_state.updateLocal(k, v, expires);
}

Gossiper.prototype.getLocalState = function(k) {
	return this.my_state.getValue(k);
}

Gossiper.prototype.peerKeys = function(peer) {
	if (!peer) throw new Error('must specify a peer')
	return this.peers[peer].getKeys();
}

Gossiper.prototype.peerValue = function(peer, k) {
	if (!peer) throw new Error('must specify a peer')
	if (!k) throw new Error('must specify a key')

	return this.peers[peer].getValue(k);
}

Gossiper.prototype.allPeers = function() {
	var keys = [];
	for (var k in this.peers) {
		var peer = this.peers[k];
		if (peer !== this.my_state)
			keys.push(k)
	}
	return keys;
}

Gossiper.prototype.livePeers = function() {
	var keys = [];

	for (var k in this.peers) {
		var peer = this.peers[k];
		if (peer !== this.my_state && peer.alive) {
			keys.push(k)
		}
	}

	return keys;
}

Gossiper.prototype.deadPeers = function() {
	var keys = [];

	for (var k in this.peers) {
		var peer = this.peers[k];
		if (peer !== this.my_state && !peer.alive) {
			keys.push(k)
		}
	}

	return keys;
}
