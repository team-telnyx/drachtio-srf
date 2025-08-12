const Emitter = require('events');
const net = require('net');
const tls = require('tls');
const uuidV4 = require('uuid-random') ;
const debug = require('debug')('drachtio:agent');
const noop = require('node-noop').noop;
const CRLF = '\r\n' ;
const assert = require('assert');
const DEFAULT_PING_INTERVAL = 15000;
const MIN_PING_INTERVAL = 5000;
const MAX_PING_INTERVAL = 300000;


module.exports = class WireProtocol extends Emitter {

  constructor(opts) {
    super() ;

    this._logger = opts.logger || noop ;
    this.mapIncomingMsg = new Map() ;

    this.enablePing = false;
    this.pingInterval = DEFAULT_PING_INTERVAL;
    this.mapTimerPing = new Map();
  }

  connect(opts) {
    // inbound connection to drachtio server
    let socket;
    assert.ok(typeof this.server === 'undefined', 'WireProtocol#connect: cannot be both client and server');
    this.host = opts.host ;
    this.port = opts.port ;
    this.reconnectOpts = opts.reconnect || {} ;
    this.reconnectVars = {} ;
    this._evalPingOpts(opts);
    if (opts.tls) {
      debug(`wp connecting (tls) to ${this.host}:${this.port}`);
      socket = tls.connect(opts.port, opts.host, opts.tls, () => {
        debug(`tls socket connected: ${socket.authorized}`);
      });
    }
    else {
      debug(`wp connecting (tcp) to ${this.host}:${this.port}`);
      socket = net.connect({
        port: opts.port,
        host: opts.host
      }) ;
    }
    socket.setNoDelay(true);
    socket.setKeepAlive(true);
    this.installListeners(socket) ;
  }

  _evalPingOpts(opts) {
    if (opts.enablePing === true) {
      this.enablePing = true;
      if (opts.pingInterval) {
        const interval = parseInt(opts.pingInterval);
        assert.ok(interval >= MIN_PING_INTERVAL,
          `Srf#connect: opts.pingInterval must be greater than or equal to ${MIN_PING_INTERVAL}`);
        assert.ok(interval <= MAX_PING_INTERVAL,
          `Srf#connect: opts.pingInterval must be less than or equal to ${MAX_PING_INTERVAL}`);
        this.pingInterval = interval;
      }
    }
  }

  startPinging(socket) {
    if (!this.enablePing) return;
    assert.ok(!this.mapTimerPing.has(socket), 'duplicate call to startPinging for this socket');
    const timerPing = setInterval(() => {
      if (socket && !socket.destroyed) {
        const msgId = this.send(socket, 'ping');
        this.emit('ping', {msgId, socket});
      }
    }, this.pingInterval);
    this.mapTimerPing.set(socket, timerPing);
  }

  _stopPinging(socket) {
    const timerPing = this.mapTimerPing.get(socket);
    if (timerPing) {
      clearInterval(timerPing);
      this.mapTimerPing.delete(socket);
    }
  }

  listen(opts) {
    assert.ok(typeof this.reconnectOpts === 'undefined', 'WireProtocol#listen: cannot be both server and client');
    this._evalPingOpts(opts);

    let useTls = false;
    if (opts.server instanceof net.Server) {
      this.server = opts.server ;
    }
    else if (opts.tls) {
      useTls = true;
      this.server = tls.createServer(opts.tls);
      this.server.listen(opts.port, opts.host);
    }
    else {
      this.server = net.createServer() ;
      this.server.listen(opts.port, opts.host) ;
    }
    this.server.on('listening', () => {
      debug(`wp listening on ${JSON.stringify(this.server.address())} for ${useTls ? 'tls' : 'tcp'} connections`);
      this.emit('listening');
    });

    if (useTls) {
      this.server.on('secureConnection', (socket) => {
        debug('wp tls handshake succeeded');
        socket.setKeepAlive(true);
        this.installListeners(socket);
        this.emit('connection', socket);
      });
    }
    else {
      this.server.on('connection', (socket) => {
        debug(`wp received connection from ${socket.remoteAddress}:${socket.remotePort}`);
        socket.setKeepAlive(true);
        this.installListeners(socket);
        this.emit('connection', socket);
      });
    }
    return this.server ;
  }

  get isServer() {
    return this.server ;
  }

  get isClient() {
    return !this.isServer ;
  }

  setLogger(logger) {
    this._logger = logger ;
  }
  removeLogger() {
    this._logger = function() {} ;
  }

  installListeners(socket) {
    socket.on('error', (err) => {
      debug(`wp#on error - ${err} ${this.host}:${this.port}`);
      if (this.enablePing) this._stopPinging(socket);

      // Clear any corrupted message state
      if (this.mapIncomingMsg.has(socket)) {
        const obj = this.mapIncomingMsg.get(socket);
        debug(`wp#on error - clearing ${obj.incomingMsg.length} bytes of buffered data, corruption count: ${obj.corruptionCount}`);
        this.mapIncomingMsg.delete(socket);
      }

      if (this.isServer || this.closing) {
        return;
      }

      this.emit('error', err, socket);

      // "error" events get turned into exceptions if they aren't listened for.  If the user handled this error
      // then we should try to reconnect.
      this._onConnectionGone();
    });

    socket.on('connect', () => {
      debug(`wp#on connect ${this.host}:${this.port}`);
      if (this.isClient) {
        this.initializeRetryVars() ;
      }
      this.emit('connect', socket);
    }) ;

    socket.on('close', () => {
      debug(`wp#on close ${this.host}:${this.port}`);
      if (this.enablePing) this._stopPinging(socket);
      
      // Clear any message state for this socket
      if (this.mapIncomingMsg.has(socket)) {
        const obj = this.mapIncomingMsg.get(socket);
        debug(`wp#on close - clearing ${obj.incomingMsg.length} bytes of buffered data`);
        this.mapIncomingMsg.delete(socket);
      }
      
      if (this.isClient) {
        this._onConnectionGone();
      }
      this.emit('close', socket) ;
    }) ;

    socket.on('data', this._onData.bind(this, socket)) ;
  }

  initializeRetryVars() {
    assert(this.isClient);

    this.reconnectVars.retryTimer = null;
    this.reconnectVars.retryTotaltime = 0;
    this.reconnectVars.retryDelay = 150;
    this.reconnectVars.retryBackoff = 1.7;
    this.reconnectVars.attempts = 1;
  }

  _onConnectionGone() {
    assert(this.isClient);

    // If a retry is already in progress, just let that happen
    if (this.reconnectVars.retryTimer) {
      debug('WireProtocol#connection_gone: retry is already in progress') ;
      return;
    }

    // If this is a requested shutdown, then don't retry
    if (this.closing) {
      this.reconnectVars.retryTimer = null;
      return;
    }

    const nextDelay = Math.floor(this.reconnectVars.retryDelay * this.reconnectVars.retryBackoff);
    if (this.reconnectOpts.retryMaxDelay !== null && nextDelay > this.reconnectOpts.retryMaxDelay) {
      this.reconnectVars.retryDelay = this.reconnectOpts.retryMaxDelay;
    } else {
      this.reconnectVars.retryDelay = nextDelay;
    }

    if (this.reconnectOpts.maxAttempts && this.reconnectVars.attempts >= this.reconnectOpts.maxAttempts) {
      this.reconnectVars.retryTimer = null;
      return;
    }

    this.reconnectVars.attempts += 1;
    this.emit('reconnecting', {
      delay: this.reconnectVars.retryDelay,
      attempt: this.reconnectVars.attempts
    });
    this.reconnectVars.retryTimer = setTimeout(() => {
      this.reconnectVars.retryTotaltime += this.reconnectVars.retryDelay;

      if (this.reconnectOpts.connectTimeout && this.reconnectVars.retryTotaltime >= this.reconnectOpts.connectTimeout) {
        this.reconnectVars.retryTimer = null;
        console.error('WireProtocol#connection_gone: ' +
          `Couldn't get drachtio connection after ${this.reconnectVars.retryTotaltime} ms`);
        return;
      }
      this.socket = net.connect({
        port: this.port,
        host: this.host
      }) ;
      this.socket.setKeepAlive(true) ;
      this.installListeners(this.socket) ;

      this.reconnectVars.retryTimer = null;
    }, this.reconnectVars.retryDelay);
  }

  send(socket, msg) {
    const msgId = uuidV4() ;
    const s = msgId + '|' + msg ;
    socket.write(Buffer.byteLength(s, 'utf8') + '#' + s, () => {
      debug(`wp#send ${this.host}:${this.port} - ${s.length}#${s}`);
    }) ;
    this._logger('===>' + CRLF + Buffer.byteLength(s, 'utf8') + '#' + s + CRLF) ;
    return msgId ;
  }

  /*
   * Note: if you are wondering about the use of the spread operator,
   * it is because we can get SIP messages with things like emojis in them;
   * i.e. UTF8-encoded strings.
   * See https://mathiasbynens.be/notes/javascript-unicode#other-grapheme-clusters for background
   */
  _onData(socket, msg) {
    /*
    If we blindly pass in the data to the logging function
    without debugging enabled, the overhead of converting every message to a string
    is high.
    */
    if (debug.enabled) {
      const strval = msg.toString('utf8') ;
      this._logger(`<===${CRLF}${strval}${CRLF}`) ;
      debug(`<===${strval}`) ;
    }

    if (!this.mapIncomingMsg.has(socket)) {
      this.mapIncomingMsg.set(socket, {
        incomingMsg: Buffer.alloc(0),
        length: -1,
        corruptionCount: 0  // Track repeated corruption events
      });
    }
    const obj = this.mapIncomingMsg.get(socket);

    // Prevent buffer overflow by limiting maximum buffer size (100MB)
    if (obj.incomingMsg.length > 100 * 1024 * 1024) {
      const err = new Error(`buffer overflow detected, resetting buffer: current size ${obj.incomingMsg.length}`);
      debug(`wp#_onData buffer overflow: ${err.message}`);
      obj.incomingMsg = Buffer.alloc(0);
      obj.corruptionCount++;
      
      if (obj.corruptionCount > 5) {
        debug('wp#_onData too many corruption events, disconnecting');
        if (this.isServer) {
          console.error(`repeated buffer corruption, closing socket: ${err}`);
          this.disconnect(socket);
          return;
        } else {
          this.emit('error', err, socket);
          return;
        }
      }
    }

    // Append newly received buffer
    obj.incomingMsg = Buffer.concat([obj.incomingMsg, msg]);
    let index = obj.incomingMsg.indexOf('#');
    let loopCount = 0;
    const maxLoopCount = 1000; // Prevent infinite loops

    while (index > 0 && loopCount < maxLoopCount) {
      loopCount++;
      
      // Validate we have a reasonable length prefix (max 10 digits for message size)
      if (index > 10) {
        debug(`wp#_onData suspicious length prefix size: ${index}, resetting buffer`);
        obj.incomingMsg = obj.incomingMsg.subarray(index + 1);
        index = obj.incomingMsg.indexOf('#');
        obj.corruptionCount++;
        continue;
      }

      let lengthStr;
      try {
        lengthStr = obj.incomingMsg.toString('utf8', 0, index);
      } catch (utf8Error) {
        debug(`wp#_onData UTF-8 decode error in length prefix: ${utf8Error.message}`);
        obj.incomingMsg = obj.incomingMsg.subarray(1); // Skip one byte
        index = obj.incomingMsg.indexOf('#');
        obj.corruptionCount++;
        continue;
      }

      // Parse and validate message size - Fixed: check for NaN, not undefined
      const messageSize = parseInt(lengthStr, 10);
      if (isNaN(messageSize) || messageSize <= 0 || messageSize > Number.MAX_SAFE_INTEGER) {
        const errorMsg = `invalid message from server, did not start with length specifier. got: '${lengthStr}' (${lengthStr.length} chars)`;
        debug(`wp#_onData ${errorMsg}`);
        
        obj.corruptionCount++;
        
        // Try to recover by finding the next valid-looking message boundary
        let nextIndex = obj.incomingMsg.indexOf('#', index + 1);
        if (nextIndex > 0) {
          debug(`wp#_onData attempting recovery by skipping to next # at position ${nextIndex}`);
          obj.incomingMsg = obj.incomingMsg.subarray(nextIndex);
          index = obj.incomingMsg.indexOf('#');
        } else {
          // No recovery possible, reset buffer
          debug('wp#_onData no recovery possible, resetting buffer');
          obj.incomingMsg = Buffer.alloc(0);
          index = -1;
        }
        
        if (obj.corruptionCount > 5) {
          const err = new Error(`${errorMsg} - too many parse errors, disconnecting`);
          if (this.isServer) {
            console.error(`repeated protocol corruption, closing socket: ${err}`);
            this.disconnect(socket);
          } else {
            this.emit('error', err, socket);
          }
          return;
        }
        continue;
      }

      // Additional validation for reasonable message size (max 50MB per message)
      if (messageSize > 50 * 1024 * 1024) {
        const err = new Error(`message size too large: ${messageSize} bytes, max allowed: ${50 * 1024 * 1024}`);
        debug(`wp#_onData ${err.message}`);
        obj.incomingMsg = obj.incomingMsg.subarray(index + 1);
        index = obj.incomingMsg.indexOf('#');
        obj.corruptionCount++;
        continue;
      }

      const start = index + 1;
      const byteLength = obj.incomingMsg.length;
      const totalSize = start + messageSize;

      // Not enough data yet, wait for more
      if (byteLength < totalSize) {
        break;
      }

      let messageString;
      try {
        // The + 1 is because of the # separator
        messageString = obj.incomingMsg.toString('utf8', start, totalSize);
      } catch (utf8Error) {
        debug(`wp#_onData UTF-8 decode error in message body: ${utf8Error.message}`);
        obj.incomingMsg = obj.incomingMsg.subarray(totalSize);
        index = obj.incomingMsg.indexOf('#');
        obj.corruptionCount++;
        continue;
      }

      try {
        this.emit('msg', socket, messageString);
        obj.corruptionCount = Math.max(0, obj.corruptionCount - 1); // Reduce corruption count on success
      } catch (err) {
        debug(`wp#_onData error processing message: ${err.message}`);
        if (this.isServer) {
          console.error(`invalid client message, closing socket: ${err}`);
          this.disconnect(socket);
        } else {
          this.emit('error', err, socket);
        }
        return;
      }

      obj.incomingMsg = obj.incomingMsg.subarray(totalSize);
      index = obj.incomingMsg.indexOf('#');
    }

    // Check for infinite loop condition
    if (loopCount >= maxLoopCount) {
      const err = new Error(`wire protocol parsing loop detected, processed ${loopCount} iterations`);
      debug(`wp#_onData ${err.message}`);
      obj.incomingMsg = Buffer.alloc(0); // Reset buffer to break loop
      if (this.isServer) {
        console.error(`protocol parsing loop, closing socket: ${err}`);
        this.disconnect(socket);
      } else {
        this.emit('error', err, socket);
      }
      return;
    }

    // We need more data to build a full message
  }

  disconnect(socket) {
    this.closing = true ;
    this.mapIncomingMsg.delete(socket);
    if (!socket) { throw new Error('socket is not connected or was not provided') ; }
    this._stopPinging(socket);
    socket.end() ;
  }

  close(callback) {
    assert.ok(this.isServer, 'WireProtocol#close only valid in outbound connection (server) mode');
    this.server.close(callback) ;
  }

} ;
