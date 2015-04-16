'use strict';

var _ = require('lodash');
var co = require('co');
var wait = require('co-wait');
var log = require('kinda-log').create();
var util = require('kinda-util').create();
var config = require('kinda-config').get('kinda-db-sync/client');
var httpClient = require('kinda-http-client').create();
var Sync = require('./');
var Auth = require('kinda-db-auth');

var SyncClient = Sync.extend('SyncClient', function() {
  this.include(Auth);

  this.setCreator(function(url) {
    if (!url) url = config.url;
    if (!url) throw new Error('url is missing');
    if (util.endsWith(url, '/'))
      url = url.substr(0, url.length - 1);
    this.baseURL = url;

    this.excludedTables = [];
  });

  this.plug = function(database) {
    this.database = database;
    database.sync = this;

    database.onAsync('didInitialize', function *() {
      yield this.sync.initialize(this);
    });

    database.onAsync('didPutItem', function *(table, key, item, options) {
      if (options && options.sync === false) return;
      yield this.sync.addLocalOperation(this, table, key, item, 'put');
    });

    database.onAsync('didDeleteItem', function *(table, key, item, options) {
      if (options && options.sync === false) return;
      yield this.sync.addLocalOperation(this, table, key, item, 'del');
    });
  };

  this.initialize = function *(tr) {
  };

  this.getSubspaceId = function *() {
    var prefix = [this.database.name, '$SyncSubspaces'];
    var subspaces = yield this.database.store.getRange({ prefix: prefix });
    if (subspaces.length === 0) return;
    if (subspaces.length !== 1)
      throw new Error('assertion error (subspaces.length !== 1)');
    return _.last(subspaces[0].key);
  };

  this.setSubspaceId = function *(subspaceId) {
    var currentSubspaceId = yield this.getSubspaceId();
    if (subspaceId === currentSubspaceId) return;
    var isStarted = this.getIsStarted();
    if (isStarted) {
      this.stop();
      yield this.waitStop();
    }
    yield this.removeSubspace(currentSubspaceId);
    // TODO: clean database
    yield this.addSubspace(subspaceId);
    if (isStarted) this.start(); else yield this.run();
  };

  this.addSubspace = function *(subspaceId) {
    if (!subspaceId) return;
    var key = [this.database.name, '$SyncSubspaces', subspaceId];
    var value = { lastLocalOperationId: 0, lastRemoteOperationId: 0 };
    yield this.database.store.put(key, value, { errorIfExists: true });
  };

  this.removeSubspace = function *(subspaceId) {
    if (!subspaceId) return;
    var key = [this.database.name, '$SyncSubspaces', subspaceId];
    yield this.database.store.del(key);
  };

  this.getLastRemoteOperationId = function *(tr, subspaceId) {
    if (arguments.length === 1) {
      subspaceId = tr;
      tr = this.database;
    }
    var key = [tr.name, '$SyncSubspaces', subspaceId];
    var subspace = yield tr.store.get(key);
    return subspace.lastRemoteOperationId;
  };

  this.setLastRemoteOperationId = function *(tr, subspaceId, operationId) {
    if (arguments.length === 2) {
      operationId = subspaceId;
      subspaceId = tr;
      tr = this.database;
    }
    var key = [tr.name, '$SyncSubspaces', subspaceId];
    var subspace = yield tr.store.get(key);
    subspace.lastRemoteOperationId = operationId;
    yield tr.store.put(key, subspace, { createIfMissing: false });
  };

  this.getRemoteSyncId = function *(tr, subspaceId) {
    if (arguments.length === 1) {
      subspaceId = tr;
      tr = this.database;
    }
    var key = [tr.name, '$SyncSubspaces', subspaceId];
    var subspace = yield tr.store.get(key);
    return subspace.remoteSyncId;
  };

  this.setRemoteSyncId = function *(tr, subspaceId, syncId) {
    if (arguments.length === 2) {
      syncId = subspaceId;
      subspaceId = tr;
      tr = this.database;
    }
    var key = [tr.name, '$SyncSubspaces', subspaceId];
    var subspace = yield tr.store.get(key);
    subspace.remoteSyncId = syncId;
    yield tr.store.put(key, subspace, { createIfMissing: false });
  };

  this.start = function() {
    co(function *() {
      if (this._isStarted) return;
      this._isStarted = true;
      this.emit('status.didChange');
      while (!this._isStopping) {
        try {
          yield this.run();
        } catch (err) {
          console.error(err);
        }
        if (!this._isStopping) {
          this._timeout = util.createTimeout(60 * 1000);
          yield this._timeout.start();
          this._timeout = undefined;
        }
      }
      this._isStarted = false;
      this.emit('status.didChange');
    }.bind(this)).catch(function(err) {
      console.error(err.stack);
    });
  };

  this.stop = function() {
    this._isStopping = true;
    this.emit('status.didChange');
    if (this._timeout) this._timeout.stop();
    co(function *() {
      while (this._isStarted || this._isRunning) {
        yield wait(100);
      }
      this._isStopping = false;
      this.emit('status.didChange');
    }.bind(this)).catch(function(err) {
      console.error(err.stack);
    });
  };

  this.waitStop = function *() {
    while (this._isStopping) {
      yield wait(100);
    }
  }

  this.getIsStarted = function() {
    return this._isStarted;
  };

  this.run = function *() {
    if (this._isRunning) return;
    var didUpdateLocalDatabase = false;
    try {
      this._isRunning = true;
      this.emit('status.didChange');
      // log.debug('Synchronisation started');
      if (!this.token) return;
      var subspaceId = yield this.getSubspaceId();
      if (!subspaceId) return;
      try {
        yield this.lockSubspace(subspaceId);
        var remoteSyncId = yield this.getRemoteSyncId(subspaceId);
        var local = yield this.getLocalOperations(subspaceId, 0);
        var hasLocalOperations = !!local.operations.length;
        var remote, lastRemoteOperationId, previousLastRemoteOperationId,
          hasRemoteOperations;
        do {
          lastRemoteOperationId = yield this.getLastRemoteOperationId(subspaceId);
          if (lastRemoteOperationId === previousLastRemoteOperationId)
            throw new Error('assertion error (lastRemoteOperationId === previouLastRemotesOperationId)'); // avoid infinite loop in case of uncaught error
          if (lastRemoteOperationId === 0) {
            this._isInitialSync = true;
            this.emit('status.didChange');
          }
          previousLastRemoteOperationId = lastRemoteOperationId;
          remote = yield this.getRemoteOperations(
            subspaceId, lastRemoteOperationId);
          if (remoteSyncId && remoteSyncId !== remote.syncId) {
            // Local database doesn't match the remote one
            var err = new Error();
            err.reason = 'localDoesNotMatchRemote';
            throw err;
          }
          if (!remoteSyncId && remote.syncId) {
            // Save remote syncId
            yield this.setRemoteSyncId(subspaceId, remote.syncId);
            remoteSyncId = remote.syncId;
          }
          var hasRemoteOperations = !!remote.operations.length;
          if (hasRemoteOperations && !this._isStopping) {
            if (hasLocalOperations) {
              remote.operations.forEach(function(remoteOp) {
                _.remove(local.operations, function(localOp) {
                  return localOp.table === remoteOp.table && localOp.key === remoteOp.key;
                });
              });
            }
            yield this.database.transaction(function *(tr) {
              yield this.applyRemoteOperations(tr, subspaceId, remote.operations);
              yield this.setLastRemoteOperationId(tr, subspaceId, remote.lastOperationId);
              didUpdateLocalDatabase = true;
            }.bind(this));
          }
        } while (hasRemoteOperations && !this._isStopping);
        if (hasLocalOperations && !this._isStopping) {
          if (local.operations.length) {
            remote = yield this.postLocalOperations(subspaceId,
              local.operations, lastRemoteOperationId);
            yield this.database.transaction(function *(tr) {
              yield this.applyRemoteOperations(tr, subspaceId, remote.operations);
              yield this.setLastRemoteOperationId(tr, subspaceId, remote.lastOperationId);
            }.bind(this));
          }
          yield this.delLocalOperations(subspaceId, local.lastOperationId);
        }
        this._lastSyncDate = new Date();
      } finally {
        yield this.unlockSubspace(subspaceId);
      }
    } catch (err) {
      this.emit('didFail', err);
      throw err;
    } finally {
      this._isRunning = false;
      this._isInitialSync = false;
      this.emit('status.didChange');
      if (didUpdateLocalDatabase)
        this.emit('didUpdateLocalDatabase');
      // log.debug('Synchronisation finished');
    }
  };

  this.getStatus = function() {
    if (this._isStopping)
      return 'stopping';
    if (this._isRunning)
      return this._isInitialSync ? 'initializing' : 'running';
    if (this._isStarted)
      return 'started';
    return 'stopped';
  };

  this.getLastSyncDate = function() {
    return this._lastSyncDate;
  };

  this.lockSubspace = function *(subspaceId) {
    // TODO
  };

  this.unlockSubspace = function *(subspaceId) {
    // TODO
  };

  this.getSubspaces = function *() {
    var url = this.baseURL + '/subspaces'
    url += '?token=' + util.encodeValue(this.token);
    var params = { method: 'GET', url: url };
    var res = yield httpClient.request(params);
    if (res.statusCode !== 200)
      throw new Error('cannot get subspaces from server');
    return res.body;
  };

  this.getRemoteOperations = function *(subspaceId, lastOperationId) {
    console.log('getServerOperations:', subspaceId, lastOperationId);
    var url = this.baseURL + '/subspaces/' + util.encodeValue(subspaceId);
    url += '/operations?lastOperationId=' + util.encodeValue(lastOperationId);
    url += '&token=' + util.encodeValue(this.token);
    var params = { method: 'GET', url: url };
    var res = yield httpClient.request(params);
    if (res.statusCode !== 200)
      throw new Error('cannot get operations from server');
    return res.body;
  };

  this.postLocalOperations = function *(subspaceId, operations, lastOperationId) {
    console.log('postLocalOperations:', subspaceId, operations.length + ' op(s)');
    var url = this.baseURL + '/subspaces/' + util.encodeValue(subspaceId);
    url += '/operations?lastOperationId=' + util.encodeValue(lastOperationId);
    url += '&token=' + util.encodeValue(this.token);
    var body = { operations: operations };
    var params = { method: 'POST', url: url, body: body };
    var res = yield httpClient.request(params);
    if (res.statusCode !== 201)
      throw new Error('cannot post operations to server');
    return res.body;
  };

  this.reset = function *() {
    var currentSubspaceId = yield this.getSubspaceId();
    if (!currentSubspaceId) return;
    var isStarted = this.getIsStarted();
    if (isStarted) {
      this.stop();
      yield this.waitStop();
    }
    yield this.database.resetDatabase();
    if (isStarted) this.start(); else yield this.run();
    yield this.setSubspaceId(currentSubspaceId);
  };
});

module.exports = SyncClient;
