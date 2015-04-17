'use strict';

var _ = require('lodash');
var util = require('kinda-util').create();
var Sync = require('./');

var SyncServer = Sync.extend('SyncServer', function() {
  this.setCreator(function(options) {
    if (!options) options = {};

    this.excludedTables = options.excludedTables || [];
  });

  this.plug = function(database) {
    this.database = database;
    database.sync = this;

    database.onAsync('didInitialize', function *() {
      yield this.sync.initialize(this);
    });

    database.onAsync('didPutItem', function *(table, key, item) {
      yield this.sync.addLocalOperation(this, table, key, item, 'put');
    });

    database.onAsync('didDeleteItem', function *(table, key, item) {
      yield this.sync.addLocalOperation(this, table, key, item, 'del');
    });
  };

  this.signInWithCredentials = function *(username, password, expirationTime) {
    // to overload
    return undefined;
  };

  this.signInWithPreviousAuthorization = function *(authorization) { // to overload
    return undefined;
  };

  this.signOut = function *(authorization) { // to overload
  };

  this.getSubspaces = function *() { // to overload
    return [{ id: undefined, name: 'Default subspace' }];
  };

  this.determineSubspaceId = function(table, key, item) { // to overload
    return undefined;
  };

  this.initialize = function *(tr) {};

  this.postRemoteOperations = function *(subspaceId, operations, lastOperationId) {
    var local;
    yield this.database.transaction(function *(tr) {
      local = yield this.getLocalOperations(tr, subspaceId, lastOperationId);
      local.operations.forEach(function(localOp) {
        _.remove(operations, function(remoteOp) {
          return remoteOp.table === localOp.table && remoteOp.key === localOp.key;
        });
      });
      if (operations.length) {
        yield this.applyRemoteOperations(tr, subspaceId, operations, true);
        local.lastOperationId = yield this.getLastLocalOperationId(tr, subspaceId);
      }
    }.bind(this));
    return local;
  };

  this.middleware = function() {
    var that = this;
    var tokensRegExp = /^\/tokens\/(.+)$/;
    var subspacesRegExp = /^\/subspaces\?authorization=(.+)$/;
    var operationsRegExp = /^\/subspaces\/([^\/]+)\/operations\?lastOperationId=(.+)&authorization=(.+)$/;

    return function *(next) {
      var matches;

      if (this.url === '/ping') {
        if (this.method === 'GET') {
          this.body = 'pong';
          this.logLevel = 'silence';
          return;
        }
      }

      if (this.url === '/tokens') {
        if (this.method === 'POST') {
          var authorizationItem = yield that.signInWithCredentials(
            this.request.body.username,
            this.request.body.password,
            this.request.body.expirationTime
          );
          if (!authorizationItem) {
            this.status = 403;
            this.body = { error: 'authorization failed' };
            return;
          }
          this.status = 201;
          this.body = authorizationItem;
          return;
        }
      }

      matches = this.url.match(tokensRegExp);
      if (matches) {
        var authorization = util.decodeValue(matches[1]);
        if (this.method === 'GET') {
          var authorizationItem = yield that.signInWithPreviousAuthorization(authorization);
          if (!authorizationItem) {
            this.status = 404;
            return;
          }
          this.body = authorizationItem;
          return;
        } else if (this.method === 'DELETE') {
          yield that.signOut(authorization);
          this.status = 204;
          return;
        }
      }

      matches = this.url.match(subspacesRegExp);
      if (matches) {
        var authorization = util.decodeValue(matches[1]);
        if (!(yield that.signInWithPreviousAuthorization(authorization))) {
          this.status = 403;
          this.body = { error: 'authorization failed' };
          return;
        }
        if (this.method === 'GET') {
          this.body = yield that.getSubspaces();
          return;
        }
      }

      matches = this.url.match(operationsRegExp);
      if (matches) {
        var subspaceId = util.decodeValue(matches[1]);
        var lastOperationId = util.decodeValue(matches[2]);
        var authorization = util.decodeValue(matches[3]);
        if (!(yield that.signInWithPreviousAuthorization(authorization))) {
          this.status = 403;
          this.body = { error: 'authorization failed' };
          return;
        }
        if (this.method === 'GET') {
          var result = yield that.getLocalOperations(
            subspaceId, lastOperationId);
          var syncId = yield that.getLocalSyncId(that.database, subspaceId);
          if (syncId) result.syncId = syncId;
          this.body = result;
          return;
        }
        if (this.method === 'POST') {
          this.status = 201;
          this.body = yield that.postRemoteOperations(subspaceId,
            this.request.body.operations, lastOperationId);
          return;
        }
      };

      yield next;
    };
  };
});

module.exports = SyncServer;
