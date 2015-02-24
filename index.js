'use strict';

var _ = require('lodash');
var idgen = require('idgen');
var util = require('kinda-util').create();
var KindaObject = require('kinda-object');

var Sync = KindaObject.extend('Sync', function() {
  this.setCreator(function() {
    throw new Error('cannot create an abstract Sync, you must require either a client or a server');
  });

  this.getLastLocalOperationId = function *(tr, subspaceId) {
    var key = [tr.name, '$SyncSubspaces', subspaceId];
    var subspace = yield tr.store.get(key, { errorIfMissing: false });
    return subspace && subspace.lastLocalOperationId;
  };

  this.getLocalSyncId = function *(tr, subspaceId) {
    var key = [tr.name, '$SyncSubspaces', subspaceId];
    var subspace = yield tr.store.get(key, { errorIfMissing: false });
    return subspace && subspace.localSyncId;
  };

  this.incrementLastLocalOperationId = function *(tr, subspaceId) {
    var key = [tr.name, '$SyncSubspaces', subspaceId];
    var subspace = yield tr.store.get(key, { errorIfMissing: false });
    if (!subspace) {
      subspace = {
        lastLocalOperationId: 0,
        localSyncId: idgen(16)
      };
    }
    subspace.lastLocalOperationId++;
    yield tr.store.put(key, subspace);
    return subspace.lastLocalOperationId;
  };

  this.addLocalOperation = function *(tr, table, key, item, type) {
    console.log('addLocalOperation', table.name, key, type);
    var subspaceId = this.determineSubspaceId(table, key, item);
    var operationId = yield this.incrementLastLocalOperationId(tr, subspaceId);
    var operationKey = [tr.name, '$SyncOperations', subspaceId, operationId];
    var operationItem = {
      table: table.name,
      key: key,
      type: type
    };
    yield tr.store.put(operationKey, operationItem, { errorIfExists: true });
    return operationId;
  };

  this.getLocalOperations = function *(tr, subspaceId, lastOperationId) {
    var that = this;

    if (arguments.length === 2) {
      lastOperationId = subspaceId;
      subspaceId = tr;
      tr = that.database;
    }

    var prefix = [tr.name, '$SyncOperations', subspaceId];
    var items = yield tr.store.getRange({
      prefix: prefix, startAfter: lastOperationId, limit: 300 });

    var operations = items.map(function(item) { return item.value });
    // TODO: dédoublonner les opérations, soit ici soit au moment du stockage
    var tables = {};
    operations.forEach(function(operation, index) {
      if (operation.type !== 'put') return;
      var table = tables[operation.table];
      if (!table) table = tables[operation.table] = [];
      table.push({ key: operation.key, index: index });
    });
    var fns = [];
    _.forOwn(tables, function(ops, table) {
      fns.push(function *() {
        var keys = ops.map(function(op) { return op.key });
        var results = yield tr.getMany(table, keys, { errorIfMissing: false });
        ops.forEach(function(op) {
          var result = _.find(results, 'key', op.key);
          if (result) operations[op.index].value = result.value;
        });
      });
    });
    yield fns;
    _.remove(operations, function(operation) {
      return operation.type === 'put' && operation.value == null;
    });

    if (items.length) lastOperationId = _.last(_.last(items).key);

    return { operations: operations, lastOperationId: lastOperationId };
  };

  this.delLocalOperations = function *(subspaceId, lastOperationId) {
    var prefix = [this.database.name, '$SyncOperations', subspaceId];
    var items = yield this.database.store.delRange({
      prefix: prefix, end: lastOperationId });
  }

  this.applyRemoteOperations = function *(tr, subspaceId, operations, sync) {
    sync = !!sync;
    for (var i = 0; i < operations.length; i++) {
      var op = operations[i];
      switch (op.type) {
      case 'put':
        console.log('put', op.table, op.key);
        yield tr.put(op.table, op.key, op.value, { sync: sync });
        break;
      case 'del':
        console.log('del', op.table, op.key);
        yield tr.del(op.table, op.key, { errorIfMissing: false, sync: sync });
        break;
      default:
        throw new Error('invalid operation type');
      }
    }
  };
});

module.exports = Sync;
