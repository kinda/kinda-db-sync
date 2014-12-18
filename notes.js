// Client

var Database = require('kinda-db');
var Sync = require('kinda-db-sync/client');

var db = Database.create('Durable', 'websql:avc-mobile-test');

db.use(Sync.create('http://...'));

var subspaces = yield db.sync.getSubspaces();

var subspaceId = yield db.sync.getSubspaceId();

yield db.sync.setSubspaceId('g9e55x2');

var lastOperationId = yield db.sync.getLastOperationId();

yield db.sync.setLastOperationId(2387334);

// Server

var Database = require('kinda-db');
var Sync = require('kinda-db-sync/server');

var db = Database.create('Durable', 'mysql://...');

db.use(Sync.create());

db.sync.getSubspaces = function *() {
  var businesses = db.getRange('Businesses');
  var subspaces = businesses.map(function(business) {
    return { id: business.id, name: business.name };
  });
  return subspaces;
};

db.sync.determineSubspaceId = function(table, key, item) {
  if (table === 'Businesses')
    return key;
  else
    return item && item.businessId;
};

api.use(db.sync.middleware);

// --- Stratégie ---

// 1) Le client reçoit les nouvelles opérations du serveur par paquet de 300
//    GET /subspaces/xu7g45jo8/operations?lastOperationId=36552
//
// 2) Si une opération serveur rentre en conflit avec une opération présente
//    dans la queue du client, c'est l'opération serveur qui gagne
//
// 3) Quand le serveur n'a plus rien de nouveau, le client lui envoit alors
//    les opérations restantes dans sa queue (limite à 300 opérations ?)
//    POST /subspaces/xu7g45jo8/operations?lastOperationId=37172
//
// 4) Le serveur démarre une transaction, recherche les opérations stockées
//    depuis lastOperationId (serverOperations), stocke les opérations clients
//    qui ne sont pas en conflit et retourne serverOperations ainsi que le
//    nouveau lastOperationId
//
// 5) Quand le client reçoit la réponse du serveur, il supprime de sa queue
//    les opérations envoyées, ajoute les dernières serverOperations et
//    mémorise le nouveau lastOperationId.
//
// 6) Recommencer à l'étape 1

// --- Stockage ---

// $SyncSubspaces
//   id
//   lastOperationId

// $SyncOperations
//   [subspaceId, id (Number)]
//   table
//   key
//   type ('put' ou 'del')

// --- API ---

// GET /subspaces
//   RES:
//     {
//       subspaces: [
//         { id: '...', name: 'AFC 2015' }
//         { id: '...', name: 'DWS 2015' }
//       ],
//     }

// GET /subspaces/{subspaceId}/operations?lastOperationId={lastOperationId}
//   RES:
//     {
//       operations: [
//         { table: 'Customers', key: '...', type: 'put', value: { ... } }
//         { table: 'Customers', key: '...', type: 'del' }
//         { table: 'Customers', key: '...', type: 'put', value: { ... } }
//       ],
//       lastOperationId: 37172
//     }

// POST /subspaces/{subspaceId}/operations?lastOperationId={lastOperationId}
//   REQ:
//     {
//       operations: [
//         { table: 'Customers', key: '...', type: 'put', value: { ... } }
//       ]
//     }
//   RES:
//     {
//       operations: [
//         { table: 'Customers', key: '...', type: 'del' }
//       ],
//       lastOperationId: 37178
//     }
