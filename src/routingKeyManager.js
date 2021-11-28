import debugLib from 'debug'

const debug = debugLib('millegrilles:routingKeyManager')
const debugMessages = debugLib('millegrilles:routingKeyManager:messages')

const TYPES_MESSAGES_ROOM_ACCEPTES = ['evenement', 'transaction', 'commande']

export default class RoutingKeyManager {

  constructor(mq, opts) {
    if(!opts) opts = {}

    // Lien vers RabbitMQ, donne acces au channel, Q et routing keys
    this.mq = mq;
    this.exchange = opts.exchange || '3.protege'
    this.socketio = opts.socketio  // Permet de faire le lien vers cliens Socket.IO
    debug("Exchange : %s", this.exchange)

    // Dictionnaire de routing keys
    //   cle: string (routing key sur RabbitMQ)
    //   valeur: liste de callbacks
    this.registeredRoutingKeyCallbacks = {}

    // Dictionnaire de correlationIds (message direct sur notre Q)
    this.registeredCorrelationIds = {}

    // Liste de routing keys dynamiques, utilise pour garbage collection des routing keys
    this.roomsEvenements = {}

    this.handleMessage.bind(this);

    setInterval(_=>{this.nettoyerRooms()}, 15000)
  }

  async handleMessage(routingKey, messageContent, opts) {
    if(!opts) opts = {}
    const {properties, fields} = opts

    let callback = this.registeredRoutingKeyCallbacks[routingKey];
    const correlationId = properties.correlationId
    const json_message = JSON.parse(messageContent);

    var promise;
    if(callback) {
      let opts = {
        properties
      }
      promise = callback(routingKey, json_message, opts);
      // if(promise) {
      //   debug("Promise recue");
      // } else {
      //   debug("Promise non recue");
      // }
    } else if(this.socketio && correlationId && correlationId.startsWith('sid:')) {
      const socketId = correlationId.split(':')[1]
      const contenuEvenement = {
        correlationId,
        message: json_message,
      }
      debug("Reponse vers socket id %s sur Socket.IO\n%O", socketId, contenuEvenement)
      this.socketio.to(socketId).emit('mq_reponse', contenuEvenement)

    } else if(this.socketio) {
      // Emettre message sur rooms Socket.IO
      const splitKey = routingKey.split('.')
      const exchange = fields.exchange

      debug("Message routing: %s, exchange: %s", routingKey, exchange)

      try {

        if(TYPES_MESSAGES_ROOM_ACCEPTES.includes(splitKey[0]) && exchange !== '4.secure') {
          // Messages niveau

          // Le routing est "type message"."Domaine"."action".
          // Domaine peut etre splitte en "Domaine"."SousDomaine"
          // Le nom de la room commence toujours par le niveau de securite (exchange)
          const rooms = [
            // Supporter capture de tous les evenements
            exchange + '.' + splitKey[0] + '.#',

            // Pour supporter toutes les actions d'un domaine, on a room : evenement.DOMAINE.*
            [exchange, ...splitKey.slice(0, 2), '*'].join('.'),

            // Pour supporter une action sur tous les domaines, on a evenement.#.ACTION
            [exchange, splitKey[0], '#', splitKey[2]].join('.'),

            // Match exact pour evenement.DOMAINE.ACTION
            [exchange, ...splitKey.slice(0, 3)].join('.'),

            // Nom room avec key complete
            [exchange, ...splitKey].join('.'),
          ]

          var room = this.socketio //.volatile
          rooms.forEach(roomName=>{room = room.to(roomName)})

          const contenuEvenement = {
            routingKey,
            message: json_message,
            exchange: exchange,
          }
          if(properties.correlationId) {
            contenuEvenement[correlationId] = properties.correlationId
          }
          debugMessages("Emission evenement sur rooms %s Socket.IO\n%O", rooms, contenuEvenement)

          room.emit('mq_evenement', contenuEvenement)

          // Nouvelle approche, emettre sur nom d'action
          const rkSplit = routingKey.split('.')
          const nomAction = rkSplit[rkSplit.length-1]
          const domaineAction = [rkSplit[0], rkSplit[1], nomAction].join('.')
          debugMessages("Emission evenement rooms %s Socket.IO domaineAction %s\n%O", rooms, domaineAction, contenuEvenement)
          room.emit(domaineAction, contenuEvenement)

        } else {
          debug("Dropped message exchange %s, routing %s", exchange, routingKey)
        }
      } catch (err) {
        console.error("Erreur traitement message recu:\n%O", err)
      }

    } else {
      debug("Routing key pas de callback: %s", routingKey);
    }

    return promise;
  }

  handleResponse(correlationId, message, opts) {
    debug("!!! Response via correlaction Id %s", correlationId)
    const {properties, fields} = opts

    let callback = this.registeredCorrelationIds[correlationId]
    const json_message = JSON.parse(message)
    if(callback) {
      callback(json_message, opts)
    } else {
      console.error("ERROR routingKeyManager.handeResponse, message correlation inconnue %s : %O", correlationId, json_message)
    }
  }

  addResponseCorrelationId(correlationId, cb) {
    this.registeredCorrelationIds[correlationId] = cb
  }

  addRoutingKeyCallback(callback, routingKeys, opts) {
    if(!opts) opts = {}

    const operationLongue = opts.operationLongue || false
    const qCustom = opts.qCustom
    const exchange = opts.exchange || this.exchange

    for(var routingKey_idx in routingKeys) {
      let routingKeyName = routingKeys[routingKey_idx]
      this.registeredRoutingKeyCallbacks[routingKeyName] = callback

      // Ajouter la routing key
      if(qCustom) {
        debug("Ajouter callback pour routingKey %s sur Q %s", routingKeyName, qCustom)
        // this.mq.channel.bindQueue(this.mq.qOperationLongue.queue, exchange, routingKeyName)
        const infoQ = this.mq.qCustom[qCustom]
        if(!infoQ) throw new Error(`Q custom ${qCustom} n'existe pas`)
        this.mq.channel.bindQueue(infoQ.q.queue, exchange, routingKeyName)
      } else if(operationLongue) {
        debug("Ajouter callback pour routingKey %s sur Q operation longue", routingKeyName)
        // this.mq.channel.bindQueue(this.mq.qOperationLongue.queue, exchange, routingKeyName)
        const infoQ = this.mq.qCustom.operationsLongues
        this.mq.channel.bindQueue(infoQ.q.queue, exchange, routingKeyName)
      } else {
        debug("Ajouter callback pour routingKey %s", routingKeyName)
        this.mq.channel.bindQueue(this.mq.reply_q.queue, exchange, routingKeyName)
      }
    }
  }

  removeRoutingKeys(routingKeys) {
    for(var routingKey_idx in routingKeys) {
      let routingKeyName = routingKeys[routingKey_idx]
      delete this.registeredRoutingKeyCallbacks[routingKeyName]

      // Retirer la routing key
      debug("Enlever routingKeys %s", routingKeyName)
      this.mq.channel.unbindQueue(this.mq.reply_q.queue, this.exchange, routingKeyName)
    }
  }

  addRoutingKeysForSocket(socket, routingKeys, niveauSecurite, channel, reply_q) {
    const socketId = socket.id
    const exchange = niveauSecurite || this.exchange

    for(var routingKey_idx in routingKeys) {
      let routingKeyName = routingKeys[routingKey_idx];
      debug("Ajouter binding routingKey %O", routingKeyName)

      // Ajouter la routing key
      this.mq.channel.bindQueue(reply_q.queue, exchange, routingKeyName);

      // Associer le socket a la room appropriee
      var roomName = exchange + '.' + routingKeyName
      debug("Socket id:%s, join room %s", socketId, roomName)
      socket.join(roomName)

      this.roomsEvenements[roomName] = {exchange, reply_q, roomName, routingKeyName}
    }

  }

  removeRoutingKeysForSocket(socket, routingKeys, niveauSecurite, channel, reply_q) {
    const exchange = niveauSecurite || this.exchange
    for(var routingKey_idx in routingKeys) {
      let routingKeyName = routingKeys[routingKey_idx];

      // Retirer la routing key
      this.mq.channel.unbindQueue(reply_q.queue, this.exchange, routingKeyName);
    }
  }

  nettoyerRooms() {
    if(this.socketio) {
      const rooms = this.socketio.sockets.adapter.rooms

      // Faire la liste des routing keys d'evenements
      for(let roomName in this.roomsEvenements) {
        const roomConfig = this.roomsEvenements[roomName]
        const routingKeyName = roomConfig.routingKeyName
        const socketRoom = rooms.get(roomConfig.roomName)
        if(!socketRoom) {
          debug("Nettoyage room %s, retrait routing key %s", roomConfig.roomName, routingKeyName)
          this.mq.channel.unbindQueue(roomConfig.reply_q.queue, roomConfig.exchange, routingKeyName);
        } else {
          // debug("Routing key %s, room %s, %d membres", routingKeyName, roomConfig.roomName, socketRoom.size)
        }
      }

      // const rooms = this.socketio.sockets.rooms
      // debug("SocketIO rooms: %s", rooms)
      debug("Entretien rooms, nb rooms : %O", rooms.size)
      for(let room of rooms.keys()) {
        const roomInfo = rooms.get(room)
        debug("Room %s, %d membres", room, roomInfo.size)
      }
    } else {
      debug("Aucun entretien rooms, pas de socketio")
    }
  }

}
