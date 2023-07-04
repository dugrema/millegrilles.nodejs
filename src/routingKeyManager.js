const debugLib = require('debug')

const debug = debugLib('routingKeyManager')
const debugRooms = debugLib('routingKeyManager:rooms')
const debugMessages = debugLib('routingKeyManager:messages')

const L1PUBLIC = '1.public'
const TYPES_MESSAGES_ROOM_ACCEPTES = ['evenement', 'transaction', 'commande']

// Mappers custom
// Cle = regex de routing key pour match, e.g. /^evenement\.CorePki\./, Value = {exchanges [], routingKeyTest regexp, mapRoom function()}
const customRoomMappers = {

  // Mapper pour fingerprintPk de nouveau certificat
  // '/^commande\.CoreMaitreDesComptes\.activationFingerprintPk$/': {
  //   exchanges: ['2.prive'],
  //   routingKeyTest: /^commande\.CoreMaitreDesComptes\.activationFingerprintPk$/,
  //   mapRoom: (message, rk, exchange) => {
  //     const fingerprintPk = message.fingerprint_pk
  //     if(fingerprintPk) {
  //       return `2.prive/evenement.CoreMaitreDesComptes.activationFingerprintPk/${fingerprintPk}`
  //     }
  //   }
  // }

}

class RoutingKeyManager {

  constructor(mq, opts) {
    if(!opts) opts = {}

    // Lien vers RabbitMQ, donne acces au channel, Q et routing keys
    this.mq = mq;
    this.exchange = opts.exchange || L1PUBLIC
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
    const {properties, fields, certificat} = opts

    let callbackEntry = this.registeredRoutingKeyCallbacks[routingKey]
    const correlationId = properties.correlationId,
          replyTo = properties.replyTo
    const json_message = JSON.parse(messageContent)

    debug("RoutingKeyManager Message recu rk: %s, correlationId: %s, callback present?%s", routingKey, correlationId, callbackEntry?'true':'false')

    // Champs speciaux utilises pour le routage
    const userId = json_message.user_id || json_message.userId

    var promise;
    if(callbackEntry) {
      let opts = {
        properties,
        certificat,
      }
      promise = callbackEntry.callback(routingKey, json_message, opts);
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
          // Domaine peut etre splitte en "Domaine"."Partition"
          // Le nom de la room commence toujours par le niveau de securite (exchange)
          const roomsExact = [
            // Nom room avec key complete (incluant partition)
            exchange + '/' + splitKey.join('.'),
          ]

          if(splitKey.length === 4) {
            // Ajouter room sans la partition
            roomsExact.push([splitKey[0], splitKey[1], splitKey[3]].join('.'))
          }

          if(userId) {
            // Routage avec userId
            roomsExact.push(userId + '/' + [...splitKey].join('.'))
          }

          // Ajouter les mappers custom
          Object.values(customRoomMappers).forEach(mapper=>{
            debugRooms("Test mapper %s pour routingKey %s", mapper, routingKey)
            if( mapper.exchanges && ! mapper.exchanges.includes(exchange) ) return
            if( mapper.routingKeyTest.test(routingKey) ) {
              const contenu = JSON.parse(json_message.contenu)
              const roomName = mapper.mapRoom(contenu, routingKey, correlationId, exchange)
              debugRooms("Mapper routingKey match %s -> room %s", routingKey, roomName)
              if(roomName) {
                debugRooms("Emettre message sur room custom %s", roomName)
                roomsExact.push(roomName)
              }
            }
          })

          // Combiner toutes les rooms en une seule liste
          var room = this.socketio
          roomsExact.forEach(roomName=>{room = room.to(roomName)})

          const contenuEvenement = {
            routingKey,
            message: json_message,
            exchange: exchange,
            properties: {replyTo, correlationId}
          }
          if(properties.correlationId) {
            contenuEvenement[correlationId] = properties.correlationId
          }
          debugRooms("Emission evenement sur rooms %s Socket.IO\n%O", roomsExact, contenuEvenement)

          // room.emit('mq_evenement', contenuEvenement)

          // Nouvelle approche, emettre la routing key "abregee" (sans le sous-domaine)
          // Ex: evenement.GrosFichiers.abcd-1234.majFichier devient evenement.GrosFichiers.majFichier
          const rkSplit = routingKey.split('.')
          const nomAction = rkSplit[rkSplit.length-1]
          const domaineAction = [rkSplit[0], rkSplit[1], nomAction].join('.')
          debugRooms("Emission evenement rooms %s Socket.IO domaineAction %s\n%O", roomsExact, domaineAction, contenuEvenement)
          room.emit(domaineAction, contenuEvenement)

          // Rooms avec wildcards (evenements seulement)
          if(rkSplit[0] === 'evenement') {
            // Room pour toutes les actions d'un domaine
            const eventNameToutesActionsDomaine = [...splitKey.slice(0, 2), '*'].join('.')
            const nomRoomToutesActionsDomaine = exchange + '/' + eventNameToutesActionsDomaine
            debugRooms("Emettre sur %s", eventNameToutesActionsDomaine)
            this.socketio.to(nomRoomToutesActionsDomaine).emit(eventNameToutesActionsDomaine, contenuEvenement)

            // Room pour tous les domaines avec une action
            const eventNameTousDomainesAction = [splitKey[0], '*', [...splitKey].pop()].join('.')
            const roomTousDomainesAction = exchange + '/' + eventNameTousDomainesAction
            debugRooms("Emettre sur %s", eventNameTousDomainesAction)
            this.socketio.to(roomTousDomainesAction).emit(eventNameTousDomainesAction, contenuEvenement)
          }

        } else {
          debugMessages("Dropped message exchange %s, routing %s", exchange, routingKey)
        }
      } catch (err) {
        console.error("Erreur traitement message recu:\n%O", err)
      }

    } else {
      debugMessages("Routing key pas de callback: %s", routingKey);
    }

    return promise
  }

  handleResponse(correlationId, message, opts) {
    // debug("!!! Response via correlaction Id %s", correlationId)
    const {properties, fields} = opts

    let callback = this.registeredCorrelationIds[correlationId]
    const json_message = JSON.parse(message)
    if(callback) {
      callback(json_message, opts)
    } else {
      // Verifier si le message a un champ 'certificat'. Ca pourrait etre un broadcast/reponse certificat
      const certificat = json_message.certificat
      if(certificat) {
        console.warn("WARN Certificat recu en dehors du certificatManager, on le recupere (message a valider sera perdu)")
        this.mq.pki.sauvegarderMessageCertificat({chaine_pem: certificat})
          .catch(err=>console.error("ERROR Erreur sauvegarde certificat ", err))
      } else {
        console.error("ERROR routingKeyManager.handeResponse, message correlation inconnue %s : %O", correlationId, json_message)
      }
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
      this.registeredRoutingKeyCallbacks[routingKeyName] = {callback, ...opts}

      // Ajouter la routing key
      if(qCustom) {
        debug("addRoutingKeyCallback Ajouter callback pour routingKey %s sur Q %s", routingKeyName, qCustom)
        // this.mq.channel.bindQueue(this.mq.qOperationLongue.queue, exchange, routingKeyName)
        const infoQ = this.mq.qCustom[qCustom]
        if(!infoQ) throw new Error(`Q custom ${qCustom} n'existe pas`)
        debug("addRoutingKeyCallback Bind %s a %O", routingKeyName, infoQ.q.queue)
        this.mq.channel.bindQueue(infoQ.q.queue, exchange, routingKeyName)
      } else if(operationLongue) {
        debug("addRoutingKeyCallback Ajouter callback pour routingKey %s sur Q operation longue", routingKeyName)
        // this.mq.channel.bindQueue(this.mq.qOperationLongue.queue, exchange, routingKeyName)
        const infoQ = this.mq.qCustom.operationsLongues
        this.mq.channel.bindQueue(infoQ.q.queue, exchange, routingKeyName)
      } else {
        debug("addRoutingKeyCallback Ajouter callback pour routingKey %s", routingKeyName)
        this.mq.channel.bindQueue(this.mq.reply_q.queue, exchange, routingKeyName)
      }
    }
  }

  removeRoutingKeys(routingKeys, opts) {
    const queue = opts.queue || this.mq.reply_q.queue,
          exchange = opts.exchange || this.exchange

    for(let routingKeyName of routingKeys) {
      delete this.registeredRoutingKeyCallbacks[routingKeyName]

      // Retirer la routing key
      debug("Enlever routingKeys %s", routingKeyName)
      this.mq.channel.unbindQueue(queue, exchange, routingKeyName)
    }
  }

  addRoutingKeysForSocket(socket, routingKeys, niveauSecurite, channel, reply_q, opts) {
    opts = opts || {}
    const { userId, roomParam, mapper } = opts
    const socketId = socket.id
    const exchange = niveauSecurite || this.exchange

    if(mapper) {
      const cleMapper = '' + mapper.routingKeyTest
      if(!customRoomMappers[cleMapper]) {
        debug("Ajouter mapper pour evenements %s", cleMapper)
        customRoomMappers[cleMapper] = mapper
      }
    }

    let roomParamList = roomParam
    if(!roomParam) {
      roomParamList = [null]
    } else if(typeof(roomParam) === 'string') {
      roomParamList = [roomParam]
    }

    for(var routingKey_idx in routingKeys) {
      let routingKeyName = routingKeys[routingKey_idx];
      debug("Ajouter binding routingKey %O", routingKeyName)

      // Ajouter la routing key
      this.mq.channel.bindQueue(reply_q.queue, exchange, routingKeyName);

      roomParamList.forEach(roomParam=>{
        // Associer le socket a la room appropriee
        let roomName
        if(userId) {
          // Enregistrement pour un user en particulier
          roomName = `${userId}/${routingKeyName}`
        } else if(roomParam) {
          roomName = `${exchange}/${routingKeyName}/${roomParam}`
        } else {
          // Enregistrement sur l'exchange (global, voit passer tous les messages correspondants)
          roomName = `${exchange}/${routingKeyName}`
        }
        debug("Socket id:%s, join room %s", socketId, roomName)
        socket.join(roomName)

        this.roomsEvenements[roomName] = {exchange, reply_q, roomName, routingKeyName, userId}
      })
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

      const roomsParamsParBinding = {}

      debugRooms("Rooms evenements : %O", Object.keys(this.roomsEvenements))

      // Faire la liste des routing keys d'evenements
      for(let roomName in this.roomsEvenements) {
        const roomConfig = this.roomsEvenements[roomName]
        const routingKeyName = roomConfig.routingKeyName
        const socketRoom = rooms.get(roomConfig.roomName)
        const replyQ = roomConfig.reply_q.queue
        const exchange = roomConfig.exchange

        const roomNameSplit = roomName.split('/')
        if(roomNameSplit.length === 3) {
          let roomParam = null
          let rk = null
          let exchange = null
          roomParam = roomNameSplit.pop(); rk = roomNameSplit.pop(); exchange = roomNameSplit.pop()
          let roomBinding = `${exchange}/${rk}`
          if(!roomsParamsParBinding[roomBinding]) {
            roomsParamsParBinding[roomBinding] = {replyQ, exchange, routingKeyName, compteur: 0}
          }
          // debug("!!! Room verif %s, roomCOnfig.roomName: %s, binding: %s, param: %s, socketRoom : %O", roomName, roomConfig.roomName, roomBinding, roomParam, socketRoom)
          if(socketRoom) roomsParamsParBinding[roomBinding].compteur++
        } else if(!socketRoom) {
          debugRooms("Nettoyage room %s, retrait routing key %s", roomConfig.roomName, routingKeyName)
          this.mq.channel.unbindQueue(replyQ, exchange, routingKeyName);
        } else {
          // debug("Routing key %s, room %s, %d membres", routingKeyName, roomConfig.roomName, socketRoom.size)
        }

        if(!socketRoom) {
          debugRooms("Supprimer la room %s pour eviter nettoyages subsequents", roomName)
          delete this.roomsEvenements[roomName]
        }
      }

      debugRooms("Rooms par binding avec params : %O", roomsParamsParBinding)
      Object.keys(roomsParamsParBinding).map(binding=>{
        const info = roomsParamsParBinding[binding]
        if(info.compteur === 0) {
          debugRooms("Retrait binding %s", binding)
          this.mq.channel.unbindQueue(info.replyQ, info.exchange, info.routingKeyName);
        }
      })

      // const rooms = this.socketio.sockets.rooms
      // debug("SocketIO rooms: %s", rooms)
      debugRooms("Entretien rooms, nb rooms : %O", rooms.size)
      for(let room of rooms.keys()) {
        const roomInfo = rooms.get(room)
        debugRooms("Room %s, %d membres", room, roomInfo.size)
      }
    } else {
      debugRooms("Aucun entretien rooms, pas de socketio")
    }
  }

}

module.exports = RoutingKeyManager
