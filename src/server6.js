const debugLib = require('debug')
const fs = require('fs')
const morgan = require('morgan')
const session = require('express-session')
const redis = require('redis')
const redisConnect = require('connect-redis')
const {v4: uuidv4} = require('uuid')
const socketio = require('socket.io')
const socketioSession = require('express-socket.io-session')

const { MilleGrillesAmqpDAO } = require('./amqpdao')
const { MilleGrillesPKI } = require('./pki')
const initComptesUsagers = require('./comptesUsagersDao')
const { genererChallengeCertificat, veriferUpgradeProtegerApp } = require('./authentification')

const debug = debugLib('millegrilles:server6'),
      debugConnexions = debugLib('millegrilles:server6:connexions'),
      redisStore = redisConnect(session)

const REDIS_PKI_DATABASE = 0,
      REDIS_SESSION_DATABASE = 2

const CERT_CA_FILE = process.env.MG_MQ_CAFILE,
      CERT_FILE = process.env.MG_MQ_CERTFILE,
      KEY_CA_FILE = process.env.MG_MQ_KEYFILE,
      REDIS_PWD_FILE = process.env.MG_MQ_REDIS_PASSWD

const CONST_SESSION_TIMEOUT = 12 * 3_600_000

// Preparer certificats, mots de passe
function chargerCredendials() {
  const credentials = {
    millegrille: fs.readFileSync(CERT_CA_FILE).toString('utf-8'),
    cert: fs.readFileSync(CERT_FILE).toString('utf-8'),
    key: fs.readFileSync(KEY_CA_FILE).toString('utf-8'),
    redis_password: fs.readFileSync(REDIS_PWD_FILE).toString('utf-8'),
  }
  return credentials
}

function chargerCookie() {
  let cookiePasswordLoad = null
  if(process.env.COOKIE_PASSWORD) {
    debug("server6 cookie fourni par env COOKIE_PASSWORD")
    cookiePasswordLoad = process.env.COOKIE_PASSWORD
  } else if(process.env.MG_SESSION_PASSWORD) {
    debug("server6 cookie charge de fichier %s", process.env.MG_SESSION_PASSWORD)
    cookiePasswordLoad = fs.readFileSync(process.env.MG_SESSION_PASSWORD).toString('utf-8')
  } else {
    debug("server6 cookie generer aleatoirement")
    cookiePasswordLoad = ''+uuidv4()
  }
  const secretCookiesPassword = cookiePasswordLoad
  return secretCookiesPassword
}

async function server6(app, configurerEvenements, opts) {
  opts = opts || {}

  // Preparer environnement
  const credentials = chargerCredendials()
  const hostnameEnv = process.env.HOST || 'localhost',
        portEnv = process.env.PORT,
        publicUrl = process.env.PUBLIC_URL
  const redisHost = process.env.MG_REDIS_HOST || 'redis',
        redisPortStr = process.env.MG_REDIS_PORT || '6379',
        activerSocketIoSession = opts.socketIoSession?true:false
  
  const exchange = opts.exchange || process.env.MG_EXCHANGE_DEFAUT || '2.prive'
  console.info("****************")

  // Creer un URL bien forme pour le host, permet de valider le hostname/port
  let hostname = null, port = 443, urlHost = null
  if(publicUrl) {
    urlHost = new URL(publicUrl)
  } else if(hostnameEnv) {
    urlHost = new URL(`https://${hostnameEnv}`)
  }
  if(portEnv) urlHost.port = Number(portEnv)
  port = urlHost.port || 443
  hostname = urlHost.hostname
  // const urlHost = publicUrl?new URL(publicUrl):new URL(`https://${hostnameEnv}:${portEnv}`)
  // if(portEnv) urlHost.port = Number(portEnv)
  // const hostname = urlHost.hostname,
  //       port = urlHost.port || 443

  debug("server6.initialiser Utilisation urlHost %s, exchange %s", urlHost, exchange)

  // Charger PKI
  const instPki = new MilleGrillesPKI()
  const amqpdao = new MilleGrillesAmqpDAO(instPki, {exchange})

  // Connecter a MQ
  debug("Initialiser MQ, opts: %O", opts)
  await instPki.initialiserPkiPEMS(credentials)
  amqpdao.exchange = exchange

  let urlMq
  try {
    urlMq = new URL(process.env.MG_MQ_URL)
  } catch(err) {
    urlMq = new URL('amqps://mq:5673')  // Default
    urlMq.host = process.env.MQ_HOST || urlMq.host
    urlMq.port = process.env.MQ_PORT || urlMq.port
  }
  console.info("****************")
  console.info("server6.initialiser Connecter a AMQPDAO sur %s", urlMq)
  await amqpdao.connect(urlMq.href)
  console.info("server6.initialiser AMQPDAO connexion prete")
  console.info("server6.initialiser Redis host %s:%s", redisHost, redisPortStr)
  const redisClient = redis.createClient({
    username: 'client_nodejs',
    password: credentials.redis_password,
    database: REDIS_PKI_DATABASE,
    socket: {
      host: redisHost,
      port: Number(redisPortStr), 
      tls: true,
      ca: credentials.millegrille,
      cert: credentials.cert,
      key: credentials.key,
    }
  })
  debug("Redis client information :\n%O", redisClient)
  await redisClient.connect()
  await redisClient.ping()
  console.info("Connexion Redis OK")
  console.info("****************")

  // Injecter le redisClient dans pki
  instPki.redisClient = redisClient

  // Morgan logging
  // const loggingType = process.env.NODE_ENV !== 'production' ? 'dev' : 'combined'
  app.use(morgan('combined'))

  // legacyMode: true
  const redisClientSession = redis.createClient({
    legacyMode: true,  // Requis pour session manager
    username: 'client_nodejs',
    password: credentials.redis_password,
    database: REDIS_SESSION_DATABASE,
    socket: {
      host: redisHost,
      port: Number(redisPortStr), 
      tls: true,
      ca: credentials.millegrille,
      cert: credentials.cert,
      key: credentials.key,
    }
  })
  await redisClientSession.connect()
  await redisClientSession.ping()
  const sessionMiddleware = configurerSession(hostname, redisClientSession, opts)

  // Utiliser la session pour toutes les routes
  app.use(sessionMiddleware)
  app.use(transferHeaders)
  if( ! opts.noPreAuth ) {
    const authentification = opts.verifierAuthentification || verifierAuthentification
    app.use(authentification)
  }

  // Injecter DAOs
  const {comptesUsagersDao} = initComptesUsagers(amqpdao, {hostname})
  app.use((req, res, next)=>{
    req.amqpdao = amqpdao
    req.comptesUsagersDao = comptesUsagersDao
    req.redisClient = redisClient
    req.redisClientSession = redisClientSession
    next()
  })

  // Configurer server
  const server = _initServer(app, hostname, credentials)

  // Configurer socket.io
  const sessionMiddlewareSocketio = activerSocketIoSession?sessionMiddleware:null
  const socketIo = _initSocketIo(server, amqpdao, sessionMiddlewareSocketio, configurerEvenements, opts)

  // Injecter DAOs pour socketIo
  socketIo.use((socket, next)=>{
    socket.amqpdao = amqpdao
    socket.comptesUsagersDao = comptesUsagersDao
    socket.comptesUsagers = comptesUsagersDao
    socket.redisClient = redisClient

    if(opts.verifierAutorisation) {
      socket.verifierAutorisation = opts.verifierAutorisation
    }

    next()
  })

  debug('Demarrage server %s:%s', hostname, port)
  server.listen(port)

  return {
    server, socketIo, amqpdao, urlHost, urlMq,
  }
}

function _initServer(app, hostname, certs) {
  // Serveurs supportes : https, spdy, (http2)
  const serverType = process.env.SERVER_TYPE || 'spdy'
  const serverTypeLib = require(serverType)
  debug("server: Type de serveur web : %s", serverType)

  const config = {
      hostIp: hostname,
      cert: certs.cert,
      key: certs.key,
  };

  const server = serverType === 'http2'?
    serverTypeLib.createSecureServer(config, app):
    serverTypeLib.createServer(config, app)

  return server
}

function configurerSession(hostname, redisClient, opts) {
  const pathApp = opts.pathApp || '/'

  const secretCookiesPassword = chargerCookie()

  var pathCookie = pathApp
  if(opts.cookiePath) {
    pathCookie = opts.cookiePath
  }

  var cookieName = 'millegrilles.sid'
  if(opts.pathApp) {
    cookieName = opts.pathApp + '.sid'
    cookieName = cookieName.replace('/', '')
  }
  debug("Cookie name : %O, host %s", cookieName, hostname)
  const maxAge = opts.maxAge || CONST_SESSION_TIMEOUT   // 12 heures par defaut
  const sessionTtl = Math.floor(maxAge / 1000)

  // Configuration pour adresses IP directes ou sites .onion (TOR)
  const sessionConfigNoDomain = {
    secret: secretCookiesPassword,
    store: new redisStore({
      client: redisClient,
      ttl : sessionTtl,
    }),
    name: cookieName,
    cookie: {
      path: pathCookie,
      sameSite: 'strict',
      // domain: hostname,  // Domaine flottant pour supporter adresses ip ou sites .onion
      secure: true,
      maxAge,
    },
    proxy: true,
    resave: false,
    saveUninitialized: true,  // Requis pour s'assurer de creer le cookie avant ouverture socket.io (call /verifier)
  }

  debug("Setup session hostname %s avec path : %s", hostname, pathApp)

  const sessionHandlerNoDomain = session(sessionConfigNoDomain)

  // Creer une fonction pour mapper le cookie en fonction du hostname client
  const middleware = (req, res, next) => {
    // const hostnameRecu = req.hostname || req.headers.host
    debug("middleware.session http url %s", req.url)
    let sessionHandler = sessionHandlerNoDomain
    return sessionHandler(req, res, next)
  }

  return middleware
}

function _initSocketIo(server, amqpdao, sessionMiddleware, configurerEvenements, opts) {
  opts = opts || {}

  // const maxAge = opts.maxAge || 3600000   // 1 heure par defaut
  var pathSocketio = opts.pathApp
  // var cookieName = 'millegrilles.io'
  // if(opts.pathApp) {
  //   cookieName = opts.pathApp + '.io'
  //   cookieName = cookieName.replace('/', '')
  // }
  const path = [pathSocketio, 'socket.io'].join('/')
  const ioConfig = {
    path,
    // cookie: {
    //   name: cookieName,
    //   httpOnly: true,
    //   secure: true,
    //   sameSite: "strict",
    //   maxAge,
    //   path: opts.pathApp,
    // }
  }

  if(opts.socketIoCORS) {
    ioConfig.cors = opts.socketIoCORS
  }

  debug("Demarrage socket.io avec config %O", ioConfig)
  var socketIo = socketio(server, ioConfig)

  // Injecter socketIo dans le routingKeyManager pour supporter reception
  // de messages.
  amqpdao.routingKeyManager.socketio = socketIo

  if(sessionMiddleware) {
    // Ajouter middleware session
    debug("Activer session http pour socket.io")
    const socketioSessionMiddleware = socketioSession(sessionMiddleware, {autoSave: true})
    socketIo.use(socketioSessionMiddleware)
  }

  // Ajouter middleware
  socketIo.use(socketActionsMiddleware(amqpdao, configurerEvenements, opts))
  socketIo.on('connection', (socket) => {
    debug("server6._initSocketIo: Connexion id = %s, remoteAddress = %s", socket.id, socket.conn.remoteAddress);
    socket.on('disconnect', reason=>{
      if(reason === 'transport error') {
        console.error("ERROR server6._initSocketIo: Connexion id = %s, remoteAddress = %s err: %O", socket.id, socket.conn.remoteAddress, reason);
      }
    })
  })

  return socketIo
}

function socketActionsMiddleware(amqpdao, configurerEvenements, opts) {
  opts = opts || {}

  const middleware = (socket, next) => {

    try {
      // Injecter mq
      socket.amqpdao = amqpdao
      const headers = socket.handshake.headers
      debugConnexions("server6.socketActionsMiddleware Headers: %O", headers)

      // Configuration des listeners de base utilises pour enregistrer ou
      // retirer les listeners des sockets
      const configurationEvenements = configurerEvenements(socket)
      socket.configurationEvenements = configurationEvenements
      debugConnexions("server6.socketActionsMiddleware Configuration evenements : %O", socket.configurationEvenements)

      // Parametres relayes par nginx auth
      // Injecter nom d'usager sur le socket
      let nomUsager = headers['x-user-name'],
          userId = headers['x-user-id']
      // Determiner score d'authentification
      let authScore = headers['x-user-authscore']

      if(!userId) {
        if(opts.noPreAuth) {
          // Mode maitredescomptes, utilise session pour charger valeurs si disponible
          if(!nomUsager) nomUsager = session.nomUsager
          if(!userId) userId = session.userId
          if(!authScore) authScore = session.authScore
        } else {
          debugConnexions("ERREUR server6.socketActionsMiddleware : headers.user-id n'est pas fourni")
          console.error("ERREUR server6.socketActionsMiddleware : headers.user-id n'est pas fourni")
          return socket.disconnect()
        }
      }

      if(authScore) {
        authScore = Number(authScore)
      } else {
        authScore = 0
      }

      // Conserver l'information sur le socket (utiliser par apps)
      socket.nomUsager = nomUsager
      socket.userId = userId
      socket.authScore = authScore

      // Enregistrer evenements publics de l'application
      enregistrerListener(socket, configurationEvenements.listenersPublics)
      socket.activerListenersPrives = _ => enregistrerListenersPrives(socket, configurationEvenements.listenersPrives, opts)
      socket.activerModeProtege = _ => {activerModeProtege(socket, configurationEvenements.listenersProteges)}

      if(authScore > 0) {
        // On peut activer options privees, l'usager est authentifie
        debugConnexions("Configurer evenements prives : %O", configurationEvenements.listenersPrives)
        socket.on('upgradeProtege', (params, cb)=>upgradeConnexion(socket, params, cb))
        socket.on('upgrade', (params, cb)=>upgradeConnexion(socket, params, cb))
      }

      socket.on('unsubscribe', (params, cb) => unsubscribe(socket, params, cb))
      socket.on('downgradePrive', (params, cb) => downgradePrive(socket, params, cb))
      socket.on('genererChallengeCertificat', async cb => {cb(await genererChallengeCertificat(socket))})
      socket.on('getCertificatsMaitredescles', async cb => {cb(await getCertificatsMaitredescles(socket))})

      socket.subscribe =   (params, cb) => { subscribe(socket, params, cb) }
      socket.unsubscribe = (params, cb) => { unsubscribe(socket, params, cb) }
      socket.modeProtege = false

      socket.on('getInfoIdmg', (params, cb) => getInfoIdmg(socket, params, cb, opts))

      debugConnexions("Socket events apres connexion: %O", Object.keys(socket._events))

      next()
    } catch(err) {
      console.error("server6.socketActionsMiddleware.middleware ERROR %O", err)
      socket.disconnect()
    }
  }

  return middleware

}

async function upgradeConnexion(socket, params, cb) {
  debug("upgradeConnexion Params : ", params)
  try {
    const session = socket.handshake.session
    debugConnexions("server6.enregistrerListenersPrives event upgrade %O / session %O", params, session)

    const resultat = await veriferUpgradeProtegerApp(socket, params)
    debugConnexions("server6.upgradeConnexion event upgrade resultat %O", resultat)

    if(resultat.prive === true) {
      socket.activerListenersPrives()
    }

    if(resultat.protege === true) {
      socket.activerModeProtege()
    }

    cb(resultat)
  } catch(err) {
    cb({err: ''+err, stack: err.stack})
  }
}

function enregistrerListenersPrives(socket, listenersPrives, opts) {
  opts = opts || {}
  const {nomUsager} = socket
  enregistrerListener(socket, listenersPrives)
  debugConnexions("Listeners prives usager %s\n%O", nomUsager, listenersPrives)

  socket.modePrive = true
}

function activerModeProtege(socket, listenersProteges) {
  const session = socket.handshake.session

  enregistrerListener(socket, listenersProteges)
  debugConnexions("Activation mode protege pour socketId %s", socket.id)

  socket.modeProtege = true
  socket.emit('modeProtege', {'etat': true})
}

function downgradePrive(socket, params, cb) {
  try {
    const nomUsager = socket.nomUsager
    socket.modeProtege = false
    debugConnexions("Downgrade vers mode prive - usager %s", nomUsager)
    socket.emit('modeProtege', {'etat': false})

    const listenersProtegesMillegrilles = socket.configurationEvenements.listenersProteges
    debugConnexions("Listeners proteges millegrilles\n%O", listenersProtegesMillegrilles)
    retirerListener(socket, listenersProtegesMillegrilles)

    // Retrait subscribe
    socket.removeAllListeners('subscribe')

    debugConnexions("Socket events apres downgrade: %O", Object.keys(socket._events))

    if(cb) cb(true)
  } catch(err) {
    console.error('server6.downgradePrive error : %O', err)
    if(cb) cb(false)
  }
}

function enregistrerListener(socket, collectionListener) {
  if(!collectionListener) return
  
  debugConnexions("server6.enregistrerListeners %O", collectionListener.map(item=>item.eventName))

  const eventsExistants = Object.keys(socket._events)
  collectionListener.forEach(listener=>{
    const {eventName, callback} = listener
    if(eventsExistants.includes(eventName)) {
      console.warn("enregistrerListener Tentative d'enregistrement d'evenement en double (%s), on skip", eventName)
    } else if(eventName) {
      debugConnexions("Ajout listener %s", listener.eventName)
      socket.on(eventName, callback)
    }
  })
}

function retirerListener(socket, collectionListener) {
  for(let idx in collectionListener) {
    const listener = collectionListener[idx]
    debugConnexions("Retrait du listener %s", listener.eventName)
    socket.removeAllListeners(listener.eventName) //, listener.callback)
  }
}

function subscribe(socket, params, cb) {
  try {
    debugConnexions("Subscribe : %O", params)

    const { userId, roomParam, mapper} = params
    const routingKeys = params.routingKeys || []
    const exchanges = params.exchanges || []
    debugConnexions("Subscribe exchanges %O, %O, userId=%O", exchanges, routingKeys, userId)

    if(routingKeys.length === 0 || exchanges.length === 0) {
      debug("subscribe ERROR routingKeys ou exchanges vide : %O / %O", routingKeys, exchanges)
      if(cb) cb({ok: false, err: "Routings keys/exchanges vide"})
      return
    }

    const amqpdao = socket.amqpdao
    const channel = amqpdao.channel,
          reply_q = amqpdao.reply_q


    // Supporter plusieurs rooms a la fois
    let roomParamListe = [roomParam]  // Not: roomParam peut etre null
    if(roomParam && Array.isArray(roomParam)) {
      roomParamListe = roomParam  // deja une liste
    }
    roomParamListe.forEach(roomParamEffectif=>{
      exchanges.forEach(exchange=>{
        amqpdao.routingKeyManager.addRoutingKeysForSocket(socket, routingKeys, exchange, channel, reply_q, {userId, roomParam: roomParamEffectif, mapper})
      })
    })

    debugConnexions("Socket events apres subscribe: %O", Object.keys(socket._events))

    // Collapse les partitions : evenement.domaine.partition.action devient evenement.domaine.action
    let rkEvents = routingKeys
      .map(item=>item.split('.'))
      .reduce((acc, rk)=>{
        if(rk.length === 4) rk = [rk[0], rk[1], rk[3]]
        acc[rk.join('.')] = true
        return acc
      }, {})
    rkEvents = Object.keys(rkEvents)
    debugConnexions("Sockets events pour client : %O", rkEvents)

    if(cb) cb({ok: true, routingKeys: rkEvents, exchanges, roomParam})
  } catch(err) {
    console.error('server6.subscribe error : %O', err)
    if(cb) cb({ok: false, err: ''+err})
  }
}

function unsubscribe(socket, params, cb) {
  try {
    const routingKeys = params.routingKeys || [],
          exchanges = params.exchanges || [],
          roomParam = params.roomParam

    if(routingKeys.length === 0 || exchanges.length === 0) {
      debug("unsubscribe ERROR routingKeys ou exchanges vide : %O / %O / %O", routingKeys, exchanges, roomParam)
      if(cb) cb({ok: false, err: "Routings keys/exchanges vide"})
      return
    }

    // Supporter plusieurs rooms a la fois
    let roomParamListe = [roomParam]  // Not: roomParam peut etre null
    if(roomParam && Array.isArray(roomParam)) {
      roomParamListe = roomParam  // deja une liste
    }
    roomParamListe.forEach(roomParamEffectif=>{
      exchanges.forEach(ex=>{
        routingKeys.forEach(rk=>{
          let eventName = `${ex}/${rk}`
          if(roomParamEffectif) eventName += '/' + roomParamEffectif
          debug('Unsubscribe socket %s de %s', socket.id, eventName)
          socket.leave(eventName)
        })
      })
    })
    
    // Collapse les partitions : evenement.domaine.partition.action devient evenement.domaine.action
    let rkEvents = routingKeys
      .map(item=>item.split('.'))
      .reduce((acc, rk)=>{
        if(rk.length === 4) rk = [rk[0], rk[1], rk[3]]
        acc[rk.join('.')] = true
        return acc
      }, {})
    rkEvents = Object.keys(rkEvents)
    debugConnexions("Sockets events pour client : %O", rkEvents)

    if(cb) cb({ok: true, routingKeys: rkEvents, exchanges})
  } catch(err) {
    console.error('server6.unsubscribe error : %O', err)
    if(cb) cb({ok: false, err: ''+err})
  }

}

function getInfoIdmg(socket, params, cb, opts) {
  const session = socket.handshake.session,
        headers = socket.handshake.headers
  debugConnexions("server6.getInfoIdmg headers: %O\nsession %O", headers, session)

  const idmg = socket.amqpdao.pki.idmg
  let nomUsager = headers['x-user-name'] || socket.nomUsager
  let userId = headers['x-user-id'] || socket.userId

  if(!userId && opts.noPreAuth) {
    // Maitre des comptes - permettre d'utiliser la session pour recuperer l'information
    nomUsager = session.nomUsager
    userId = session.userId
  }

  const reponse = {idmg, nomUsager, userId}
  debugConnexions("server6.getInfoIdmg reponse: %O", reponse)

  cb(reponse)
}

async function getCertificatsMaitredescles(socket) {
  debugConnexions("server6.getCertificatsMaitredescles")
  const amqpdao = socket.amqpdao
  try {
    const reponse = await amqpdao.getCertificatsMaitredescles()
    debug("server6.getCertificatsMaitredescles Reponse ", reponse)
    return reponse
  } catch(err) {
    console.error(new Date() + ' serveur6.getCertificatsMaitredescles Erreur ', err)
    return {ok: false, err: ''+err}
  }
}

function transferHeaders(req, res, next) {
  /* Transferer infortion des headers vers la session. */
  const session = req.session
  debug("transferHeaders Information session : %O \nHeaders : %O", session, req.headers)
  const nomUsager = req.headers['x-user-name']
  if( session  && nomUsager ) {
    // Conserver / override cookie
    session.nomUsager = nomUsager
    session.userId = req.headers['x-user-id']
    session.authScore = req.headers['x-user-authscore']
    session.save()
  }
  next()
}

function verifierAuthentification(req, res, next) {
  const session = req.session
  if( ! (session.nomUsager && session.userId) ) {
    debug("verifierAuthentification Acces refuse (nomUsager et userId null)")
    debugConnexions("Nom usager/userId ne sont pas inclus dans les req.headers : %O", req.headers)
    res.append('Access-Control-Allow-Origin', '*')  // S'assurer que le message est recu cross-origin
    return res.sendStatus(403)
  }
  next()
}

module.exports = server6
