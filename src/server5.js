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

const debug = debugLib('millegrilles:server5'),
      debugConnexions = debugLib('millegrilles:server5:connexions'),
      redisStore = redisConnect(session)

// const { genererChallengeCertificat, veriferUpgradeProtegerApp } = require('./authentification')

// Preparer certificats
function chargerCertificats() {
  const certPems = {
    millegrille: fs.readFileSync(process.env.MG_MQ_CAFILE).toString('utf-8'),
    cert: fs.readFileSync(process.env.MG_MQ_CERTFILE).toString('utf-8'),
    key: fs.readFileSync(process.env.MG_MQ_KEYFILE).toString('utf-8'),
  }
  return certPems
}

function chargerCookie() {
  let cookiePasswordLoad = null
  if(process.env.COOKIE_PASSWORD) {
    debug("server5 cookie fourni par env COOKIE_PASSWORD")
    cookiePasswordLoad = process.env.COOKIE_PASSWORD
  } else if(process.env.MG_SESSION_PASSWORD) {
    debug("server5 cookie charge de fichier %s", process.env.MG_SESSION_PASSWORD)
    cookiePasswordLoad = fs.readFileSync(process.env.MG_SESSION_PASSWORD).toString('utf-8')
  } else {
    debug("server5 cookie generer aleatoirement")
    cookiePasswordLoad = ''+uuidv4()
  }
  const secretCookiesPassword = cookiePasswordLoad
  return secretCookiesPassword
}

async function server5(app, configurerEvenements, opts) {
  opts = opts || {}

  // Preparer environnement
  const certPems = chargerCertificats()
  const hostname = process.env.HOST,
        port = process.env.PORT || '443'
  const redisHost = process.env.MG_REDIS_HOST || 'redis',
        redisPortStr = process.env.MG_REDIS_PORT || '6379'
  
  const exchange = opts.exchange || process.env.MG_EXCHANGE_DEFAUT || '3.protege'
  console.info("****************")
  debug("server5.initialiser Utilisation hostname %s, exchange %s", hostname, exchange)

  // Charger PKI
  const instPki = new MilleGrillesPKI()
  const amqpdao = new MilleGrillesAmqpDAO(instPki, {exchange})

  // Connecter a MQ
  debug("Initialiser MQ, opts: %O", opts)
  await instPki.initialiserPkiPEMS(certPems)
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
  console.info("server5.initialiser Connecter a AMQPDAO sur %s", urlMq)
  await amqpdao.connect(urlMq.href)
  console.info("server5.initialiser AMQPDAO connexion prete")
  console.info("server5.initialiser Redis host %s:%s", redisHost, redisPortStr)
  const redisClient = redis.createClient({
    host: redisHost,
    port: Number(redisPortStr),
  })
  console.info("****************")

  // Injecter le redisClient dans pki
  instPki.redisClient = redisClient

  // Morgan logging
  const loggingType = process.env.NODE_ENV !== 'production' ? 'dev' : 'combined'
  app.use(morgan('combined'))

  const sessionMiddleware = configurerSession(hostname, redisClient, opts)

  // Utiliser la session pour toutes les routes
  app.use(sessionMiddleware)
  app.use(transferHeaders)
  if( ! opts.noPreAuth ) app.use(verifierAuthentification)

  // Configurer server et socket.io
  const server = _initServer(app, hostname, certPems)
  const socketIo = _initSocketIo(server, amqpdao, sessionMiddleware, configurerEvenements, opts)

  // Injecter DAOs
  const {comptesUsagersDao} = initComptesUsagers(amqpdao)
  app.use((req, res, next)=>{
    req.amqpdao = amqpdao
    req.comptesUsagersDao = comptesUsagersDao
    next()
  })
  socketIo.use((socket, next)=>{
    socket.amqpdao = amqpdao
    socket.comptesUsagersDao = comptesUsagersDao
    socket.comptesUsagers = comptesUsagersDao

    if(opts.verifierAutorisation) {
      socket.verifierAutorisation = opts.verifierAutorisation
    }

    next()
  })

  debug('Demarrage server %s:%s', hostname, port)
  server.listen(port)

  return {
    server, socketIo, amqpdao
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
  debug("Cookie name : %O", cookieName)
  const maxAge = opts.maxAge || 3600000   // 1 heure par defaut

  const sessionConfig = {
    secret: secretCookiesPassword,
    store: new redisStore({
      client: redisClient,
      ttl :  260,
    }),
    name: cookieName,
    cookie: {
      path: pathCookie,
      domain: hostname,
      sameSite: 'strict',
      secure: true,
      maxAge,
    },
    proxy: true,
    resave: false,
    saveUninitialized: true,  // Requis pour s'assurer de creer le cookie avant ouverture socket.io (call /verifier)
  }

  debug("Setup session hostname %s avec path : %s\n%O", hostname, pathApp, sessionConfig)
  return session(sessionConfig)
}

function _initSocketIo(server, amqpdao, sessionMiddleware, configurerEvenements, opts) {
  opts = opts || {}

  var pathSocketio = opts.pathApp
  var cookieName = 'millegrilles.io'
  if(opts.pathApp) {
    cookieName = opts.pathApp + '.io'
    cookieName = cookieName.replace('/', '')
  }
  const path = [pathSocketio, 'socket.io'].join('/')
  const ioConfig = {
    path,
    cookie: cookieName,
    // cookie: {
    //   name: cookieName,
    //   httpOnly: true,
    //   sameSite: "strict",
    //   maxAge: 86400
    // }
  }

  if(opts.socketIoCORS) {
    ioConfig.cors = opts.socketIoCORS
  }

  debug("Demarrage socket.io avec config %O", ioConfig)
  var socketIo = socketio(server, ioConfig)

  // Morgan logging
  const loggingType = process.env.NODE_ENV !== 'production' ? 'dev' : 'combined'

  // Injecter socketIo dans le routingKeyManager pour supporter reception
  // de messages.
  amqpdao.routingKeyManager.socketio = socketIo

  // Ajouter middleware
  const socketioSessionMiddleware = socketioSession(sessionMiddleware, {autoSave: true})
  socketIo.use(socketioSessionMiddleware)
  socketIo.use(socketActionsMiddleware(amqpdao, configurerEvenements, opts))
  socketIo.on('connection', (socket) => {
    debug("server5._initSocketIo: Connexion id = %s, remoteAddress = %s", socket.id, socket.conn.remoteAddress);
    socket.on('disconnect', reason=>{
      if(reason === 'transport error') {
        console.error("ERROR server5._initSocketIo: Connexion id = %s, remoteAddress = %s err: %O", socket.id, socket.conn.remoteAddress, reason);
      }
    })
  })

  return socketIo
}

function socketActionsMiddleware(amqpdao, configurerEvenements, opts) {
  opts = opts || {}

  const middleware = (socket, next) => {
    // Injecter mq
    socket.amqpdao = amqpdao
    const headers = socket.handshake.headers
    debugConnexions("server5.socketActionsMiddleware Headers: %O", headers)

    // Configuration des listeners de base utilises pour enregistrer ou
    // retirer les listeners des sockets
    const configurationEvenements = configurerEvenements(socket)
    socket.configurationEvenements = configurationEvenements
    debugConnexions("server5.socketActionsMiddleware Configuration evenements : %O", socket.configurationEvenements)

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
        debugConnexions("ERREUR server5.socketActionsMiddleware : headers.user-id n'est pas fourni")
        console.error("ERREUR server5.socketActionsMiddleware : headers.user-id n'est pas fourni")
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
      // socket.activerListenersPrives()
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
  }

  return middleware

}

async function upgradeConnexion(socket, params, cb) {
  const session = socket.handshake.session

  debugConnexions("server5.enregistrerListenersPrives event upgrade %O / session %O", params, session)
  try {
    const resultat = await veriferUpgradeProtegerApp(socket, params)
    debugConnexions("server5.upgradeConnexion event upgrade resultat %O", resultat)

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

  // socket.on('upgradeProtege', async (params, cb) => {
  //   debugConnexions("server5.enregistrerListenersPrives event upgradeProtege %O / session %O", params, session)
  //   try {
  //     const resultat = await veriferUpgradeProtegerApp(socket, params, opts)
  //     debugConnexions("server5.enregistrerListenersPrives event upgradeProtege resultat %O", resultat)
  //     cb(resultat)
  //   } catch(err) {
  //     cb({err: ''+err, stack: err.stack})
  //   }
  // })
}

function activerModeProtege(socket, listenersProteges) {
  const session = socket.handshake.session

  enregistrerListener(socket, listenersProteges)
  debugConnexions("Activation mode protege pour socketId %s\n%O", socket.id, Object.values(socket._events))

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
    console.error('server5.downgradePrive error : %O', err)
    if(cb) cb(false)
  }
}

function enregistrerListener(socket, collectionListener) {
  debugConnexions("server5.enregistrerListener %O", collectionListener)
  for(let idx in collectionListener) {
    const listener = collectionListener[idx]
    debugConnexions("Ajout listener %s", listener.eventName)
    if(listener.eventName) {
      socket.on(listener.eventName, listener.callback)
    }
  }
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

    const routingKeys = params.routingKeys
    const niveauxSecurite = params.exchange || ['2.prive']
    const userId = params.userId
    debugConnexions("Subscribe securite %O, %O, userId=%O", niveauxSecurite, routingKeys, userId)

    const amqpdao = socket.amqpdao
    const channel = amqpdao.channel,
          reply_q = amqpdao.reply_q

    niveauxSecurite.forEach(niveauSecurite=>{
      amqpdao.routingKeyManager.addRoutingKeysForSocket(socket, routingKeys, niveauSecurite, channel, reply_q, {userId})
    })

    debugConnexions("Socket events apres subscribe: %O", Object.keys(socket._events))

    if(cb) cb(true)
  } catch(err) {
    console.error('server5.subscribe error : %O', err)
    if(cb) cb(false)
  }
}

function unsubscribe(socket, params, cb) {
  try {
    const routingKeys = params.routingKeys
    if(routingKeys) {
      routingKeys.forEach(rk=>{
        socket.leave(rk)
      })
    }
    if(cb) cb(true)
  } catch(err) {
    console.error('server5.subscribe error : %O', err)
    if(cb) cb(false)
  }

}

function getInfoIdmg(socket, params, cb, opts) {
  const session = socket.handshake.session,
        headers = socket.handshake.headers
  debugConnexions("server5.getInfoIdmg headers: %O\nsession %O", headers, session)

  const idmg = socket.amqpdao.pki.idmg
  let nomUsager = headers['x-user-name'] || socket.nomUsager
  let userId = headers['x-user-id'] || socket.userId

  if(!userId && opts.noPreAuth) {
    // Maitre des comptes - permettre d'utiliser la session pour recuperer l'information
    nomUsager = session.nomUsager
    userId = session.userId
  }

  const reponse = {idmg, nomUsager, userId}
  debugConnexions("server5.getInfoIdmg reponse: %O", reponse)

  cb(reponse)
}

async function getCertificatsMaitredescles(socket, cb) {
  debugConnexions("server5.getCertificatsMaitredescles")

  const amqpdao = socket.amqpdao
  const domaineAction = 'MaitreDesCles.certMaitreDesCles'
  const params = {}

  try {
    debugConnexions("Requete certificats maitredescles")
    const reponse = await amqpdao.transmettreRequete(domaineAction, params, {decoder: true})
    debugConnexions("Reponse certificats maitredescles %O", reponse)
    return reponse
  } catch(err) {
    debugConnexions("Erreur traitement liste applications\n%O", err)
    return {err}
  }

}

function transferHeaders(req, res, next) {
  /* Transferer infortion des headers vers la session. */
  const session = req.session
  if( ! session.nomUsager ) {
    session.nomUsager = req.headers['x-user-name']
    session.userId = req.headers['x-user-id']
    session.authScore = req.headers['x-user-authscore']
  }
  next()
}

function verifierAuthentification(req, res, next) {
  const session = req.session
  if( ! (session.nomUsager && session.userId) ) {
    debugConnexions("Nom usager/userId ne sont pas inclus dans les req.headers : %O", req.headers)
    res.append('Access-Control-Allow-Origin', '*')  // S'assurer que le message est recu cross-origin
    return res.sendStatus(403)
  }
  next()
}

// function cors(req, res, next) {
//   // CORS
//   res.append('Access-Control-Allow-Origin', '*')
//   next()
// }

// function calculerAuthScore(auth) {
//   if(!auth) return 0
//   const score = Object.values(auth)
//     .reduce((score, item)=>{return score + item}, 0)
//   return score
// }

module.exports = { server5 }
