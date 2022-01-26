// export { server5 } from './server5.js'

// export * as authentification from './authentification.js'
// export { MilleGrillesAmqpDAO } from './amqpdao'
// export * from './authentification.js'
// export { MilleGrillesPKI } from './pki'
// export * as webauthn from './webauthn'

// // Exporter hachage - inject aussi les algorithmes de hachage natifs dans utiljs (setHacheurs)
// export * from './hachage.js'

// // Wiring chiffrage (et hachage, inclus dans chiffrage)
// import './chiffrage.ciphers'

//

const { server5 } = require('./server5.js')

const authentification = require('./authentification.js')
const { MilleGrillesAmqpDAO } = require('./amqpdao')
const { MilleGrillesPKI } = require('./pki')
const webauthn = require('./webauthn')

// Exporter hachage - inject aussi les algorithmes de hachage natifs dans utiljs (setHacheurs)
// const hachage = require('./hachage.js')

// Wiring chiffrage (et hachage, inclus dans chiffrage)
require('./chiffrage.ciphers')

module.exports = {
    server5,
    authentification,
    MilleGrillesAmqpDAO,
    MilleGrillesPKI,
    webauthn,
}
