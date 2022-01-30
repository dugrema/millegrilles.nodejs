require('./hachage')  // Charger hachage nodejs
const { setCiphers, getCipher } = require('./chiffrage.ciphers')
const chiffrage = require('@dugrema/millegrilles.utiljs/src/chiffrage')

module.exports = { setCiphers, getCipher, ...chiffrage }
