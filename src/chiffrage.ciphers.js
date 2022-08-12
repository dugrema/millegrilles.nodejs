/* Facade pour crypto de nodejs. */
const crypto = require('crypto')
const { base64 } = require('multiformats/bases/base64')
const chiffrageCiphers = require('@dugrema/millegrilles.utiljs/src/chiffrage.ciphers')
const _sodium = require('libsodium-wrappers')

const hachageLib = require('./hachage')

/* ----- Debut section Chacha20Poly1305 ----- */ 

async function creerCipherChacha20Poly1305(key, nonce, opts) {
    opts = opts || {}
    const digestAlgo = opts.digestAlgo || 'blake2b-512'
    const cipher = crypto.createCipheriv('chacha20-poly1305', key, nonce, { authTagLength: 16 })
    const hacheur = new hachageLib.Hacheur({hashingCode: digestAlgo})
    await hacheur.ready
    let tag = null, hachage = null, taille = 0
    return {
        update: async data => {
            const ciphertext = cipher.update(data)
            await hacheur.update(ciphertext)
            taille += data.length
            return ciphertext
        },
        finalize: async () => {
            cipher.final()
            tag = cipher.getAuthTag()
            hachage = await hacheur.finalize()
            return {rawTag: tag, tag: base64.encode(tag), hachage, taille}
        },
        tag: () => tag,
        hachage: () => hachage,
    }
}

async function creerDecipherChacha20Poly1305(key, nonce) {
    const decipher = crypto.createDecipheriv('chacha20-poly1305', key, nonce, { authTagLength: 16 })
    return {
        update: data => decipher.update(data),
        finalize: tag => {
            if(typeof(tag)==='string') tag = base64.decode(tag)
            decipher.setAuthTag(tag);
            return decipher.final()
        },
    }
}

/**
 * One pass encrypt ChaCha20Poly1305.
 * @param {*} key 
 * @param {*} nonce 
 * @param {*} data 
 * @param {*} opts 
 */
async function encryptChacha20Poly1305(key, nonce, data, opts) {
    const cipher = await creerCipherChacha20Poly1305(key, nonce, opts)
    const ciphertext = await cipher.update(data)
    const {tag, rawTag, hachage} = await cipher.finalize()
    return {ciphertext, tag, rawTag, hachage}
}

/**
 * One pass decrypt ChaCha20Poly1305.
 * @param {*} key 
 * @param {*} nonce 
 * @param {*} data 
 * @param {*} tag 
 * @param {*} opts 
 */
async function decryptChacha20Poly1305(key, nonce, data, tag, opts) {
    if(typeof(tag)==='string') tag = base64.decode(tag)
    const cipher = await creerDecipherChacha20Poly1305(key, nonce, opts)
    const ciphertext = await cipher.update(data)
    await cipher.finalize(tag)
    return ciphertext
}

/* ----- Fin section Chacha20Poly1305 ----- */ 

/* ----- Debut section stream XChacha20Poly1305 ----- */ 

const OVERHEAD_MESSAGE = 17,
      DECIPHER_MESSAGE_SIZE = 64 * 1024,
      MESSAGE_SIZE = DECIPHER_MESSAGE_SIZE - OVERHEAD_MESSAGE

async function creerStreamCipherXChacha20Poly1305(opts) {
    opts = opts || {}
    const digestAlgo = opts.digestAlgo || 'blake2b-512'
    const messageBuffer = new Uint8Array(MESSAGE_SIZE)
    let positionBuffer = 0,
        tailleOutput = 0

    let { key } = opts
    if(key) {
        if(typeof(key) === 'string') key = base64.decode(key)
    } else {
        key = getRandom(32)
    }

    // Preparer libsodium, hachage (WASM)
    const hacheur = new hachageLib.Hacheur({hashingCode: digestAlgo})
    await hacheur.ready
    await _sodium.ready
    const sodium = _sodium

    // Preparer cipher
    const res = sodium.crypto_secretstream_xchacha20poly1305_init_push(key)
    const state_out = res.state
    const header = base64.encode(new Uint8Array(res.header))
    
    let hachage = null
    return {
        update: async data => {
            data = Uint8Array.from(data)
            let ciphertext = null

            // Chiffrer tant qu'on peut remplir le buffer
            while (positionBuffer + data.length >= MESSAGE_SIZE) {
                // Slice data
                let endPos = MESSAGE_SIZE - positionBuffer
                messageBuffer.set(data.slice(0, endPos), positionBuffer)
                data = data.slice(endPos)

                // Chiffrer
                let ciphertextMessage = sodium.crypto_secretstream_xchacha20poly1305_push(
                    state_out, messageBuffer, null, sodium.crypto_secretstream_xchacha20poly1305_TAG_MESSAGE)
                if(ciphertextMessage === false) throw new CipherError('Erreur encodage')
                if(!ciphertext) ciphertext = ciphertextMessage
                else ciphertext = new Uint8Array([...ciphertext, ...ciphertextMessage])

                positionBuffer = 0  // Reset position
            }

            // Inserer data restant dans le buffer
            if(positionBuffer + data.length <= MESSAGE_SIZE) {
                messageBuffer.set(data, positionBuffer)
                positionBuffer += data.length
            }
            
            if(ciphertext) {
                await hacheur.update(ciphertext)
                tailleOutput += ciphertext.length
            }

            return ciphertext
        },
        finalize: async () => {
            let ciphertextMessage = sodium.crypto_secretstream_xchacha20poly1305_push(
                state_out, messageBuffer.slice(0,positionBuffer), null, sodium.crypto_secretstream_xchacha20poly1305_TAG_FINAL)
            if(ciphertextMessage === false) throw new CipherError('Erreur encodage')
            
            await hacheur.update(ciphertextMessage)
            tailleOutput += ciphertextMessage.length
            hachage = await hacheur.finalize()

            return {key, header, hachage, taille: tailleOutput, format: 'mgs4', ciphertext: ciphertextMessage}
        },
        header: () => header,
        hachage: () => hachage,
        key: () => key,
        format: () => 'mgs4'
    }
}

async function creerStreamDecipherXChacha20Poly1305(key, header) {
    const messageBuffer = new Uint8Array(DECIPHER_MESSAGE_SIZE)
    let positionBuffer = 0,
        tailleOutput = 0

    if(typeof(key)==='string') key = base64.decode(key)
    if(typeof(header)==='string') header = base64.decode(header)

    // Preparer libsodium (WASM)
    await _sodium.ready
    const sodium = _sodium

    // Preparer decipher
    const state_in = sodium.crypto_secretstream_xchacha20poly1305_init_pull(header, key)

    return {
        update: async data => {
            data = Uint8Array.from(data)
            let message = null

            // Chiffrer tant qu'on peut remplir le buffer
            while (positionBuffer + data.length >= DECIPHER_MESSAGE_SIZE) {
                // Slice data
                let endPos = DECIPHER_MESSAGE_SIZE - positionBuffer
                messageBuffer.set(data.slice(0, endPos), positionBuffer)
                data = data.slice(endPos)

                // Dechiffrer
                const resultatDechiffrage = sodium.crypto_secretstream_xchacha20poly1305_pull(state_in, messageBuffer)
                if(resultatDechiffrage === false) throw new DecipherError('Erreur dechiffrage')

                if(!message) message = resultatDechiffrage.message
                else message = new Uint8Array([...message, ...resultatDechiffrage.message])

                positionBuffer = 0  // Reset position
            }

            // Inserer data restant dans le buffer
            if(positionBuffer + data.length <= DECIPHER_MESSAGE_SIZE) {
                messageBuffer.set(data, positionBuffer)
                positionBuffer += data.length
            }
            
            if(message) {
                tailleOutput += message.length
            }

            return message
        },
        finalize: async () => {
            let decipheredMessage
            if(positionBuffer) {
                const resultat = sodium.crypto_secretstream_xchacha20poly1305_pull(state_in, messageBuffer.slice(0,positionBuffer))
                if(resultat === false) throw new DecipherError('Erreur dechiffrage')
                const {message} = resultat
                decipheredMessage = message
                tailleOutput += decipheredMessage.length
            }
            return {taille: tailleOutput, message: decipheredMessage}
        }
    }
}

/**
 * One pass encrypt ChaCha20Poly1305.
 * @param {*} key 
 * @param {*} nonce 
 * @param {*} data 
 * @param {*} opts 
 */
async function encryptStreamXChacha20Poly1305(data, opts) {
    const cipher = await creerStreamCipherXChacha20Poly1305(opts)

    // Creer buffer pour resultat
    const tailleBuffer = (Math.ceil(data.length / MESSAGE_SIZE) + 1) * DECIPHER_MESSAGE_SIZE
    const buffer = new Uint8Array(tailleBuffer)
    let positionLecture = 0, positionEcriture = 0

    while(positionLecture < data.length) {
        const tailleLecture = Math.min(data.length - positionLecture, MESSAGE_SIZE)
        const dataSlice = data.slice(positionLecture, positionLecture+tailleLecture)
        const output = await cipher.update(dataSlice)
        if(output) {
            buffer.set(output, positionEcriture)
            positionEcriture += output.length
        }

        positionLecture += tailleLecture
    }
    let {ciphertext, header, hachage, key, format} = await cipher.finalize()

    if(ciphertext) {
        // Concatener
        buffer.set(ciphertext, positionEcriture)
        positionEcriture += ciphertext.length
    }

    return {key, ciphertext: buffer.slice(0, positionEcriture), header, hachage, format}
}

/**
 * One pass decrypt stream XChaCha20Poly1305.
 * @param {*} key 
 * @param {*} nonce 
 * @param {*} data 
 * @param {*} tag 
 * @param {*} opts 
 */
async function decryptStreamXChacha20Poly1305(key, header, ciphertext) {
    const decipher = await creerStreamDecipherXChacha20Poly1305(key, header)

    // Creer buffer pour resultat
    const tailleBuffer = Math.ceil(ciphertext.length / DECIPHER_MESSAGE_SIZE) * MESSAGE_SIZE
    const buffer = new Uint8Array(tailleBuffer)
    let positionLecture = 0, positionEcriture = 0

    while(positionLecture < ciphertext.length) {
        const tailleLecture = Math.min(ciphertext.length - positionLecture, DECIPHER_MESSAGE_SIZE)
        const cipherSlice = ciphertext.slice(positionLecture, positionLecture+tailleLecture)
        const output = await decipher.update(cipherSlice)
        if(output) {
            buffer.set(output, positionEcriture)
            positionEcriture += output.length
        }

        positionLecture += tailleLecture
    }
    let {message} = await decipher.finalize()

    if(message) {
        // Concatener
        buffer.set(message, positionEcriture)
        positionEcriture += message.length
    }

    return buffer.slice(0, positionEcriture)
}

/* ----- Fin section stream XChacha20Poly1305 ----- */ 

// const chacha20poly1305Algorithm = {
//     encrypt: (data, opts) => encryptChacha20Poly1305,
//     decrypt: (key, data, opts) => decryptChacha20Poly1305(key, opts.nonce||opts.iv, data, opts.tag, opts),
//     getCipher: creerCipherChacha20Poly1305,
//     getDecipher: (key, opts) => creerDecipherChacha20Poly1305(key, opts.nonce||opts.iv),
//     nonceSize: 12,
// }
// const streamXChacha20poly1305Algorithm = {
//     encrypt: encryptStreamXChacha20Poly1305,
//     decrypt: decryptStreamXChacha20Poly1305,
//     getCipher: creerStreamCipherXChacha20Poly1305,
//     getDecipher: creerStreamDecipherXChacha20Poly1305,
//     messageSize: MESSAGE_SIZE,
// }

const streamXchacha20poly1305Algorithm = {
    encrypt: encryptStreamXChacha20Poly1305,
    decrypt: (key, data, opts) => decryptStreamXChacha20Poly1305(key, opts.header, data),
    getCipher: creerStreamCipherXChacha20Poly1305,
    getDecipher: (key, opts) => creerStreamDecipherXChacha20Poly1305(key, opts.header),
    messageSize: MESSAGE_SIZE,
    stream: true,
}

const chacha20poly1305Algorithm = {
    encrypt: (data, opts) => encryptChacha20Poly1305(opts.key, opts.iv||opts.nonce, data, opts),
    decrypt: (key, data, opts) => decryptChacha20Poly1305(key, opts.nonce||opts.iv, data, opts.tag),
    getCipher: (opts) => creerCipherChacha20Poly1305(opts.key, opts.nonce||opts.iv, opts),
    getDecipher: (key, opts) => creerDecipherChacha20Poly1305(key, opts.nonce||opts.iv),
    stream: true,
}

const ciphers = {
    'chacha20-poly1305': chacha20poly1305Algorithm,
    'stream-xchacha20poly1305': streamXchacha20poly1305Algorithm,
    'mgs3': chacha20poly1305Algorithm,
    'mgs4': streamXchacha20poly1305Algorithm,
}

chiffrageCiphers.setCiphers(ciphers)

module.exports = { ciphers }
