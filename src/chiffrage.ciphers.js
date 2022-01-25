/* Facade pour crypto de nodejs. */
import crypto from 'crypto'
import { setCiphers, Hacheur } from '@dugrema/millegrilles.utiljs'
import '../src/hachage'

// console.info("Ciphers disponibles : %s", crypto.getCiphers().reduce((liste, item)=>{
//     return liste + '\n' + item
// }, ''))

async function creerCipherChacha20Poly1305(key, nonce, opts) {
    opts = opts || {}
    const digestAlgo = opts.digestAlgo || 'blake2b-512'
    const cipher = crypto.createCipheriv('chacha20-poly1305', key, nonce, { authTagLength: 16 })
    const hacheur = new Hacheur({hashingCode: digestAlgo})
    await hacheur.ready
    let tag = null
    let hachage = null
    return {
        update: async data => {
            const ciphertext = cipher.update(data)
            await hacheur.update(ciphertext)
            return ciphertext
        },
        finalize: async () => {
            cipher.final()
            tag = cipher.getAuthTag()
            hachage = await hacheur.finalize()
            return {tag, hachage}
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
    const {tag, hachage} = await cipher.finalize()
    return {ciphertext, tag, hachage}
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
    const cipher = await creerDecipherChacha20Poly1305(key, nonce, opts)
    const ciphertext = await cipher.update(data)
    await cipher.finalize(tag)
    return ciphertext
}

const ciphers = {
    // Nodejs Crypto
    'chacha20-poly1305': {
        encrypt: encryptChacha20Poly1305,
        decrypt: decryptChacha20Poly1305,
        getCipher: creerCipherChacha20Poly1305,
        getDecipher: creerDecipherChacha20Poly1305,
        nonceSize: 12,
    }
}

setCiphers(ciphers)
