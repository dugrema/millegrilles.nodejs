// Sanity test de chiffrage.ciphers.js
//require('../src/chiffrage.ciphers')
// const { 
//     chiffrer, dechiffrer, preparerCipher, preparerDecipher,
//     ed25519: utiljsEd25519, 
//     preparerCommandeMaitrecles,
//     genererClePrivee, genererCertificatMilleGrille
// } = require('@dugrema/millegrilles.utiljs')

const { chiffrer, dechiffrer, preparerCipher, preparerDecipher, preparerCommandeMaitrecles } = require('../src/chiffrage')
const utiljsEd25519 = require('@dugrema/millegrilles.utiljs/src/chiffrage.ed25519')
const { genererClePrivee, genererCertificatMilleGrille } = require('@dugrema/millegrilles.utiljs/src/certificats')

const { ed25519 } = require('@dugrema/node-forge')
const { base64 } = require('multiformats/bases/base64')

// const { ed25519 } = nodeforge

async function genererCert() {
    const clePrivee = genererClePrivee()
    console.debug("Cle privee pem : %O", clePrivee)
    const certInfo = await genererCertificatMilleGrille(clePrivee.pem)
    console.debug("certInfo: %O", certInfo)
    return {...certInfo, clePrivee}
}

test('chiffrage/dechiffrage contenu one-pass', async () => {
    console.debug("Test chiffrage/dechiffrage")

    const { publicKey, privateKey } = ed25519.generateKeyPair()
    const publicKeyBytes = publicKey.publicKeyBytes,
          privateKeyBytes = privateKey.privateKeyBytes

    const message = new TextEncoder().encode("Un message secret")
    console.debug("Message bytes : %O", message)

    const contenuChiffre = await chiffrer(message, {clePubliqueEd25519: publicKeyBytes})
    console.debug("Contenu chiffre : %O", contenuChiffre)
    const {ciphertext, secretKey, meta} = contenuChiffre,
          {iv, tag} = meta

    const contenuDechiffre = new Uint8Array(await dechiffrer(ciphertext, secretKey, iv, tag))
    console.debug("Contenu dechiffre : %O", contenuDechiffre)

    expect.assertions(1)
    expect(message).toEqual(contenuDechiffre)
})

test('chiffrage/dechiffrage cle ed25519', async () => {
    console.debug("Test chiffrage/dechiffrage")

    const { publicKey, privateKey } = ed25519.generateKeyPair()
    const publicKeyBytes = publicKey.publicKeyBytes,
          privateKeyBytes = privateKey.privateKeyBytes

    const message = new TextEncoder().encode("Un message secret")
    console.debug("Message bytes : %O", message)

    const contenuChiffre = await chiffrer(message, {clePubliqueEd25519: publicKeyBytes})
    console.debug("Contenu chiffre : %O", contenuChiffre)
    const { secretChiffre } = contenuChiffre

    // Trouver cle secrete
    const cleSecretRederivee = await utiljsEd25519.dechiffrerCle(secretChiffre, privateKeyBytes)
    console.debug("Cle secrete rederivee : %O", cleSecretRederivee)

    expect.assertions(1)
    expect(contenuChiffre.secretKey).toEqual(cleSecretRederivee)
})

test('chiffrage/dechiffrage contenu cipher incremental', async () => {
    console.debug("Test chiffrage/dechiffrage")

    const { publicKey, privateKey } = ed25519.generateKeyPair()
    const publicKeyBytes = publicKey.publicKeyBytes

    const message = new TextEncoder().encode("Un message secret")
    console.debug("Message bytes : %O", message)

    const { cipher, secretKey, iv } = await preparerCipher({clePubliqueEd25519: publicKeyBytes})
    const ciphertext = await cipher.update(message)
    const { tag } = await cipher.finalize()
    console.debug("Contenu chiffre : %O\niv: %O\ntag: %O\ncleSecrete: %O", ciphertext, iv, tag, secretKey)

    const decipher = await preparerDecipher(secretKey, iv)
    const messageDechiffre = new Uint8Array(await decipher.update(ciphertext))
    await decipher.finalize(tag)
    
    expect.assertions(1)
    expect(message).toEqual(messageDechiffre)
})

test('chiffrage/dechiffrage cipher incremental cle ed25519', async () => {
    console.debug("Test chiffrage/dechiffrage")

    const { publicKey, privateKey } = ed25519.generateKeyPair()
    const publicKeyBytes = publicKey.publicKeyBytes,
          privateKeyBytes = privateKey.privateKeyBytes

    const message = new TextEncoder().encode("Un message secret")
    console.debug("Message bytes : %O", message)

    const { secretKey, secretChiffre } = await preparerCipher({clePubliqueEd25519: publicKeyBytes})
    console.debug("Contenu chiffre : cleSecrete: %O", secretChiffre)

    // Trouver cle secrete
    const cleSecretRederivee = await utiljsEd25519.dechiffrerCle(secretChiffre, privateKeyBytes)
    console.debug("Cle secrete rederivee : %O", cleSecretRederivee)

    expect.assertions(1)
    expect(secretKey).toEqual(cleSecretRederivee)
})

test('preparer commande maitrecles', async () => {

    // Generer 2 certs pour simuler chiffrage avec cert maitredescles et cert millegrille
    const certMillegrille = await genererCert()
    const certMaitredescles = await genererCert()

    console.debug("Cert millegrille: %O", certMillegrille.pem)
    console.debug("Cert maitre des cles: %O", certMaitredescles.pem)

    const certificatsPem = [certMaitredescles.pem, certMillegrille.pem]
    const opts = {}

    const password = base64.encode(new Uint8Array(32)),
          iv = base64.encode(new Uint8Array(12)),
          tag = base64.encode(new Uint8Array(16)),
          hachage_bytes = base64.encode(new Uint8Array(64)),
          identificateurs_document = {'mondoc': 'doctest'},
          domaine = 'Test'

    const commande = await preparerCommandeMaitrecles(
        certificatsPem, password, domaine, hachage_bytes, iv, tag, identificateurs_document, opts)
    console.debug("Commande maitre des cles : %O", commande)

    expect.assertions(1)
    expect(Object.values(commande.cles).length).toBe(2)
})
