// Sanity test de chiffrage.ciphers.js
import '../src/chiffrage.ciphers'
import { chiffrer, dechiffrer, ed25519 as utiljsEd25519, preparerCipher, preparerDecipher } from '@dugrema/millegrilles.utiljs'
import nodeforge from '@dugrema/node-forge'

const { ed25519 } = nodeforge

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
