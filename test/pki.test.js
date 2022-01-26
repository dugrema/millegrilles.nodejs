require('../src/chiffrage.ciphers')

const { MilleGrillesPKI } = require('../src/pki')
const { 
    chiffrer, dechiffrer, ed25519: utiljsEd25519, preparerCipher, preparerDecipher,
    preparerCommandeMaitrecles,
    genererClePrivee, genererCertificatMilleGrille, genererCsrNavigateur, genererCertificatIntermediaire
} = require('@dugrema/millegrilles.utiljs')

async function genererCert(ca) {
    const clePrivee = genererClePrivee()
    console.debug("Cle privee pem : %O", clePrivee)
    if(ca) {
        const csr = await genererCsrNavigateur('dummy', clePrivee.pem)
        console.debug("CA : %O", ca)
        const certPem = await genererCertificatIntermediaire(csr, ca.pem, ca.clePrivee.privateKey)
        return {pem: certPem, clePrivee}
    } else {
        const certInfo = await genererCertificatMilleGrille(clePrivee.pem)
        console.debug("certInfo: %O", certInfo)
        return {...certInfo, clePrivee}
    }
}

test('creerCipher', async () => {

    // Generer 2 certs pour simuler chiffrage avec cert maitredescles et cert millegrille
    const certMillegrille = await genererCert()
    const certLocal = await genererCert(certMillegrille)
    // const certMaitredescles = await genererCert(certMillegrille)

    console.debug("Cert Millegrille genere : %O", certMillegrille)
    console.debug("Cert local genere : %O", certLocal)
    
    const configCert = {
        millegrille: certMillegrille.pem,
        cert: certLocal.pem,
        key: certLocal.clePrivee.pem
    }

    const pki = new MilleGrillesPKI()
    await pki.initialiserPkiPEMS(configCert)

    const message = new Uint8Array(20)  // 20 bytes, tous 0x0
    try {
        // Chiffrer
        const cipher = await pki.creerCipherChiffrageAsymmetrique([certLocal.pem], 'TestDomaine', {cleDoc: 'valeurDummy'})
        console.debug("Cipher : %O", cipher)
        const ciphertext = await cipher.update(message)
        const outputCipher = await cipher.finalize()

        console.debug("Ciphertext : %O\nOutput: %O", ciphertext, outputCipher)

        // Dechiffrer
        const informationDechiffrage = {
            iv: outputCipher.iv,
            cles: outputCipher.cles
        }
        const decipher = await pki.creerDecipherChiffrageAsymmetrique(informationDechiffrage)
        console.debug("Decipher : %O", decipher)

        const messageDechiffre = new Uint8Array(await decipher.update(ciphertext))
        expect.assertions(1)
        expect(messageDechiffre).toEqual(message)

        await decipher.finalize(outputCipher.tag)

    } finally {
        // Cleanup
        pki.fermer()
    }
})
