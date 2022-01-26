require('../src/chiffrage.ciphers')

const { MilleGrillesPKI } = require('../src/pki')
const { 
    chiffrer, dechiffrer, ed25519: utiljsEd25519, preparerCipher, preparerDecipher,
    preparerCommandeMaitrecles,
    genererClePrivee, genererCertificatMilleGrille
} = require('@dugrema/millegrilles.utiljs')

async function genererCert() {
    const clePrivee = genererClePrivee()
    console.debug("Cle privee pem : %O", clePrivee)
    const certInfo = await genererCertificatMilleGrille(clePrivee.pem)
    console.debug("certInfo: %O", certInfo)
    return {...certInfo, clePrivee}
}

test('creerCipher', async () => {

    // Generer 2 certs pour simuler chiffrage avec cert maitredescles et cert millegrille
    const certMillegrille = await genererCert()
    const certMaitredescles = await genererCert()

    console.debug("Cert Millegrille genere : %O", certMillegrille)
    console.debug("Cert maitre des cles genere : %O", certMaitredescles)
    
    const configCert = {
        millegrille: certMillegrille.pem,
        cert: certMaitredescles.pem,
        key: certMaitredescles.clePrivee.pem
    }

    //const pki = new MilleGrillesPKI()
//    await pki.initialiserPkiPEMS(configCert)



})