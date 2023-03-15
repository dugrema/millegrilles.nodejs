const fs = require('fs')
const _ciphers = require('../src/chiffrage.ciphers')  // Charger ciphers, hachage
const {pki: forgePki} = require('@dugrema/node-forge')

const EmetteurNotifications = require('../src/notifications')

const caPem = fs.readFileSync('/var/opt/millegrilles/configuration/pki.millegrille.cert')
const maitredesclesKeyPem = fs.readFileSync('/var/opt/millegrilles/secrets/pki.maitredescles.cle')
const maitredesclesCertPem = fs.readFileSync('/var/opt/millegrilles/secrets/pki.maitredescles.cert')

// Stub mq/pki
const mq = {
    pki: {
        formatterMessage: (message) => {
            return {...message, '_signature': 'DUMMY', 'en-tete': {hachage: 'DUMMY'}}
        }
    },
    getCertificatsMaitredescles: () => {
        return [
            {pem: [caPem]},
            {pem: [maitredesclesCertPem]}
        ]
    },
    transmettreCommande: () => {
        return {ok: true}
    }
}

test('notifications/preparer', async () => {
    console.debug("Test notifications/preparer")

    // Charger CA
    const certCA = forgePki.certificateFromPem(caPem)
    console.debug("Cert CA ", certCA)

    const emetteur = new EmetteurNotifications(certCA, 'dummy source')
    await emetteur.ready
    console.debug("Ready")
})

test('notifications/nouveau', async () => {
    console.debug("notifications/nouveau")

    // Charger CA
    const caPem = fs.readFileSync('/var/opt/millegrilles/configuration/pki.millegrille.cert')
    const certCA = forgePki.certificateFromPem(caPem)
    console.debug("Cert CA ", certCA)

    const emetteur = new EmetteurNotifications(certCA, 'dummy source')
    await emetteur.ready
    console.debug("Ready")

    const contenu = '<p>Dummy contenu</p>',
          subject = 'Dummy subject',
          niveau = 'info',
          destinataires = null

    await emetteur.emettreNotification(mq, contenu, subject, niveau, destinataires)
})
