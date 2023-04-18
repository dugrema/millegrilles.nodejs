const debug = require('debug')('millegrilles:notifications')
const { deflate } = require('node:zlib')
const { MESSAGE_KINDS } = require('@dugrema/millegrilles.utiljs/src/constantes')
const chiffrageEd25519 = require('@dugrema/millegrilles.utiljs/src/chiffrage.ed25519')
const chiffrage = require('../src/chiffrage')
const { base64 } = require('multiformats/bases/base64')

class EmetteurNotifications {

    /**
     * 
     * @param {*} enveloppe_ca Certificat CA format Forge
     * @param {*} champ_from Valeur a mettre dans le champ _from_ de la notification
     */
    constructor(certCa, champ_from) {
        if(!certCa.publicKey) throw new Error('certCa doit etre de format certificat node-forge')
        if(champ_from && !typeof(champ_from) === 'string') throw new Error('champ_from doit etre de format string')

        this.certCa = certCa
        this.from = champ_from

        this.cleSecrete = null
        this.peerPublic = null
        this.ref_hachage_bytes = null

        this.commandeCle = null
        this.cleTransmise = false

        // Generer une nouvelle cle secrete pour chiffrage de la notification
        this.ready = this.genererCle(this)
    }

    async genererCle() {
        const publicKeyBytes = this.certCa.publicKey.publicKeyBytes
        const {cle, peer, peerPublicBytes} = await chiffrageEd25519.genererCleSecrete(publicKeyBytes)
        debug("Cle %O, peer %O, peerPublicBytes %O", cle, peer, peerPublicBytes)
        this.cleSecrete = cle
        this.peerPublic = peer
        this.ready = true
    }

    // async chargerCertificatMaitredescles(mq) {
    //     const certificats = await mq.getCertificatsMaitredescles()
    //     // const certificatPem = reponse.certificat
    //     // return await mq.pki.validerCertificat(certificatPem)
    // }
    
    async preparerContenu(mq, contenu, subject) {
        const message = {'content': contenu}
        if(this.from) message.from = this.from
        if(subject) message.subject = subject

        debug("Contenu notification a preparer : %s" % message)

        const messageSigne = await mq.pki.formatterMessage(MESSAGE_KINDS.KIND_DOCUMENT, message, {domaine: 'message'})
        debug("Message signe : ", messageSigne)
        let messageBytes = JSON.stringify(messageSigne)
        messageBytes = await new Promise((resolve, reject)=>{
            deflate(new TextEncoder().encode(messageBytes), (err, buffer)=>{
                if(err) return reject(err)
                resolve(buffer)
            })
        })
        debug("Message bytes compresse ", messageBytes)

        // Chiffrer le contenu compresse
        const messageChiffre = await chiffrage.chiffrer(messageBytes, {key: this.cleSecrete})
        debug("Notification chiffree ", messageChiffre)
        const ref_hachage_bytes = messageChiffre.hachage,
              messageChiffreStr = base64.baseEncode(messageChiffre.ciphertext)
        debug("Message chiffre string : ", messageChiffreStr)

        if(!this.cleTransmise && !this.commandeCle) {
            debug("Generer commande maitre des cles")
            const certifcatsMaitredescles = await mq.getCertificatsMaitredescles()
            const certificatsPem = certifcatsMaitredescles.map(item=>item.pem[0])
            const identificateurs_document = {notification: 'true'}
            const commande = await chiffrage.preparerCommandeMaitrecles(
                certificatsPem, this.cleSecrete, 'Messagerie', ref_hachage_bytes, identificateurs_document, 
                {peer: this.peerPublic}
            )
            const commandeSignee = await mq.pki.formatterMessage(MESSAGE_KINDS.KIND_COMMANDE, commande, {domaine: 'MaitreDesCles', action: 'sauvegarderCle'})
            debug("Commande maitre des cles ", commandeSignee)

            this.commandeCle = commandeSignee
        }

        const reponse = {
            'chiffre': messageChiffreStr,
            'ref_hachage_bytes': this.commandeCle['hachage_bytes'],
            'header': messageChiffre.header,
            'format': messageChiffre.format,
        }

        return reponse
    }

    async emettreNotification(mq, contenu, subject, niveau, destinataires) {
        const notification = await this.preparerContenu(mq, contenu, subject)
        debug("emettreNotification Notification a emettre ", notification)

        const now = Math.round(new Date().getTime() / 1000),
              expiration = now + 7 * 86400
        
        const notificationFormattee = {
            expiration,
            message: {
                niveau,
                ref_hachage_bytes: notification.ref_hachage_bytes,
                format: notification.format,
                header: notification.header,
                message_chiffre: notification.chiffre,
                destinataires,
            }
        }
        if(!this.cleTransmise) notificationFormattee['_cle'] = this.commandeCle

        debug("Notification formattee : ", notificationFormattee)
        const reponse = await mq.transmettreCommande(
            'Messagerie', notificationFormattee, {action: 'notifier', exchange: '1.public'})
        if(reponse.ok === true) {
            this.cleTransmise = true
        }
    }
}

module.exports = EmetteurNotifications
