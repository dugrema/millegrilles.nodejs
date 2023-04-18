const debug = require('debug')('millegrilles:common:certificatmanager')
// import { pki } from '@dugrema/node-forge'
const { hacherCertificat } = require('./hachage')
const forgecommon = require('@dugrema/millegrilles.utiljs/src/forgecommon')
const { MESSAGE_KINDS } = require('@dugrema/millegrilles.utiljs/src/constantes')

const { pki } = require('@dugrema/node-forge')

// const debug = debugLib('millegrilles:common:certificatmanager')

const MG_ROUTING_EMETTRE_CERTIFICAT = 'evenement.certificat.infoCertificat'

// const verifierMessage = validateurMessage.verifierMessage

class GestionnaireCertificatMessages {
  // Gere les requetes et reponses de certificats

  constructor(pki, mq) {
    this.pki = pki
    this.mq = mq

    // Messages en attente de certificat
    // Cle: fingerprint
    // Valeur: [ {callback, creationDate}, ... ]
    this.attenteCertificat = {}

    this.intervalEntretien = setInterval(_=>{this.entretien()}, 30000)

    this.cacheMaitredescles = {}
  }

  entretien() {
    this.cleanupAttenteCerticats()
    this.cleanupCertificatsMaitredescles()
  }

  cleanupCertificatsMaitredescles() {
    const expire = new Date().getTime() - 10 * 60_000;
    for(const fingerprint of Object.keys(this.cacheMaitredescles)) {
      const value = this.cacheMaitredescles[fingerprint]
      const timeActivite = value.dateActivite.getTime()
      if(timeActivite < expire) {
        debug("cleanupCertificatsMaitredescles Expirer certificat maitre des cles %s", fingerprint)
        delete this.cacheMaitredescles[fingerprint]
      } else {
        debug("cleanupCertificatsMaitredescles Certificat maitre des cle %s OK", fingerprint)
      }
    }
  }

  cleanupAttenteCerticats() {
    const attenteCertificatUpdate = {},
          tempsCourant = new Date().getTime()

          // Cleanup attente certificats
    for(let fingerprint in this.attenteCertificat) {
      var listeCertificats = this.attenteCertificat[fingerprint]

      const expires = []
      listeCertificats = listeCertificats.filter(item=>{
        // Verifier si la requete a plus de 15 secondes
        const valide = item.creationDate.getTime() + 15000 < tempsCourant
        if(!valide) {
          expires.push(item)
        }
        return valide
      })

      if(listeCertificats.length > 0) {
        attenteCertificatUpdate[fingerprint] = listeCertificats
      }
      debug("Attente certificat %s", fingerprint)

      if(expires.length > 0) {
        // Requete expirees
        expires.forEach(item=>{
          if(item.callback) {
            try{
              item.callback(item.erreurExpiration)
            } catch(err) {
              debug("certificatManager.entretien callback erreur: %O", err)
            }
          }
        })
      }
    }
    this.attenteCertificat = attenteCertificatUpdate
  }

  async sauvegarderMessageCertificat(messageContent, fingerprint) {
    debug("Sauvegarder message certificat %s\n%O", fingerprint, messageContent)
    // Note : la sauvegarde lance une erreur si la chaine est invalide
    const message = {...messageContent}
    if(message.chaine_pem) message['certificat'] = undefined // Retirer _certificat pour lire chaine_pem
    await this.pki.sauvegarderMessageCertificat(message, fingerprint)

    // Charger le certificat recu est valide (aucune erreur lancee)
    // On le charge a partir de pki avec instruction nowait=true (pour eviter recursion infinie)
    const chaineForge = await this.pki.getCertificate(fingerprint, {nowait: true})

    debug("Liste attentes certificats : %O", this.attenteCertificat)

    const callbacks = this.attenteCertificat[fingerprint]
    if(callbacks) {
      debug("Recu certificat %s, application callbacks", fingerprint)
      callbacks.forEach(item=>{
        debug("Certificat recu, callback info : %O", item)
        try {
          // Parametres du callback : (err, certificatForge)
          item.callback(null, chaineForge)
        } catch(err) {
          console.error("sauvegarderMessageCertificat: Erreur traitement callback sur reception certificat %s : %O", fingerprint, err)
        }
      })
      delete this.attenteCertificat[fingerprint]
    }

  }

  async recevoirCertificatMaitredescles(messageDict) {
    debug("Sauvegarder message certificat maitre des cles %s", messageDict)
    // Valider le certificat
    try {
      const pem = messageDict['certificat']
      const { valide, certificat } = await this.pki.verifierMessage(messageDict)

      if(valide) {
        const fingerprint = await hacherCertificat(certificat)
        debug("Certificat %s valide? %O, cert forge %O", fingerprint, valide, certificat)
        const extensions = forgecommon.extraireExtensionsMillegrille(certificat)
        debug("Extensions certificat : ", extensions)
        const roles = extensions.roles || []
        if(roles.includes('maitredescles')) {
          debug("Certificat maitre des cles confirme, on sauvegarde dans le cache")
          let entree = this.cacheMaitredescles[fingerprint]
          if(!entree) {
            entree = {fingerprint, pem, certificat, extensions}
            this.cacheMaitredescles[fingerprint] = entree
          }
          entree.dateActivite = new Date()  // Met a jour derniere date activite
        }
      }
    } catch(err) {
      console.error("recevoirCertificatMaitredescles Erreur reception certificat maitredescles : %O", err)
    }

  }

  demanderCertificat(fingerprint, opts) {
    opts = opts || {}

    if(fingerprint.indexOf(":") > -1) {
      fingerprint = fingerprint.split(':')[1]
    }

    // Eviter avalanche de requetes - admettre une seule demande / certificat a la fois
    var listeCallbacks = this.attenteCertificat[fingerprint]
    if( ! listeCallbacks ) {
      listeCallbacks = []
      this.attenteCertificat[fingerprint] = listeCallbacks

      // Transmettre requete
      var requete = {fingerprint}
      var routingKey = 'certificat.' + fingerprint

      const niveauExchange = this.mq.exchange.split('.').shift()
      debug("Demander certificat %s exchange %s et +", routingKey, niveauExchange)
      if(niveauExchange >= 3) {
        this.mq.transmettreRequete(routingKey, requete, {nowait: true, exchange: '3.protege'})
          .catch(err=>console.error("ERROR demanderCertificat 3.protege : ", err))
      }
      if(niveauExchange >= 2) {
        this.mq.transmettreRequete(routingKey, requete, {nowait: true, exchange: '2.prive'})
          .catch(err=>console.error("ERROR demanderCertificat 2.prive : ", err))
      }
      this.mq.transmettreRequete(routingKey, requete, {nowait: true, exchange: '1.public'})
        .catch(err=>console.error("ERROR demanderCertificat 1.public : ", err))

    } else {
      throw new Error("Requete certificat %s deja en cours", fingerprint)
    }

    if(opts.callback) {
      debug("Ajout callback pour fingerprint %s", fingerprint)
      const erreurExpiration = new Error(`Requete sur certificat ${fingerprint} expiree`)
      listeCallbacks.push({ callback: opts.callback, creationDate: new Date(), erreurExpiration })
    } else {
      // Pas de callback - on ajoute quand meme un element pour bloquer
      // requetes subsequentes (eviter une avalanche)
      listeCallbacks.push({creationDate: new Date()})
    }

  }

  async demanderCertificatMaitreDesCles() {
    // Verifier si on a au moins un certificat present dans le cache
    const fingerprintsActifs = Object.keys(this.cacheMaitredescles)
    if(fingerprintsActifs.length === 0) {
      debug("demanderCertificatMaitreDesCles Aucun certificats MaitreDesCles connus, on fait une requete")
      var routingKey = 'MaitreDesCles.certMaitreDesCles';
      const reponse = await this.mq.transmettreRequete(routingKey, {})
      debug("demanderCertificatMaitreDesCles Reponse ", reponse)
      await this.recevoirCertificatMaitredescles(reponse)
    }

    // Retourner la liste de certificats connus
    return Object.values(this.cacheMaitredescles).map(item=>item.pem)
  }

  transmettreCertificat(properties) {
    debug("Repondre avec le certificat local (props: %O)", properties)
    let messageCertificat = this.pki.preparerMessageCertificat()
    let fingerprint = messageCertificat.fingerprint

    if(properties && properties.correlationId && properties.replyTo) {
      const correlationId = properties.correlationId,
            replyTo = properties.replyTo

      // On repond avec le certificat
      debug("Transmettre certificat %s a %s (%s)", fingerprint, replyTo, correlationId)
      this.mq.transmettreReponse(messageCertificat, properties.replyTo, properties.correlationId)
    } else {
      // Il n'y a pas de demandeur specifique, on emet le certificat
      // let messageJSONStr = JSON.stringify(messageCertificat)
      //this.mq._publish(MG_ROUTING_EMETTRE_CERTIFICAT, messageJSONStr)
      this.mq.emettreEvenement(messageCertificat, 
        {domaine: 'certificat', action: 'infoCertificat', attacherCertificat: true}
      )
        .then(()=>{
          debug("Certificat emis OK")
        })
        .catch(e=>{
          console.error("%O Erreur emission certificat : %O", new Date(), e)
        })
    }

    return fingerprint
  }
}

module.exports = GestionnaireCertificatMessages
