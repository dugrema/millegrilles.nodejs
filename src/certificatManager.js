import debugLib from 'debug'
import forge from 'node-forge'

const debug = debugLib('millegrilles:common:certificatmanager')
const { validateurMessage } = require('@dugrema/millegrilles.utiljs')

const MG_ROUTING_EMETTRE_CERTIFICAT = 'evenement.certificat.infoCertificat'

const verifierMessage = validateurMessage.verifierMessage

export default class GestionnaireCertificatMessages {
  // Gere les requetes et reponses de certificats

  constructor(pki, mq) {
    this.pki = pki
    this.mq = mq

    // Messages en attente de certificat
    // Cle: fingerprint
    // Valeur: [ {callback, creationDate}, ... ]
    this.attenteCertificat = {}

    this.intervalEntretien = setInterval(_=>{this.entretien()}, 30000)
  }

  entretien() {
    const attenteCertificatUpdate = {},
          tempsCourant = new Date().getTime()

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
    debug("Sauvegarder message certificat %s", fingerprint)
    // Note : la sauvegarde lance une erreur si la chaine est invalide
    await this.pki.sauvegarderMessageCertificat(messageContent, fingerprint)

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
      }
      if(niveauExchange >= 2) {
        this.mq.transmettreRequete(routingKey, requete, {nowait: true, exchange: '2.prive'})
      }
      this.mq.transmettreRequete(routingKey, requete, {nowait: true, exchange: '1.public'})

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

  demanderCertificatMaitreDesCles() {
    const tempsCourant = new Date().getTime();
    if(this.certificatMaitreDesCles && this.certificatMaitreDesCles.expiration < tempsCourant) {
      return new Promise((resolve, reject) => {
        resolve(this.certificatMaitreDesCles.cert);
      });
    } else {
      let objet_crypto = this;
      // console.debug("Demander certificat MaitreDesCles");
      var requete = {}
      var routingKey = 'MaitreDesCles.certMaitreDesCles';
      return this.mq.transmettreRequete(routingKey, requete)
      .then(reponse=>{
        let messageContent = decodeURIComponent(escape(reponse.content));
        let json_message = JSON.parse(messageContent);
        // console.debug("Reponse cert maitre des cles");
        // console.debug(messageContent);
        const cert = forge.pki.certificateFromPem(json_message.certificat);
        objet_crypto.certificatMaitreDesCles = {
          expiration: tempsCourant + 120000,
          cert,
        }

        return cert;
      })
    }
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
      this.mq.emettreEvenement(messageCertificat, 'certificat', {action: 'infoCertificat', attacherCertificat: true})
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

