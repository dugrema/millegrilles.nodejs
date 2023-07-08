const debug = require('debug')('common:comptesUsagersDao')

const { extraireExtensionsMillegrille } = require('@dugrema/millegrilles.utiljs/src/forgecommon')
const { verifierChallenge } = require('./webauthn')

// const debug = debugLib('millegrilles:common:dao:comptesUsagersDao')
//const { extraireInformationCertificat } = forgecommon

const DOMAINE_MAITRECOMPTES = 'CoreMaitreDesComptes',
      L2Prive = '2.prive'

class ComptesUsagers {

  constructor(amqDao, opts) {
    opts = opts || {}
    const { hostname } = opts
    this.amqDao = amqDao
    this.idmg = amqDao.pki.idmg
    this.proprietairePresent = false

    this.hostname = hostname
    console.info("ComptesUsagers init hostname : ", this.hostname)
  }

  infoMillegrille = async () => {
    return {
      idmg: this.idmg,
    }
  }

  chargerCompte = async (nomUsager, fingerprintPublicNouveau, fingerprintPublicCourant, opts) => {
    opts = opts || {}
    if( ! nomUsager ) throw new Error("Usager undefined")

    const hostname = opts.hostname || this.hostname

    const domaine = 'CoreMaitreDesComptes'
    const action = 'chargerUsager'

    const requete = {nomUsager, hostUrl: hostname}
    debug("Requete compte usager %s (fingerprintPublicNouveau : %s, fingerprintPublicCourant: %s, hostname: %s)", 
      nomUsager, fingerprintPublicNouveau, fingerprintPublicCourant, hostname)

    const promiseCompteUsager = this.amqDao.transmettreRequete(
      domaine, requete, {action, decoder: true, attacherCertificat: true})
      .then(reponse=>{

        const compte = reponse.compte
        if(compte) {

          debug("Requete compte usager, recu %s : %s", compte.nomUsager, reponse)

          // if(fingerprintPublicCourant && compteUsager.activations_par_fingerprint_pk) {
          //   // Verifier si l'usager peut utiliser un nouveau certificat pour bypasser
          //   // l'authentification forte (e.g. account recovery)
          //   const etatAssociation = compteUsager.activations_par_fingerprint_pk[fingerprintPublicCourant] || {}
          //   if(etatAssociation.associe === false) {
          //     compteUsager.bypassActif = true
          //   }
          // }

          return reponse
        } else {
          debug("Requete compte usager, compte %s inexistant", nomUsager)
          return false
        }

      })

    var promiseFingerprintNouveau = null
    if(fingerprintPublicNouveau) {
      const domaine  = 'CorePki'
      const action = 'certificatParPk'
      const requete = {fingerprint_pk: fingerprintPublicNouveau}
      promiseFingerprintNouveau = this.amqDao.transmettreRequete(
        domaine, requete, {action, decoder: true, splitDomaineAction: true})
        .then(resultat=>{
          debug("Resultat requete fingerprintPk %s : %O", fingerprintPublicNouveau, resultat)
          let certificat = resultat.certificat || resultat.chaine_pem
          if(certificat) return certificat
          else return false
        })
    }

    const resultats = await Promise.all([promiseCompteUsager, promiseFingerprintNouveau])

    const valeurs = resultats[0]
    if(valeurs) {
      if(resultats[1]) {
        valeurs.certificat = resultats[1]
      } else if(fingerprintPublicCourant) {
        // On n'a pas de certificat correspondant. On doit generer un challenge
        // cote serveur pour confirmer la demande de signature.
      }
    }

    // Combiner webauthn par hostname si disponible
    // const webauthn_hostnames = valeurs.webauthn_hostnames
    // let webauthn = valeurs.webauthn || []
    // if(webauthn_hostnames && hostname) {
    //   const hostname_converti = hostname.replaceAll('.', '_')
    //   if(webauthn_hostnames[hostname_converti]) {
    //     debug("Remplacer webauthn (%O) par (%s: %O)", webauthn, hostname_converti, webauthn_hostnames[hostname_converti])
    //     webauthn = webauthn_hostnames[hostname_converti]
    //   }
    // }

    debug("Compte usager charge : %O", valeurs)
    return valeurs
  }

  chargerCompteUserId = async (userId) => {
    if( ! userId ) throw new Error("Usager undefined")

    const domaine = 'CoreMaitreDesComptes'
    const action = 'chargerUsager'
    const requete = {userId}
    debug("Requete compte usager %s", userId)

    const valeurs = await this.amqDao.transmettreRequete(
      domaine, requete, {action, decoder: true, attacherCertificat: true})
      .then(compteUsager=>{

        if(compteUsager.nomUsager) {
          debug("Requete compte usager, recu %s : %s", userId, compteUsager)
          return compteUsager
        } else {
          debug("Requete compte usager, compte %s inexistant", userId)
          return false
        }

      })
    debug("Compte usager charge : %O", valeurs)
    return valeurs
  }

  chargerCompteUsager = (socket, requete) => {
    // Utilise la signature de l'usager pour charger son compte
    if(!requete.sig) return {ok: false, err: 'Signature de message "chargerCompteUsager" absente'}

    const domaine = DOMAINE_MAITRECOMPTES
    const action = 'chargerUsager'

    return transmettreRequete(socket, requete, action, {domaine})
  }  

  inscrireCompte = async (nomUsager, userId, fingerprintPk, securite, csr) => {
    const domaine = 'CoreMaitreDesComptes'
    const action = 'inscrireUsager'
    // Conserver csr hors de la transaction
    const transaction = {nomUsager, userId, securite, fingerprint_pk: fingerprintPk, csr}
    debug("Transaction inscrire compte usager %s (userId: %s, securite: %s)", nomUsager, userId, securite)
    const reponse = await this.amqDao.transmettreCommande(domaine, transaction, {action})
    debug("Inscription compte usager %s completee", nomUsager)
    return reponse
  }

  changerCleComptePrive = async (nomUsager, nouvelleCle) => {
    const domaineAction = 'MaitreDesComptes.majCleUsagerPrive'
    const transaction = {nomUsager, cle: nouvelleCle}
    debug("Transaction changer mot de passe de %s", nomUsager)
    await this.amqDao.transmettreTransactionFormattee(transaction, domaineAction)
    debug("Transaction changer mot de passe de %s completee", nomUsager)
  }

  ajouterCle = async reponseClient => {
    // opts = opts || {}

    if( ! reponseClient.sig ) {
      throw new Error("La reponse client doit etre signee par le navigateur")
    }

    const domaine = 'CoreMaitreDesComptes'
    const action = 'ajouterCle'

    // Creer une commande comme enveloppe - confirme que la transaction a ete validee
    const commande = {transaction: reponseClient}
    debug("Commande ajouter cle U2F : %O", commande)

    return await this.amqDao.transmettreCommande(domaine, commande, {action})
  }

  supprimerCles = async (nomUsager) => {
    const domaineAction = 'MaitreDesComptes.supprimerCles'
    const transaction = {nomUsager}
    debug("Transaction supprimer cles U2F %s", nomUsager)
    await this.amqDao.transmettreTransactionFormattee(transaction, domaineAction)
    debug("Transaction supprimer cles U2F de %s completee", nomUsager)
  }

  resetWebauthn = async (userId) => {
    const domaineAction = 'MaitreDesComptes.supprimerCles'
    const transaction = {userId}
    debug("Transaction supprimer cles U2F %s", userId)
    await this.amqDao.transmettreTransactionFormattee(transaction, domaineAction)
    debug("Transaction supprimer cles U2F de %s completee", userId)
  }

  supprimerUsager = async (nomUsager) => {
    const domaineAction = 'MaitreDesComptes.supprimerUsager'
    const transaction = {nomUsager}
    debug("Transaction supprimer usager %s", nomUsager)
    await this.amqDao.transmettreTransactionFormattee(transaction, domaineAction)
    debug("Transaction supprimer usager %s completee", nomUsager)
  }

  ajouterCertificatNavigateur = async (nomUsager, params) => {
    const domaineAction = 'MaitreDesComptes.ajouterNavigateur'
    const transaction = {nomUsager, ...params}
    debug("Transaction ajouter certificat navigateur compte usager %s", nomUsager)
    await this.amqDao.transmettreTransactionFormattee(transaction, domaineAction)
    debug("Transaction ajouter certificat navigateur compte usager %s completee", nomUsager)
  }

  // relayerTransaction = async (transaction) => {
  //   debug("relayerTransaction : %O", transaction)
  //   const confirmation = await this.amqDao.transmettreEnveloppeTransaction(transaction)
  //   debug("Confirmation relayer transactions : %O", confirmation)
  //   return confirmation
  // }

  signerCertificatNavigateur = async (csr, nomUsager, userId, opts) => {
    opts = opts || {}
    // const domaineAction = 'commande.servicemonitor.signerNavigateur'
    const domaine = 'CoreMaitreDesComptes'
    const action = 'signerCompteUsager'
    const params = {csr, nomUsager, userId, ...opts}

    // const commande

    try {
      debug("Commande signature certificat navigateur %O", params)
      const reponse = await this.amqDao.transmettreCommande(domaine, params, {action, decoder: true})
      debug("Reponse commande signature certificat : %O", reponse)
      const resultats  = reponse.resultats || {}
      if(resultats.err) { return {err: ''+resultats.err, code: resultats.code} }

      return reponse
    } catch(err) {
      debug("Erreur signerCertificatNavigateur\n%O", err)
      return {err: ''+err, stack: err.stack}
    }
  }

  // emettreCertificatNavigateur = async (fullchainPems) => {
  //   // Verifier les certificats et la signature du message
  //   // Permet de confirmer que le client est bien en possession d'une cle valide pour l'IDMG
  //   // const { cert: certNavigateur, idmg } = validerChaineCertificats(fullchain)
  //   const infoCertificat = extraireInformationCertificat(fullchainPems[0])
  //   debug("Information certificat navigateur : %O", infoCertificat)
  //   let messageInfoCertificat = {
  //       fingerprint: infoCertificat.fingerprintBase64,
  //       fingerprint_sha256_b64: infoCertificat.fingerprintSha256Base64,
  //       chaine_pem: fullchainPems,
  //   }
  //   const domaineAction = 'evenement.certificat.infoCertificat'
  //   try {
  //     debug("Emettre certificat navigateur fingerprint: %s", infoCertificat.fingerprintBase64)
  //     await this.amqDao.emettreEvenement(messageInfoCertificat, domaineAction)
  //   } catch(err) {
  //     debug("Erreur emission certificat\n%O", err)
  //   }
  // }

  activerDelegationParCleMillegrille = async (_socket, params) => {
    const {userId, confirmation} = JSON.parse(params.contenu)
    const domaine = 'CoreMaitreDesComptes'
    const action = 'ajouterDelegationSignee'
    const transaction = {
      confirmation,
      userId,  // Ajouter le userid, n'est pas present dans la demande signee initiale
    }
    debug("Transaction ajouterDelegationSignee %O", transaction)
    const reponse = await this.amqDao.transmettreCommande(domaine, transaction, {action})
    debug("Transaction ajouterDelegationSignee %s completee", userId)
    return reponse
  }

  ajouterCsrRecovery = (socket, commande) => {
    const domaine = DOMAINE_MAITRECOMPTES,
          action = 'ajouterCsrRecovery',
          exchange = '2.prive'
  
    return socket.amqpdao.transmettreCommande(
      domaine, 
      commande, 
      {domaine, action, exchange, decoder: true}
    )
  }

  getRecoveryCsr = (socket, requete) => {
    // Utilise la signature de l'usager pour charger son compte
    if(!requete.sig) return {ok: false, err: 'Signature de message "getRecoveryCsr" absente'}
    const domaine = DOMAINE_MAITRECOMPTES
    const action = 'getCsrRecoveryParcode'
    return transmettreRequete(socket, requete, action, {domaine})
  }

  signerRecoveryCsr = async (socket, commande) => {
    // const session = socket.handshake.session
    // debug("!!! \nSOCKET\n%O", socket)
    debug("signerRecoveryCsr ", commande)
    const contenu = JSON.parse(commande.contenu)

    // Supporter session.nomUsager pour enregistrement usager maitrcomptes
    const session = socket.handshake.session || {}
    const nomUsager = socket.nomUsager || session.nomUsager

          // Utilise la signature de l'usager pour charger son compte
    if(!commande.sig) return {ok: false, err: 'Signature de message "signerRecoveryCsr" absente'}
    const domaine = DOMAINE_MAITRECOMPTES
    const action = 'signerCompteUsager'

    // Charger usager
    const webauthnChallenge = socket.webauthnChallenge,
          clientAssertionResponse = contenu.clientAssertionResponse,
          demandeCertificat = contenu.demandeCertificat,
          userId = contenu.userId

    if(webauthnChallenge) {
      // Signature webauthn par l'usager du compte. Verifier avant de passer sur MQ.

      const compteUsager = await socket.comptesUsagersDao.chargerCompte(nomUsager)
      debug("Compte usager charge : %O\nChallenge session : %O\nVerifier %O", compteUsager, webauthnChallenge, clientAssertionResponse)
  
      // Verification du challenge
      try {
        await verifierChallenge(webauthnChallenge, compteUsager, clientAssertionResponse, {demandeCertificat})
        debug("Transmettre commande recovery CSR\n%O", commande)
        return transmettreCommande(socket, commande, action, {domaine})
      } catch(err) {
        debug("signerRecoveryCsr Erreur verification: %O", err)
        return {ok: false, 'err': 'Erreur verification challenge webauthn'}
      }

    } else if(userId) {
      // On n'a pas de signature webauthn. Verifier la signature de la requete, doit etre une delegation globale
      const chaineForge = await socket.amqpdao.pki.getCertificatMessage(commande)
      const certificat = chaineForge[0]
      const infoCertificat = extraireExtensionsMillegrille(certificat)
      if(infoCertificat.delegationGlobale !== 'proprietaire') {
        return {ok: false, err: 'Acces refuse, doit etre delegation globale'}
      }

      // Ok, on transmet la commande
      return transmettreCommande(socket, commande, action, {domaine})
    } else {
      return {ok: false, err: 'Acces refuse, commande incomplete ou non autorisee'}
    }

  }
}

// Fonction qui injecte l'acces aux comptes usagers dans req
function injecter(amqDao, opts) {
  opts = opts || {}
  const comptesUsagers = new ComptesUsagers(amqDao, opts)

  const injecterComptesUsagers = async (req, res, next) => {
    debug("Injection req.comptesUsagers")
    req.comptesUsagers = comptesUsagers  // Injecte db de comptes
    next()
  }

  const extraireUsager = async (req, res, next) => {

    const nomUsager = req.nomUsager  // Doit avoir ete lu par sessions.js
    const estProprietaire = req.sessionUsager?req.sessionUsager.estProprietaire:false
    if(estProprietaire) {
      debug("Chargement compte proprietaire")
      const compte = await comptesUsagers.infoCompteProprietaire()
      if(compte) {
        req.compteUsager = compte
      }

    } else if(nomUsager) {
      debug('Nom usager %s', nomUsager)

      // Extraire compte usager s'il existe
      const compte = await comptesUsagers.chargerCompte(nomUsager)
      if(compte) {
        req.compteUsager = compte
      }
    }

    next()
  }

  return {injecterComptesUsagers, extraireUsager, comptesUsagersDao: comptesUsagers}
}

async function transmettreRequete(socket, params, action, opts) {
  opts = opts || {}
  const domaine = opts.domaine || DOMAINE_MAITRECOMPTES
  const exchange = opts.exchange || L2Prive
  try {
      verifierMessage(params, domaine, action)
      return await socket.amqpdao.transmettreRequete(
          domaine, 
          params, 
          {action, exchange, noformat: true, decoder: true}
      )
  } catch(err) {
      console.error("mqdao.transmettreRequete ERROR : %O", err)
      return {ok: false, err: ''+err}
  }
}

async function transmettreCommande(socket, params, action, opts) {
  opts = opts || {}
  const domaine = opts.domaine || DOMAINE_MAITRECOMPTES
  const exchange = opts.exchange || L2Prive
  try {
      verifierMessage(params, domaine, action)
      return await socket.amqpdao.transmettreCommande(
          domaine, 
          params, 
          {action, exchange, noformat: true, decoder: true}
      )
  } catch(err) {
      console.error("mqdao.transmettreCommande ERROR : %O", err)
      return {ok: false, err: ''+err}
  }

}

/* Fonction de verification pour eviter abus de l'API */
function verifierMessage(message, domaine, action) {
  const routage = message.routage || {},
        domaineRecu = routage.domaine,
        actionRecue = routage.action
  
  // const entete = message['en-tete'] || {},
  //       domaineRecu = entete.domaine,
  //       actionRecue = entete.action
  if(domaineRecu !== domaine) throw new Error(`Mismatch domaine (${domaineRecu} !== ${domaine})"`)
  if(actionRecue !== action) throw new Error(`Mismatch action (${actionRecue} !== ${action})"`)
}

module.exports = injecter
