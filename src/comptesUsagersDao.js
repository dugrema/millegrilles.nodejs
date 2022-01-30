const debug = require('debug')('millegrilles:common:dao:comptesUsagersDao')

const { extraireInformationCertificat } = require('@dugrema/millegrilles.utiljs/src/forgecommon')

// const debug = debugLib('millegrilles:common:dao:comptesUsagersDao')
//const { extraireInformationCertificat } = forgecommon

class ComptesUsagers {

  constructor(amqDao) {
    this.amqDao = amqDao
    this.idmg = amqDao.pki.idmg
    this.proprietairePresent = false
  }

  infoMillegrille = async () => {
    return {
      idmg: this.idmg,
    }
  }

  chargerCompte = async (nomUsager, fingerprintPk) => {
    if( ! nomUsager ) throw new Error("Usager undefined")

    const domaine = 'CoreMaitreDesComptes'
    const action = 'chargerUsager'

    const requete = {nomUsager}
    debug("Requete compte usager %s", nomUsager)

    const promiseCompteUsager = this.amqDao.transmettreRequete(
      domaine, requete, {action, decoder: true, attacherCertificat: true})
      .then(compteUsager=>{

        if(compteUsager.nomUsager) {
          debug("Requete compte usager, recu %s : %s", nomUsager, compteUsager)
          return compteUsager
        } else {
          debug("Requete compte usager, compte %s inexistant", nomUsager)
          return false
        }

      })

    var promiseFingerprintPk = null
    if(fingerprintPk) {
      const domaine  = 'CorePki'
      const action = 'certificatParPk'
      const requete = {fingerprint_pk: fingerprintPk}
      promiseFingerprintPk = this.amqDao.transmettreRequete(
        domaine, requete, {action, decoder: true, splitDomaineAction: true})
        .then(resultat=>{
          debug("Resultat requete fingerprintPk %s : %O", fingerprintPk, resultat)
          let certificat = resultat.certificat || resultat.chaine_pem
          if(certificat) return certificat
          else return false
        })
    }

    const resultats = await Promise.all([promiseCompteUsager, promiseFingerprintPk])

    const valeurs = resultats[0]
    if(valeurs) {
      if(resultats[1]) {
        valeurs.certificat = resultats[1]
      } else if(fingerprintPk) {
        // On n'a pas de certificat correspondant. On doit generer un challenge
        // cote serveur pour confirmer la demande de signature.
      }
    }
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

  ajouterCle = async (nomUsager, cle, reponseClient, opts) => {
    opts = opts || {}
    const domaine = 'CoreMaitreDesComptes'
    const action = 'ajouterCle'
    const transaction = {nomUsager, cle, reponseClient, ...opts}
    if(opts.resetCles) {
      transaction['reset_cles'] = true
    }
    debug("Transaction ajouter cle U2F pour %s", nomUsager)
    await this.amqDao.transmettreCommande(domaine, transaction, {action})
    debug("Transaction ajouter cle U2F pour %s completee", nomUsager)
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

  relayerTransaction = async (transaction) => {
    debug("relayerTransaction : %O", transaction)
    const confirmation = await this.amqDao.transmettreEnveloppeTransaction(transaction)
    debug("Confirmation relayer transactions : %O", confirmation)
    return confirmation
  }

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

  emettreCertificatNavigateur = async (fullchainPems) => {
    // Verifier les certificats et la signature du message
    // Permet de confirmer que le client est bien en possession d'une cle valide pour l'IDMG
    // const { cert: certNavigateur, idmg } = validerChaineCertificats(fullchain)
    const infoCertificat = extraireInformationCertificat(fullchainPems[0])
    debug("Information certificat navigateur : %O", infoCertificat)
    let messageInfoCertificat = {
        fingerprint: infoCertificat.fingerprintBase64,
        fingerprint_sha256_b64: infoCertificat.fingerprintSha256Base64,
        chaine_pem: fullchainPems,
    }
    const domaineAction = 'evenement.certificat.infoCertificat'
    try {
      debug("Emettre certificat navigateur fingerprint: %s", infoCertificat.fingerprintBase64)
      await this.amqDao.emettreEvenement(messageInfoCertificat, domaineAction)
    } catch(err) {
      debug("Erreur emission certificat\n%O", err)
    }
  }

  activerDelegationParCleMillegrille = async (userId, demandeSignee) => {
    const domaine = 'CoreMaitreDesComptes'
    const action = 'ajouterDelegationSignee'
    const transaction = {
      confirmation: demandeSignee,
      userId,  // Ajouter le userid, n'est pas present dans la demande signee initiale
    }
    debug("Transaction ajouterDelegationSignee %O", transaction)
    const reponse = await this.amqDao.transmettreCommande(domaine, transaction, {action})
    debug("Transaction ajouterDelegationSignee %s completee", userId)
    return reponse
  }

}

// Fonction qui injecte l'acces aux comptes usagers dans req
function injecter(amqDao) {
  const comptesUsagers = new ComptesUsagers(amqDao)

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

module.exports = injecter
