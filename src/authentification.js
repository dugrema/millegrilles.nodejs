/*
Module d'authentification web
*/
const debug = require('debug')('millegrilles:authentification')

const { randomBytes } = require('crypto')
const { validerChaineCertificats, extraireExtensionsMillegrille } = require('@dugrema/millegrilles.utiljs/src/forgecommon')
const { verifierSignatureMessage } = require('@dugrema/millegrilles.utiljs/src/validateurMessage')

const CONST_CHALLENGE_CERTIFICAT = 'challengeCertificat',
      CONST_AUTH_PRIMAIRE = 'authentificationPrimaire',
      CONST_AUTH_SECONDAIRE = 'authentificationSecondaire',
      CONST_WEBAUTHN_CHALLENGE = 'webauthnChallenge'

async function verifierUsager(socket, params) {
  /*
  Verifier l'existence d'un usager par methode http.
  Retourne des methodes d'authentification lorsque l'usager existe.
  Genere les challenges en session.

  Requires :
    - req.body
    - req.session
    - req.comptesUsagers
  */

  debug("common.authentification.verifierUsager PARAMS : %O", params)
  const session = socket.handshake.session,
        hostnameRequest = socket.handshake.headers.host

  if(!params.nomUsager) throw new Error("verifierUsager Params sans nomUsager")

  const nomUsager = params.nomUsager,
        fingerprintPk = params.fingerprintPk,
        fingerprintCourant = params.fingerprintCourant,
        // hostname = params.hostname,
        genererChallenge = params.genererChallenge || false

  debug("Verification d'existence d'un usager : %s\nBody: %O", nomUsager, params)

  if( ! nomUsager ) {
    console.error(new Date() + " verifierUsager: Requete sans nom d'usager")
    return {err: 'Requete sans nom d\'usager'}
  }
  
  const optsChargerCompte = {}
  if(genererChallenge) optsChargerCompte.hostname = hostnameRequest  // Agit comme flag pour generer un challenge dans la DB
  const infoUsager = await socket.comptesUsagersDao.chargerCompte(nomUsager, fingerprintPk, null, optsChargerCompte)
  const compteUsager = infoUsager.compte

  debug("Compte usager recu : %O", infoUsager)

  if(compteUsager) {
    // Usager connu
    // const {methodesDisponibles} = await auditMethodes(socket.handshake, params, {compteUsager})
    debug("Usager %s connu, transmission challenge login", nomUsager)

    const reponse = {
      // Filtrer methodes webauthn, vont etre restructurees en un challenge
      // methodesDisponibles: Object.keys(methodesDisponibles).filter(item=>!item.startsWith('webauthn.')),
    }

    session.userId = compteUsager.userId

    if(socket.modeProtege === true) {
      // La session est ouverte et verifiee - on retourne info de certificat
      reponse.updates = {
        delegations_date: compteUsager.delegations_date,
        delegations_version: compteUsager.delegations_version,
      }
    }

    // Tranferer information client vers reponse
    reponse.registration_challenge = infoUsager.registration_challenge

    if(genererChallenge) {
      // Conserver passkey_authentication pour verifier localement avant d'emettre vers back-end
      session.passkey_authentication = infoUsager.passkey_authentication
      reponse.authentication_challenge = infoUsager.authentication_challenge
    }

    // Attacher le nouveau certificat de l'usager si disponible
    if(compteUsager.certificat) {
      reponse.certificat = compteUsager.certificat
    }

    // Trouver activation. Privilegier activation du nouveau certificat (fingerprintPk)
    // Fallback sur certificat courant (fingerprintCourant)
    const activations = infoUsager.activations || {}
    let activation = activations[fingerprintPk]
    if(!activation) {
      activation = activations[fingerprintCourant]
    }
    if(activation) {
      // Filtrer methodes d'activation
      reponse.activation = {...activation, fingerprint: activation.fingerprint_pk, valide: true}
      if(reponse.activation.certificat) {
        // Extraire le certificat vers top du compte
        reponse.certificat = reponse.activation.certificat
        delete reponse.activation.certificat
      }
      reponse.methodesDisponibles = {certificat: true}
    } else if(socket.modeProtege === true) {
      reponse.methodesDisponibles = {certificat: true}
    }

    return reponse
  } else {
    // Usager inconnu
    debug("Usager inconnu")
    return {compteUsager: false}
  }
}

async function genererChallengeCertificat(socket) {
  debug("genererChallengeCertificat: Preparation challenge")

  // Generer challenge pour le certificat de navigateur ou cle de millegrille
  // Ces methodes sont toujours disponibles
  try {
    var challengeCertificat = socket[CONST_CHALLENGE_CERTIFICAT]
    if(!challengeCertificat) {
      challengeCertificat = {
        date: new Date().getTime(),
        data: Buffer.from(randomBytes(32)).toString('base64'),
      }
      socket[CONST_CHALLENGE_CERTIFICAT] = challengeCertificat
    }
    const reponse = {challengeCertificat}

    return reponse
  } catch(err) {
    console.error(new Date() + " genererChallengeCertificat Erreur ", err)
    return {ok: false, err: ''+err}
  }
}

// function auditMethodesDisponibles(compteUsager, opts) {
//   opts = opts || {}

//   // Creer une liste de methodes disponibles et utilisees
//   // Comparer pour savoir si on a une combinaison valide

//   const methodesDisponibles = {
//     certificat: false
//   }

//   // Override methode certificat au besoin
//   if( opts.socket && opts.socket.session && opts.socket.session[CONST_AUTH_PRIMAIRE] ) {
//     // Certificat est toujours disponible pour l'upgrade socket
//     methodesDisponibles.certificat = true
//   // } else if(compteUsager.activations_par_fingerprint_pk) {
//   } else if(compteUsager.activations) {
//     // Certificat est disponible pour un appareil qui vient d'etre active
//     var auMoins1actif = false
//     // for(let fp in compteUsager.activations_par_fingerprint_pk) {
//     //   const info = compteUsager.activations_par_fingerprint_pk[fp]
//     for(let fp in compteUsager.activations) {
//       const info = compteUsager.activations[fp]
//       auMoins1actif |= !info.associe
//     }
//     if(auMoins1actif) {
//       methodesDisponibles.certificat = true
//     }
//   }

//   // Methodes disponibles
//   if(compteUsager.totp) methodesDisponibles.totp = true
//   if(compteUsager.motdepasse) methodesDisponibles.motdepasse = true
//   if(compteUsager.webauthn) {
//     // Mettre chaque methode comme cle - permet de facilement retirer la/les
//     // creds deja utilisees pour demander une 2e verification
//     compteUsager.webauthn.forEach(item=>{
//       methodesDisponibles['webauthn.' + item.credId] = true
//     })
//   }
//   if(compteUsager['est_proprietaire']) {
//     // Pour le compte proprietaire, on permet d'utiliser la cle de millegrille
//     methodesDisponibles.cleMillegrille = true
//   }

//   return methodesDisponibles
// }

// function auditMethodesUtilisees(session, params, opts) {
//   opts = opts || {}
//   const socket = opts.socket || {}

//   // Verifier methode d'authentification - refuser si meme que la methode primaire
//   const methodePrimaire = session[CONST_AUTH_PRIMAIRE],
//         methodeSecondaire = session[CONST_AUTH_SECONDAIRE],
//         challengeSession = session[CONST_CHALLENGE_CERTIFICAT]

//   var challengeWebauthn = socket[CONST_WEBAUTHN_CHALLENGE] || session[CONST_WEBAUTHN_CHALLENGE]
//   if(challengeWebauthn && challengeWebauthn.challenge) {
//     // Ramener le challenge d'un niveau
//     challengeWebauthn = challengeWebauthn.challenge
//   }

//   const methodesUtilisees = {}

//   // Indiquer les methodes primaires et secondaires utilisees, considerer deja verifiees
//   if(methodePrimaire) {
//     methodesUtilisees[methodePrimaire] = {verifie: true}
//     if(methodeSecondaire) {
//       methodesUtilisees[methodeSecondaire] = {verifie: true}
//     }
//   }

//   if(params.cleMillegrille) {
//     methodesUtilisees.cleMillegrille = {
//       valeur: params.cleMillegrille,
//       challengeSession,
//       verifie: false
//     }
//   }
//   if(params.motdepasse) {
//     methodesUtilisees.motdepasse = {
//       valeur: params.motdepasse,
//       verifie: false
//     }
//   }
//   if(params.tokenTotp) {
//     methodesUtilisees.totp = {
//       valeur: params.tokenTotp,
//       verifie: false
//     }
//   }
//   if(params.date && params.data && params._certificat && params._signature) {
//     methodesUtilisees.certificat = {
//       valeur: params, challengeSession, certificat: params._certificat,
//       verifie: false,
//     }
//   }
//   if(params.webauthn) {
//     methodesUtilisees['webauthn.' + params.webauthn.id64] = {
//       valeur: params.webauthn,
//       challenge: challengeWebauthn,
//       verifie: false,
//     }
//   }

//   return methodesUtilisees
// }

// async function auditMethodes(req, params, opts) {
//   opts = opts || {}
//   debug("Audit methodes d'authentification, params : %O", params)

//   /* Audit des methodes d'authentifications utilisees et disponibles pour l'usager */
//   opts = opts || {}
//   const socket = opts.socket || {},
//         session = opts.session || req.session || socket.session,
//         nomUsager = session.nomUsager || params.nomUsager

//   debug("auditMethodes usager %s session : %O", nomUsager, session)

//   var compteUsager = opts.compteUsager
//   if(!compteUsager) {
//     const comptesUsagersDao = socket.comptesUsagersDao || req.comptesUsagersDao
//     compteUsager = await comptesUsagersDao.chargerCompte(nomUsager)
//   }
//   debug("Audit methodes authentification pour compteUsager : %O", compteUsager)

//   // const methodesUtilisees = auditMethodesUtilisees(session, params, {socket})
//   const methodesDisponibles = auditMethodesDisponibles(compteUsager, {socket})

//   debug("Methodes disponibles pour authentification\n%O", methodesDisponibles)

//   // Retrirer la methode primaire des methodes disponibles
//   var nombreVerifiees = 0
//   Object.keys(methodesUtilisees).forEach(item=>{
//     if(methodesDisponibles[item] && methodesUtilisees[item].verifie) {
//       nombreVerifiees++
//       delete methodesDisponibles[item]
//     }
//   })

//   debug("Methode d'authentification disponibles : %O\nMethodes utilisees: %O", methodesDisponibles, methodesUtilisees)

//   return {methodesDisponibles, methodesUtilisees, nombreVerifiees}
// }

async function upgradeProtegeCertificat(socket, params) {
  // const compteUsager = await comptesUsagersDao.chargerCompte(socket.nomUsager)
  params = params || {}
  const session = socket.handshake.session
  const challengeSession = session[CONST_CHALLENGE_CERTIFICAT],
        idmg = socket.amqpdao.pki.idmg,
        certCa = socket.amqpdao.pki.ca

  debug("authentification.upgradeProtegeCertificat Params recus : %O, challenge session %O", params, challengeSession)

  const resultat = await verifierSignatureCertificat(
    idmg, params.certificat, challengeSession, params, {certCa})

  debug("upgradeProtegeCertificat: Resultat = %O", resultat)

  return resultat.valide
}

async function upgradeProteger(socket, params) {
  params = params || {}

  // debug("upgradeProteger, params : %O", params)
  const session = socket.handshake.session,
        userId = session.userId,
        auth = session.auth

  const comptesUsagersDao = socket.comptesUsagersDao
  const compteUsager = await comptesUsagersDao.chargerCompteUserId(userId)
  const certMillegrilleForge = socket.amqpdao.pki.caForge,
        idmg = socket.amqpdao.pki.idmg

  // Pour permettre l'upgrade, on doit avoir un score d'authentification d'au
  // moins 2 methodes.
  let score = Object.values(auth).reduce((score, item)=>{
    score += item
  }, 0)

  if(score > 1) {
    // Conserver dans la session qu'on est alle en mode protege
    // Permet de revalider le mode protege avec le certificat de navigateur

    // Emettre le certificat de navigateur pour s'assurer qu'il existe sur le noeud
    var fullchain = params['certificat']
    if(fullchain) {
      debug("Authentification valide, info certificat : %O", fullchain)
      await comptesUsagersDao.emettreCertificatNavigateur(fullchain)
    }

    socket.activerModeProtege()

    return true

  } else {
    return false
  }

}

async function veriferUpgradeProtegerApp(socket, params, opts) {
  params = params || {}
  opts = opts || {}

  debug("upgradeProteger, params : %O", params)
  const session = socket.handshake.session,
        userId = socket.userId,
        nomUsager = socket.nomUsager,
        authScore = socket.authScore,
        challengeSocket = socket[CONST_CHALLENGE_CERTIFICAT],
        certCa = socket.amqpdao.pki.ca

  if(!challengeSocket) {
    debug("Challenge socket n'a pas ete genere")
    return false
  }

  const idmg = socket.amqpdao.pki.idmg

  // Pour permettre l'upgrade, on doit avoir un score d'authentification d'au
  // moins 2 methodes.
  let score = Number(authScore)

  if(score > 1) {
    // Conserver dans la session qu'on est alle en mode protege
    // Permet de revalider le mode protege avec le certificat de navigateur
    const resultat = await verifierSignatureCertificat(
      idmg, params.certificat, challengeSocket, params, {certCa})

    debug("upgradeProtegeCertificat: Resultat = %O", resultat)

    if(resultat.valide) {

      // Verifier que le userId correspond
      const extensions = resultat.extensions
      if(extensions.userId !== userId) {
        debug("Mauvais userId : socket %s, certificat %s", userId, extensions.userId)
        return false
      }

      // Verifier si on a des conditions supplementaires (e.g. delegations)
      if(socket.verifierAutorisation) {
        debug("Verifier autorisation usager %s / ext: %O / session : %O", nomUsager, extensions, session)
        let autorise = await socket.verifierAutorisation(socket, '3.protege', resultat.certificat)
        if(autorise) {
          debug("Activer mode protege pour socket %s de l'usager %s", socket.id, nomUsager)
          return {prive: true, protege: true}
        } else {
          autorise = await socket.verifierAutorisation(socket, '2.prive', resultat.certificat)
          return {prive: true, protege: false}
        }
      } else {
        debug("Activer mode prive pour socket %s de l'usager %s", socket.id, nomUsager)
        return {prive: true, protege: false}
      }

    }
  }

  return {prive: false, protege: false}
}

async function verifierSignatureCertificat(idmg, chainePem, challengeSession, challengeRecu, opts) {
  opts = opts || {}
  debug("verifierSignatureCertificat : idmg=%s, challengeRecu: %O, opts: %O", idmg, challengeRecu, opts)

  const challengeBody = JSON.parse(challengeRecu.contenu)

  if( ! challengeSession || ! challengeBody ) return false

  const { cert: certificat, idmg: idmgChaine } = await validerChaineCertificats(chainePem, opts)

  const nomUsager = certificat.subject.getField('CN').value,
        extensions = await extraireExtensionsMillegrille(certificat),
        userId = extensions.userId,
        roles = extensions.roles

  debug("verifierSignatureCertificat userId: %s, roles: %s, cert: %O", userId, roles, chainePem)

  if(!idmg || idmg !== idmgChaine) {
    console.error("Le certificat ne correspond pas a la millegrille : idmg %s !== %s", idmg, idmgChaine)
  } else if(!roles.includes('usager')) {
    console.error("Certificat fin n'est pas un certificat de role usager. roles=" + roles)
  } else if( challengeBody.date !== challengeSession.date ) {
    console.error(`Challenge certificat mismatch date : session ${challengeSession.date} et body ${challengeBody.date}`)
  } else if( challengeBody.data !== challengeSession.data ) {
    console.error(`Challenge certificat mismatch data session ${challengeSession.data} et body ${challengeBody.data}`)
  } else {

    debug("Verification authentification par certificat pour idmg %s, signature :\n%s", idmg, challengeBody['_signature'])

    // Verifier les certificats et la signature du message
    // Permet de confirmer que le client est bien en possession d'une cle valide pour l'IDMG
    debug("authentifierCertificat, cert :\n%O\nchallengeJson\n%O", certificat, challengeBody)
    const valide = await verifierSignatureMessage(challengeRecu, certificat)
    debug("Validation certificat, resultat : %O", valide)

    if(valide) {
      const extensions = extraireExtensionsMillegrille(certificat)
      return { valide, certificat, idmg, nomUsager, userId, extensions }
    }

  }

  throw new Error("Signature avec certificat invalide")
}

async function verifierSignatureMillegrille(certificatMillegrille, challengeSession, challengeBody) {
  // Validation de la signature de la cle de MilleGrille

  if( challengeBody.date !== challengeSession.date ) {
    console.error(`Challenge certificat mismatch date : session ${challengeSession.date} et body ${challengeBody.date}`)
  } else if( challengeBody.data !== challengeSession.data ) {
    console.error(`Challenge certificat mismatch date : session ${challengeSession.data} et body ${challengeBody.data}`)
  } else {

    // Verifier les certificats et la signature du message
    // Permet de confirmer que le client est bien en possession d'une cle valide pour l'IDMG
    debug("Verification authentification par certificat de millegrille, cert :\n%O\nchallenge\n%O", certificatMillegrille, challengeBody)
    const valide = await verifierSignatureMessage(challengeBody, certificatMillegrille)
    debug("Resultat verifier signature : %O", valide)

    if( valide ) {
      return { valide, certificatMillegrille }
    }

  }

  throw new Error("Signature avec cle de Millegrille invalide")
}

module.exports = {
  verifierUsager, 
  // auditMethodes, auditMethodesDisponibles,
  upgradeProteger, upgradeProtegeCertificat,
  verifierSignatureCertificat, verifierSignatureMillegrille,
  genererChallengeCertificat,
  // verifierMethode, 
  veriferUpgradeProtegerApp,

  CONST_CHALLENGE_CERTIFICAT, CONST_AUTH_PRIMAIRE, CONST_AUTH_SECONDAIRE, CONST_WEBAUTHN_CHALLENGE,
}
