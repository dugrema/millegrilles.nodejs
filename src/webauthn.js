import debugLib from 'debug'

import multibase from 'multibase'
import base64url from 'base64url'
import { Fido2Lib } from 'fido2-lib'

import { hachage, constantes } from '@dugrema/millegrilles.utiljs'

const { hacherMessageSync } = hachage
const { CONST_COMMANDE_AUTH, CONST_COMMANDE_SIGNER_CSR } = constantes

const debug = debugLib('millegrilles:webauthn')

// Variables globales
var _f2l = null
var _hostname = null

function init(hostname, idmg, opts) {
  opts = opts || {}
  debug("common.webauthn Init hostname: %s, idmg: %s, opts: %O", hostname, idmg, opts)

  if(opts.f2l) {
    _f2l = opts.f2l
  } else {
    const options = {
      timeout: 60000,
      rpId: hostname,  // "mg-dev4.maple.maceroc.com",
      rpName: idmg,  // "MilleGrilles",
      // rpIcon: "https://example.com/logo.png",
      challengeSize: 128,
      attestation: "none",
      cryptoParams: [-7, -257],
      // authenticatorAttachment: "platform",
      // authenticatorAttachment: "cross-platform",
      authenticatorRequireResidentKey: false,
      authenticatorUserVerification: "preferred"
    }
    debug("Initialisation webauthn : %O", options)
    _f2l = new Fido2Lib(options)
  }

  // Conserver instance
  _hostname = hostname

  return {f2l: _f2l}
}

export async function genererChallenge(methodesDisponibles, opts) {
  opts = opts || {}
  const authnOptions = await _f2l.assertionOptions()
  debug("AuthnOptions : %O, opts: %O", authnOptions, opts)
  const hostname = opts.hostname || authnOptions.rpId
  authnOptions.rpId = hostname

  const challengeTyped = new Uint8Array(authnOptions.challenge)

  // Indiquer que le challenge est pour une authentification
  challengeTyped[0] = CONST_COMMANDE_AUTH

  const challenge = String.fromCharCode.apply(null, multibase.encode('base64', challengeTyped))

  const allowCredentials = Object.keys(methodesDisponibles)
    .filter(item=>item.startsWith('webauthn.'))
    .map(item=>{
        const credId = item.split('.')[1]
        return {id: credId, type: 'public-key'}
      }
    )

  const authnOptionsBuffer = {
    ...authnOptions,
    challenge,
    allowCredentials,
    hostname,
  }

  debug("Authentication options : %O", authnOptionsBuffer)

  return authnOptionsBuffer
}

export function webauthnResponseBytesToMultibase(clientAssertionResponse) {
  // Structure {
  //    id,
  //    id64,
  //    response: {
  //      authenticatorData, clientDataJSON, signature, userHandle
  //    }
  //  }

  debug("webauthnResponseBytesToMultibase : Preparer versions serialisable de %O", clientAssertionResponse)

  const clientResponse = clientAssertionResponse.response

  // Debug, encoder response en multibase
  const authenticatorDataMB = String.fromCharCode.apply(null, multibase.encode('base64', new Uint8Array(clientResponse.authenticatorData)))
  const clientDataJSONMB = String.fromCharCode.apply(null, multibase.encode('base64', new Uint8Array(clientResponse.clientDataJSON)))
  const signatureMB = String.fromCharCode.apply(null, multibase.encode('base64', new Uint8Array(clientResponse.signature)))
  debug('"authenticatorData": "%s",\n"clientDataJSON": "%s",\n"signature": "%s"', authenticatorDataMB, clientDataJSONMB, signatureMB)

  const responseConvertie = {
    id64: clientAssertionResponse.id64,
    response: {
      authenticatorData: authenticatorDataMB,
      clientDataJSON: clientDataJSONMB,
      signature: signatureMB,
      userHandle: null,
    }
  }

  debug("webauthnResponseBytesToMultibase : Reponse serialisable %O", responseConvertie)

  return responseConvertie
}

export async function verifierChallenge(challengeInfo, compteUsager, clientAssertionResponse, opts) {
  opts = opts || {}
  const authChallenge = challengeInfo.challenge,
        rpId = challengeInfo.rpId,
        demandeCertificat = opts.demandeCertificat

  debug(
    "authentifier: challenge : %O\ncompteUsager: %O\nauthResponse: %O\nopts : %O",
    challengeInfo, compteUsager, clientAssertionResponse, opts
  )

  // Faire correspondre credId
  const credId64 = clientAssertionResponse.id64
  const credInfo = compteUsager.webauthn.filter(item=>{
    return item.credId === credId64
  })[0]

  const userId = multibase.decode(compteUsager.userId)
  const prevCounter = credInfo.counter

  debug("Cred info match: %O", credInfo)
  clientAssertionResponse.id = new Uint8Array(clientAssertionResponse.id).buffer

  const clientResponse = clientAssertionResponse.response

  // Debug, encoder response en multibase
  const authenticatorDataMB = String.fromCharCode.apply(null, multibase.encode('base64', clientResponse.authenticatorData))
  const clientDataJSONMB = String.fromCharCode.apply(null, multibase.encode('base64', clientResponse.clientDataJSON))
  const signatureMB = String.fromCharCode.apply(null, multibase.encode('base64', clientResponse.signature))
  debug('"authenticatorData": "%s",\n"clientDataJSON": "%s",\n"signature": "%s"', authenticatorDataMB, clientDataJSONMB, signatureMB)

  clientResponse.authenticatorData = new Uint8Array(clientResponse.authenticatorData).buffer,
  clientResponse.clientDataJSON = new Uint8Array(clientResponse.clientDataJSON).buffer,
  clientResponse.signature = new Uint8Array(clientResponse.signature).buffer,
  clientResponse.userHandle = new Uint8Array(clientResponse.userHandle).buffer

  const publicKey = credInfo.publicKeyPem
  const challengeBuffer = multibase.decode(authChallenge)

  debug("Public Key : %O", publicKey)

  if(demandeCertificat) {
    debug("Reponse avec challenge de signature, on recalcule les 65 premiers bytes pour la verification")
    const hachageDemandeCert = hacherMessageSync(demandeCertificat)
    debug("Hachage demande cert %O = %O", hachageDemandeCert, demandeCertificat)
    challengeBuffer[0] = CONST_COMMANDE_SIGNER_CSR
    challengeBuffer.set(hachageDemandeCert, 1)  // Override bytes 1-65 du challenge
    debug("Challenge override pour demander signature certificat : %O", challengeBuffer)
  }

  let typeChallenge = challengeBuffer[0]  // Par defaut, authentification
  debug("Challenge (type: %d): %O", typeChallenge, challengeBuffer)

  var assertionExpectations = {
      challenge: challengeBuffer,
      origin: "https://" + rpId,
      factor: "either",
      publicKey,
      userHandle: userId,
      prevCounter,
  }

  debug("Client assertion response : %O", clientAssertionResponse)
  debug("Assertion expectations : %O", assertionExpectations)

  var authnResult = await _f2l.assertionResult(clientAssertionResponse, assertionExpectations) // will throw on error
  debug("Authentification OK, resultat : %O", authnResult)

  const counter = authnResult.authnrData.get('counter') || 0
  let userVerification = false,
      userPresence = false
  try {
    const flags = authnResult.authnrData.get('flags')
    userPresence = flags.has('UP')
    userVerification = flags.has('UV')
  } catch(err) {console.warn("webauthn.verifierChallenge Erreur verif flags : %O", err)}

  return {authentifie: true, counter, userVerification, userPresence, assertionExpectations}
}

export async function genererRegistrationOptions(userId, nomUsager, opts) {
  opts = opts || {}
  debug("Registration request, userId %s, usager %s, opts: %O", userId, nomUsager, opts)
  // const attestationParams = {
  //     relyingParty: { name: _hostname },
  //     user: { id: userId, name: nomUsager }
  // }
  // debug("Registration attestation params : %O", attestationParams)

  const attestationOptions = await _f2l.attestationOptions()
  debug("Registration options : %O", attestationOptions)

  // Override rpId au besoin
  const relayingParty = attestationOptions.rp
  const hostname = opts.hostname || relayingParty.id
  relayingParty.id = hostname

  const challenge = String.fromCharCode.apply(null, multibase.encode('base64', new Uint8Array(attestationOptions.challenge)))

  var userIdString = userId
  if(typeof(userIdString) !== 'string') {
    userIdString = String.fromCharCode.apply(null, multibase.encode('base58btc', new Uint8Array(userId)))
  }

  const attestationExpectations = {
      challenge,
      origin: `https://${hostname}`,
      factor: 'either'
  }

  var attestationOptionsSerialized = {
    ...attestationOptions,
    user: {
      ...attestationOptions.user,
      id: userIdString,
      //name: nomUsager,
      //displayName: nomUsager,
    },
    challenge,
  }
  debug("Attestation opts serialized : %O\nExpectations: %O", attestationOptionsSerialized, attestationExpectations)

  return {
    userId: userIdString,
    nomUsager,
    hostname,
    challenge,  // Retourner challenge encode pour serialiser dans la session
    attestation: attestationOptionsSerialized,
    attestationExpectations,
  }
}

export async function validerRegistration(response, attestationExpectations) {
  debug("validerRegistration sessionChallenge : %O, reponse : %O", attestationExpectations, response)
  let sessionChallenge = attestationExpectations.challenge
  if(typeof(sessionChallenge) === 'string') {
    sessionChallenge = multibase.decode(sessionChallenge)
  }

  const updExpectations = {...attestationExpectations, challenge: sessionChallenge}

  // const attestationExpectations = {
  //     challenge: sessionChallenge,
  //     origin: `https://${_hostname}`,
  //     factor: 'either'
  // }
  debug("Attestation expectations : %O", updExpectations)

  const rawId = new Uint8Array(Buffer.from(base64url.decode(response.id))).buffer
  const clientAttestationResponse = {
    id: response.id,
    rawId,
    response: response.response,
  }
  debug("Client attestation response : %O", clientAttestationResponse)

  var regResult = await _f2l.attestationResult(clientAttestationResponse, updExpectations)
  debug("Registration result OK : %O", regResult)

  const authnrData = regResult.authnrData

  const credId = String.fromCharCode.apply(null, multibase.encode('base64', new Uint8Array(authnrData.get('credId'))))
  const counter = authnrData.get('counter') || 0
  const publicKeyPem = authnrData.get('credentialPublicKeyPem')

  const informationCle = {
    // userId,
    credId,
    // nomUsager,
    counter,
    publicKeyPem,
    type: 'public-key',
  }

  return informationCle
}

// module.exports = {
//   init,
//   genererRegistrationOptions,
//   genererChallenge, verifierChallenge, validerRegistration,
//   webauthnResponseBytesToMultibase,
// }
