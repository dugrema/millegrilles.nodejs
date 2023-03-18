const debug = require('debug')('nodejs:jwt')
const { SignJWT, jwtVerify, decodeProtectedHeader } = require("jose")
const { createPrivateKey, createPublicKey } = require("crypto")
const { extraireExtensionsMillegrille } = require('@dugrema/millegrilles.utiljs/src/forgecommon')

async function signerTokenFichier(fingerprint, clePriveePem, userId, fuuid, opts) {
    opts = opts || {}

    const expiration = opts.expiration || '2h'

    const privateKey = createPrivateKey({
        key: clePriveePem,
        format: "pem",
        type: "pkcs8",
    })
  
    const params = {userId}

    // Copier champs optionnels
    const champsCopier = ['domaine', 'mimetype', 'ref', 'header', 'iv', 'tag', 'format']
    for(const champ of champsCopier) {
        if(opts[champ]) params[champ] = opts[champ]
    }

    const jwt = await new SignJWT(params)
        .setProtectedHeader({ alg: 'EdDSA', kid: fingerprint })
        .setSubject(fuuid)
        .setExpirationTime(expiration)
        .sign(privateKey);

    return jwt
}

async function verifierTokenFichier(pki, jwt) {
    debug("JWT : ", jwt)

    const header = decodeProtectedHeader(jwt)
    const fingerprint = header.kid
    debug("verifierTokenFichier Decoder header : %O\nFingerprint : %O", header, fingerprint)
  
    const certificat = await pki.getCertificate(fingerprint)
    debug("verifierTokenFichier Certificat %s charge : %O", fingerprint, certificat)

    const certificatForge = pki.validerCertificat(certificat)
    // console.debug("!!!! Resultat validation : ", certificatForge)
    const extensions = extraireExtensionsMillegrille(certificatForge)

    const publicKeyBytes = certificatForge.publicKey.publicKeyBytes
    const publicKey = createPublicKey({
        key: Buffer.concat([Buffer.from("302a300506032b6570032100", "hex"), publicKeyBytes]),
        format: "der",
        type: "spki",
      });
    debug("verifierTokenFichier Cert charge : %O\nPublic key : %O", certificat[0], publicKey)

    const verification = await jwtVerify(jwt, publicKey)

    return {...verification, extensions}
}

async function signerTokenApplication(fingerprint, clePriveePem, applicationId, uuid_transaction, opts) {
    opts = opts || {}

    const expiration = opts.expiration || '24h'

    const privateKey = createPrivateKey({
        key: clePriveePem,
        format: "pem",
        type: "pkcs8",
    })
  
    const params = {
        application_id: applicationId, 
        // uuid_transaction,
    }

    // Copier champs optionnels
    // const champsCopier = ['domaine', 'mimetype', 'ref', 'header', 'iv', 'tag', 'format']
    // for(const champ of champsCopier) {
    //     if(opts[champ]) params[champ] = opts[champ]
    // }

    const jwt = await new SignJWT(params)
        .setProtectedHeader({ alg: 'EdDSA', kid: fingerprint })
        .setSubject(uuid_transaction)
        .setExpirationTime(expiration)
        .sign(privateKey);

    return jwt
}

async function verifierTokenApplication(pki, jwt) {
    return await verifierTokenFichier(pki, jwt)
}

module.exports = { signerTokenFichier, verifierTokenFichier, signerTokenApplication, verifierTokenApplication }
