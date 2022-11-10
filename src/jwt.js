const debug = require('debug')('nodejs:jwt')
const { SignJWT, jwtVerify, decodeProtectedHeader } = require("jose")
const { createPrivateKey, X509Certificate } = require("crypto")

async function signerTokenFichier(fingerprint, clePriveePem, userId, fuuid, opts) {
    opts = opts || {}

    const expiration = opts.expiration || '2h'

    const privateKey = createPrivateKey({
        key: clePriveePem,
        format: "pem",
        type: "pkcs8",
    })
  
    const jwt = await new SignJWT({userId})
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

    // TODO : valider le certificat

    const certNode = new X509Certificate(certificat)
    const publicKey = certNode.publicKey
    debug("verifierTokenFichier Cert charge : %O\nPublic key : %O", certNode, publicKey)

    const verification = await jwtVerify(jwt, publicKey)

    return verification.payload
}

module.exports = { signerTokenFichier, verifierTokenFichier }
