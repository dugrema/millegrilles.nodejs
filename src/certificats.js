const { pki } = require('@dugrema/node-forge')
const { hacher } = require('./hachage')

/**
 * 
 * @param {*} pem Certificat X.509 en format PEM.
 * @returns Fingerprint de la cle publique 
 */
 async function fingerprintPublicKeyFromCertPem(pem) {
    const cert = pki.certificateFromPem(pem)
    const publicKeyBytes = cert.publicKey.publicKeyBytes
    const fingerprintPk = await hacher(publicKeyBytes, {hashingCode: 'blake2s-256', encoding: 'base58btc'})
    return fingerprintPk
  }
  
module.exports = {
    fingerprintPublicKeyFromCertPem
}