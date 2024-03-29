const debug = require('debug')('millegrilles:pki')
const debugMessages = require('debug')('millegrilles:pki:messages')

const { base64 } = require('multiformats/bases/base64')
const { pki } = require('@dugrema/node-forge')

const forgecommon = require('@dugrema/millegrilles.utiljs/src/forgecommon')
const {splitPEMCerts, FormatteurMessageEd25519} = require('@dugrema/millegrilles.utiljs/src/formatteurMessage')
const { verifierMessage } = require('@dugrema/millegrilles.utiljs/src/validateurMessage')
const { encoderIdmg } = require('@dugrema/millegrilles.utiljs/src/idmg')
const { chargerPemClePriveeEd25519, exporterPemClePriveeEd25519 } = require('@dugrema/millegrilles.utiljs/src/certificats')
const { dechiffrerCle } = require('@dugrema/millegrilles.utiljs/src/chiffrage.ed25519')

const { hacherCertificat } = require('./hachage')
const { preparerCipher, preparerCommandeMaitrecles, preparerDecipher } = require('./chiffrage')

const PEM_CERT_DEBUT = '-----BEGIN CERTIFICATE-----'
const PEM_CERT_FIN = '-----END CERTIFICATE-----'
const EXPIRATION_CERTCACHE = 2 * 60000,   // 2 minutes en millisecs
      EXPIRATION_REDIS_CERT = ''+(48 * 60 * 60)  // 48 heures en secondes

class MilleGrillesPKI {
  // Classe qui supporte des operations avec certificats et cles privees.

  constructor() {
    this.idmg = null

    // Cle pour cert
    this.cle = null
    this.password = null  // Mot de passe pour la cle, optionnel

    // Liste de certificats de la chaine
    // this.chaineCertificatsList = null

    // Contenu format texte PEM
    this.chainePEM = null
    // this.hotePEM = null  // Chaine XS pour connexion middleware
    // this.hoteCA = null
    this.ca = null
    this.fingerprintCa = null
    this.fingerprint = null

    // this.caIntermediaires = []

    // Objets node-forge
    this.certPEM = null
    this.cleForge = null  // Objet cle charge en memoire (forge)
    this.cert = null      // Objet certificat charge en memoire (forge)
    this.caForge = null   // CA (certificat de MilleGrille)
    this.caStore = null   // CA store pour valider les chaines de certificats
    this.caCertificateStore = null  // instance CertificateStore

    // Cle : fingerprintb58, value = { ts (date millisecs), chaineForge:[...certForge] }
    this.cacheCertsParFingerprint = {}

    // this.algorithm = 'aes256'
    // this.rsaAlgorithm = 'RSA-OAEP'

    // Gestionnaire de messages de certificats (optionnel)
    // Permet de faire des requetes pour aller chercher des nouveaux
    // certificats
    this.gestionnaireCertificatMessages = null

    this.formatteurMessage = null

    // Client redis pour caching - optionnel, permet de stocker les certificats
    this.redisClient = null
  }

  async initialiserPkiPEMS(certs) {

    // Cle pour cert
    this.cle = certs.key
    this.password = certs.password  // Mot de passe pour la cle, optionnel

    // Contenu format texte PEM
    this.chainePEM = certs.cert
    // this.hotePEM = certs.hote || certs.cert  // Chaine XS pour connexion middleware
    // this.hoteCA = certs.hoteMillegrille || certs.millegrille
    this.ca = certs.millegrille

    // DEPRECATED, remplace par redis
    // // Preparer repertoire pour sauvegarder PEMS
    // fs.mkdir(REPERTOIRE_CERTS_TMP, {recursive: true, mode: 0o700}, e=>{
    //   if(e) {
    //     throw new Error(e)
    //   }
    // });

    // Charger le certificat pour conserver commonName, fingerprint
    await this._initialiserStoreCa(this.chainePEM, this.ca)

    let cle = this.cle
    if(this.password) {
      debug("Cle chiffree")
      // this.cleForge = pki.decryptRsaPrivateKey(cle, this.password)
      this.cleForge = chargerPemClePriveeEd25519(cle, {password: this.password})
      // Re-exporter la cle en format PEM dechiffre (utilise par RabbitMQ, formatteur)
      this.cle = exporterPemClePriveeEd25519(this.cleForge)
    } else {
      // this.cleForge = pki.privateKeyFromPem(cle)
      this.cleForge = chargerPemClePriveeEd25519(cle)
    }

    // Creer instance de formatteur de messages
    this.formatteurMessage = new FormatteurMessageEd25519(this.chainePEM, this.cle)

    // Initialiser maintenance cache
    this.intervalleMaintenanceCache = setInterval(()=>{this.maintenanceCache()}, 60000)
  }

  fermer() {
    clearInterval(this.intervalleMaintenanceCache)
  }

  // _verifierCertificat() {
  //   this.getFingerprint()
  // }

  formatterMessage(kind, message, opts) {
    if(isNaN(Number.parseInt(kind))) throw new Error("nodejs.pki.formatterMessage Kind doit etre un integer")
    // Retourner promise
    return this.formatteurMessage.formatterMessage(kind, message, opts)
  }

  async verifierMessage(message, opts) {
    opts = opts || {}
    const optsMessages = {...opts}  // Copie

    debugMessages("verifierMessage ", message)

    const chaineForge = await this.getCertificatMessage(message, opts)
    // Trouver le certificat correspondant au message
    // const fingerprint = message['en-tete'].fingerprint_certificat
    // const chaine = message['_certificat'],
    //       millegrille = message['_millegrille']

    // if(chaine) {
    //   // On a un certificat inline - on tente quand meme d'utiliser le cache
    //   optsMessages.nowait = true
    // }
    // var chaineForge = null
    // var _err = null
    // try {
    //   chaineForge = await this.getCertificate(fingerprint, optsMessages)
    // } catch(err) {
    //   _err = err
    // }

    // if(!chaineForge) {
    //   if(!chaine) throw _err
    //   debug("Certificat non trouve localement, mais il est inline")
    //   await this.sauvegarderMessageCertificat({chaine_pem: chaine, millegrille}, fingerprint, opts)
    //   const certCache = this.cacheCertsParFingerprint[fingerprint]
    //   // let chaineForge = null
    //   if(certCache) {
    //     chaineForge = certCache.chaineForge
    //     certCache.ts = new Date().getTime()  // Touch
    //   }
    //   debug("Certificat inline sauvegarde sous %s\n%O", fingerprint, chaineForge)
    // }

    // Retourner promise
    const certificat = chaineForge[0]

    const valide = await verifierMessage(message, {certificat})
    return {valide, certificat}
  }

  async getCertificatMessage(message, opts) {
    opts = opts || {}
    const fingerprint = message.pubkey
    // const fingerprint = message['en-tete'].fingerprint_certificat
    const chaine = message['certificat'],
          millegrille = message['millegrille']

    const optsMessages = {...opts}
    if(chaine) {
      // On a un certificat inline - on tente quand meme d'utiliser le cache
      optsMessages.nowait = true
    }
    var chaineForge = null
    var _err = null
    try {
      chaineForge = await this.getCertificate(fingerprint, optsMessages)
    } catch(err) {
      _err = err
    }

    if(!chaineForge) {
      if(!chaine) throw _err
      debug("Certificat non trouve localement, mais il est inline")
      const fingerprintLocal = await this.sauvegarderMessageCertificat({chaine_pem: chaine, millegrille}, fingerprint, opts)
      const certCache = this.cacheCertsParFingerprint[fingerprintLocal]
      // let chaineForge = null
      if(certCache) {
        chaineForge = certCache.chaineForge
        certCache.ts = new Date().getTime()  // Touch
      } else {
        throw new Error(`Echec preparation de certificat : ${fingerprint}`)
      }
    }

    return chaineForge
  }

  async _initialiserStoreCa(certPem) {

    // Charger certificat local
    var certs = splitPEMCerts(certPem)
    this.chaineCertificatsList = certs
    debug("Certificat local: %O", certs)
    this.certPEM = certs[0]

    let parsedCert = pki.certificateFromPem(this.certPEM)

    const fingerprint = await hacherCertificat(parsedCert)
    this.fingerprint = fingerprint
    this.cert = parsedCert
    this.commonName = parsedCert.subject.getField('CN').value

    // Sauvegarder certificats intermediaires
    const certsChaineCAList = certs.slice(1)
    const certsIntermediaires = []
    for(let idx in certsChaineCAList) {
      var certIntermediaire = certsChaineCAList[idx]
      let intermediaire = pki.certificateFromPem(certIntermediaire)
      certsIntermediaires.push(intermediaire)
    }
    this.caIntermediaires = certsIntermediaires

    // Creer le CA store pour verifier les certificats.
    let parsedCACert = pki.certificateFromPem(this.ca)
    this.caForge = parsedCACert
    this.caStore = pki.createCaStore([parsedCACert])
    // Objet different pour valide certs, supporte date null
    this.caCertificateStore = new forgecommon.CertificateStore(parsedCACert, {DEBUG: false})

    // Recalculer IDMG, calculer fingerprint
    this.idmg = await encoderIdmg(this.ca)
    this.fingerprintCa = await hacherCertificat(parsedCACert)
    const localCertIdmg = parsedCert.subject.getField("O").value
    debug("IDMG CA %s, IDMG local cert %s", this.idmg, localCertIdmg)
    if(localCertIdmg !== this.idmg) throw new Error(`Cert local IDMG: ${localCertIdmg} ne correspond pas au CA ${this.idmg}`)
  }

  preparerMessageCertificat() {
    // Retourne un message qui peut etre transmis a MQ avec le certificat
    // utilise par ce noeud. Sert a verifier la signature des transactions.
    let transactionCertificat = {
        fingerprint: this.fingerprint,
        chaine_pem: this.chaineCertificatsList,
        attache: true,
    }

    return transactionCertificat;
  }

  async decrypterAsymetrique(contenuSecret) {
    // return dechiffrerCleSecreteForge(this.cleForge, contenuSecret)
    return dechiffrerCle(contenuSecret, this.cleForge.privateKeyBytes)
  }

  async creerCipherChiffrageAsymmetrique(certificatsPem, domaine, identificateurs_document, opts) {
    const publicKeyBytesMillegrille = this.caForge.publicKey.publicKeyBytes
    const cipherInst = await preparerCipher({clePubliqueEd25519: publicKeyBytesMillegrille}),
          cipher = cipherInst.cipher

    console.debug("CipherInst : %O", cipherInst)

    const cipherWrapper = {
      update: cipher.update,
      finalize: async () => {
        const infoChiffrage = await cipher.finalize()
        console.debug("InfoChiffrag : %O", infoChiffrage)

        // Chiffrer le password avec les certificats
        const commandeMaitreCles = await preparerCommandeMaitrecles(
          certificatsPem, cipherInst.secretKey, domaine, 
          infoChiffrage.hachage, identificateurs_document,
          {...opts, ...infoChiffrage}
        )

        // Remplacer cle chiffree de millegrille par peerPublic (secretChiffre)
        const cles = commandeMaitreCles.cles
        cles[this.fingerprintCa] = cipherInst.secretChiffre

        return {meta: infoChiffrage, commandeMaitreCles}
      }
    }

    return cipherWrapper
  }

  /**
   * 
   * @param informationCle { iv, cle } ou { iv, cles }. cles est un dict {[fingerprint]: cle}
   * @returns Decipher initialise avec la cle privee locale
   */
  async creerDecipherChiffrageAsymmetrique(informationCle) {
    let { iv, cle, cles } = informationCle

    if(cle) {
      // On utilise la cle fournie directement
    } else if(cles) {
      // Trouver la cle qui correspond, par fingerprint
      cle = cles[this.fingerprint]
      if(!cle) throw new Error(`Cle non trouvee pour fingerprint ${this.fingerprint}`)
    } else throw new Error("Cle non trouvee")

    if(typeof(cle) === 'string') cle = base64.decode(cle)
    else if( ! Buffer.isBuffer(cle) && ! ArrayBuffer.isView(cle) ) throw new Error(`Format cle non supporte : ${cle}`)

    if( typeof(iv) === 'string') iv = base64.decode(iv)
    else if( ! Buffer.isBuffer(iv) && ! ArrayBuffer.isView(iv) ) throw new Error(`Format IV non supporte : ${iv}`)

    // Dechiffrer la cle asymmetrique
    debug("Dechiffrer cle asymmetrique : %O", cle)
    const cleSecrete = await this.decrypterAsymetrique(cle)

    return preparerDecipher(cleSecrete, iv)
  }

  // Sauvegarde un message de certificat en format JSON
  async sauvegarderMessageCertificat(message_in, fingerprint_in, opts) {
    opts = opts || {}
    debug("sauvegarderMessageCertificat Message in\n", message_in)    
    let message = message_in
    if(typeof(message) === 'string') message = JSON.parse(message)
    if(message.contenu) message = JSON.parse(message.contenu)
    let chaine_pem = ''
    const routage = message_in.routage || {},
          action = routage.action
    // const entete = message['en-tete'] || {},
    //       action = entete.action

    if(action === 'infoCertificat') {
      chaine_pem = message.chaine_pem
    } else if(message.attache || message['certificat']) {
      // Nouveau format
      chaine_pem = message['certificat']
    } else {
      chaine_pem = message.chaine_pem
      if(!chaine_pem && message.resultats) {
        chaine_pem = message.resultats.chaine_pem
      }
    }

    if(!chaine_pem) {
      throw new Error("Erreur reception certificat attache, mauvais format.")
    }

    if(!fingerprint_in) {
      // Calculer hachage du cerficat
      const cert = pki.certificateFromPem(chaine_pem[0])
      fingerprint_in = await hacherCertificat(cert)
      debug("sauvegarderMessageCertificat Fingerprint calcule ", fingerprint_in)
    }

    // debug("sauvegarderMessageCertificat, fichier %s existe? %s", fingerprintBase58, fichierExiste)
    let fichierExiste = false
    const cleCert = 'certificat_v1:' + fingerprint_in

    debug("sauvegarderMessageCertificat %s", cleCert)

    if(this.redisClient) {
      debug("Sauvegarder/touch certificat dans client redis : %s", fingerprint_in)
      const resultat = await this.redisClient.expire(cleCert, EXPIRATION_REDIS_CERT)
      fichierExiste = resultat > 0
      debug("Certificat %s existe?%s", fingerprint_in, fichierExiste)
    }

    if( ! fichierExiste ) {
      // Verifier la chain de certificats
      let certificatCa = this.ca
      let certificatCaTiers = null
      if(opts.tiers === true && message['millegrille']) {
        // Supporter certificat CA tiers attache sous _millegrille
        certificatCa = message['millegrille']
        certificatCaTiers = message['millegrille']
        debug("Utiliser certificat tiers : %O", certificatCa)
      }
      const store = new forgecommon.CertificateStore(certificatCa, {DEBUG: false})
      if(store.verifierChaine(chaine_pem, {validityCheckDate: null})) {
        const chaineCerts = chaine_pem.map(pem=>{
          return pki.certificateFromPem(pem)
        })
        let certificat = chaineCerts[0]
        let fingerprint = await hacherCertificat(certificat)

        // La chaine est valide, on sauvegarde le certificat
        const certCache = {ts: new Date().getTime(), chaineForge: chaineCerts, ca: certificatCa}
        debug("sauvegarderMessageCertificat: Ajouter certificat au cache : %O", certCache)
        this.cacheCertsParFingerprint[fingerprint] = certCache

        if(this.redisClient) {
          const valeurCache = {'pems': chaine_pem, 'ca': certificatCaTiers}
          await this.redisClient.set(cleCert, JSON.stringify(valeurCache), {NX: true, EX: EXPIRATION_REDIS_CERT})
        }

        // Informatif seulement : verifier si c'est bien le certificat qui a ete demande
        if(fingerprint !== fingerprint_in) {
          debug(`WARN: Certificat ${fingerprint} sauvegarde localement, mais ne correspond pas au fingerprint demande ${fingerprint_in}`)
        }

        debug("sauvegarderMessageCertificat Cert %s sauvegarde", fingerprint_in)

        return fingerprint_in

      } else {
        throw new Error(`Erreur validation certificat recu : ${fingerprint_in}`)
      }

    } else {
      debug("Certificat (%s) existe deja dans redis", fingerprint_in)
      return fingerprint_in
    }
  }

  // Charge la chaine de certificats pour ce fingerprint
  async getCertificate(fingerprint, opts) {
    opts = opts || {}
    const fingerprintEffectif = fingerprint  // Changement pour multibase, multihash

    // Tenter de charger le certificat a partir du cache memoire
    debug("getCertificate: Fingerprint (nowait: %s) : %s", opts.nowait, fingerprintEffectif)
    try {
      const cacheCert = this.cacheCertsParFingerprint[fingerprintEffectif]
      if(cacheCert && cacheCert.chaineForge) {
        cacheCert.ts = new Date().getTime()  // Touch
        return cacheCert.chaineForge
      }
    } catch(err) {
      debug("ERROR getCertificate Erreur verification certificat dans le cache %O", err)
    }

    // Verifier si le certificat existe sur le disque
    if(this.redisClient) {
      try {
        const certificatInfo = await chargerCertificatFS(this.redisClient, fingerprintEffectif)
        const chaine = certificatInfo.certificat,
              ca = certificatInfo.ca

        // Valider le certificat avec le store
        let valide = true
        try {
          if(opts.tiers && ca) {
            debug("getCertificat Verification certificat avec store custom pour CA\n%O", ca)
            const store = pki.createCaStore([ca])
            pki.verifyCertificateChain(store, chaine)
          } else {
            pki.verifyCertificateChain(this.caStore, chaine)
          }
          const certCache = {ts: new Date().getTime(), chaineForge: chaine}
          // debug("getCertificate: Ajouter certificat au cache : %O", certCache)
          this.cacheCertsParFingerprint[fingerprintEffectif] = certCache
          return chaine
        } catch (err) {
          valide = false
          debug('Certificate verification failure: %O', err)
          // const erreur = new Error("Certificat local invalide pour " + fingerprint)
          err.fingerprint = fingerprint
          throw err
        }

      } catch(err) {
        debug("Chargement certificat via redis MISS, tenter via MQ : %O", err)
      }
    }

    // Demander le certificat via MQ
    let certificat = null
    if( ! opts.nowait && ! certificat && this.gestionnaireCertificatMessages ) {

      // Effectuer une requete pour recuperer le certificat
      certificat = await new Promise((resolve, reject)=>{
        const callback = (err, chaineForge) => {
          if(err) reject(err)
          else {
            resolve(chaineForge)
          }
        }
        this.gestionnaireCertificatMessages.demanderCertificat(fingerprint, {callback})
      })

    }

    if(!certificat) {
      const erreur = new CertificatInconnu(`Certificat inconnu : ${fingerprint}`)
      erreur.fingerprint = fingerprint
      throw erreur
    }

    return certificat
  }

  async maintenanceCache() {
    const cacheCertsParFingerprint = {}
    const expiration = new Date().getTime() - EXPIRATION_CERTCACHE
    for(let key in this.cacheCertsParFingerprint) {
      const value = this.cacheCertsParFingerprint[key]
      const ts = value.ts
      if(ts >= expiration) {
        // Conserver, pas expire
        cacheCertsParFingerprint[key] = value
      }
    }

    // Conserver nouvelle version du cache
    debug("Maintenance cache amqpdao.pki (%d keys left)", Object.keys(cacheCertsParFingerprint).length)
    this.cacheCertsParFingerprint = cacheCertsParFingerprint
  }

  validerCertificat(chainePem, opts) {
    opts = opts || {}
    let validityCheckDate = new Date()
    if(opts.validityCheckDate !== undefined) {
      validityCheckDate = opts.validityCheckDate
    }
    
    // Verifier la chaine
    // Retourne le certificat (forge) si valide
    // retourne false si certificat invalide
    const certificat = this.caCertificateStore.verifierChaine(chainePem, {validityCheckDate})

    return certificat
  }

}

async function chargerCertificatFS(redisClient, fingerprint) {
  const cleCert = 'certificat_v1:' + fingerprint
  const data = await redisClient.get(cleCert)
  debug("chargerCertificatFS Resultat chargement cert redis : %O", data)

  if(!data) throw new Error(`Aucune donnee pour certificat ${fingerprint}`) // No data

  const certCache = JSON.parse(data)   //splitPEMCerts(data)
  const listePems = certCache.pems
  const chaineForge = listePems.map(pem=>{
    return pki.certificateFromPem(pem)
  })
  const caForge = certCache.ca?pki.certificateFromPem(certCache.ca):null

  const fingerprintCalcule = await hacherCertificat(listePems[0])
  var fingerprintMatch = false
  if(fingerprintCalcule === fingerprint) {
    fingerprintMatch = true
  }
  if( ! fingerprintMatch ) {
    // Supprimer certificat invalide
    await redisClient.del(cleCert)
    throw Error('Fingerprint ' + fingerprintCalcule + ' ne correspond pas a : ' + fingerprint + '. Entree supprimee de redis.')
  }

  // Touch - reset expiration
  await redisClient.expire(cleCert, EXPIRATION_REDIS_CERT)

  return {certificat: chaineForge, ca: caForge}

}

// function str2ab(str) {

//   const buf = new ArrayBuffer(str.length);
//   const bufView = new Uint8Array(buf);
//   for (let i = 0, strLen = str.length; i < strLen; i++) {
//     bufView[i] = str.charCodeAt(i);
//   }
//   return bufView;

// }

class CertificatInconnu extends Error {
  constructor(message) {
    super(message);
    this.inconnu = true;
  }
}

module.exports = { MilleGrillesPKI }
