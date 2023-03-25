// Transfert de fichiers entre serveurs vers consignation
const debug = require('debug')('nodesjs:fichiersTransfertUpstream')
const fs = require('fs')
const fsPromises = require('fs/promises')
const path = require('path')
const readdirp = require('readdirp')
const https = require('node:https')
const axios = require('axios')

const { VerificateurHachage } = require('./hachage')

const PATH_STAGING_DEFAUT = '/var/opt/millegrilles/consignation/staging/commun',
    PATH_STAGING_READY = 'ready',
    FICHIER_TRANSACTION_CLES = 'transactionCles.json',
    FICHIER_TRANSACTION_CONTENU = 'transactionContenu.json',
    FICHIER_ETAT = 'etat.json',
    INTERVALLE_PUT_CONSIGNATION = 900_000,
    CONST_TAILLE_SPLIT_MAX_DEFAULT = 100 * 1024 * 1024,
    // CONST_TAILLE_SPLIT_MAX_DEFAULT = 5 * 1024 * 1024,
    CONST_INTERVALLE_REFRESH_URL = 900_000

var _timerPutFichiers = null,
    _amqpdao = null,
    _urlConsignationTransfert = null,
    _disableRefreshUrlTransfert = false,
    _urlTimestamp = 0,
    _httpsAgent = null,
    _pathStaging = PATH_STAGING_DEFAUT,
    _consignerFichier = transfererFichierVersConsignation,
    _primaire = false,
    _instance_id_consignationTransfer = null,
    _tailleMaxTransfert = CONST_TAILLE_SPLIT_MAX_DEFAULT

const _queueItems = []

function configurerThreadPutFichiersConsignation(amqpdao, opts) {
    opts = opts || {}
    _amqpdao = amqpdao
    _primaire = opts.primaire || false

    // Option pour indiquer que le URL de transfert est statique
    _disableRefreshUrlTransfert = opts.DISABLE_REFRESH || false

    try {
        const url = opts.url
        if (url) _urlConsignationTransfert = new URL('' + url)
    } catch (err) {
        console.error("Erreur configuration URL upload : ", err)
        if (_disableRefreshUrlTransfert) throw err  // L'URL est invalide et on ne doit pas le rafraichir
    } finally {
        // Tenter chargement initial
        if (!_disableRefreshUrlTransfert) {
            chargerUrlRequete({ primaire: true })
                .then(reponse => {
                    debug("Chargement initial URL transfert : ", reponse)
                    _urlConsignationTransfert = reponse.url
                    _instance_id_consignationTransfer = reponse.instance_id
                })
                .catch(err => console.warn("configurerThreadPutFichiersConsignation Erreur chargement initial URL transfert ", err))
        }
    }

    _pathStaging = opts.PATH_STAGING || PATH_STAGING_DEFAUT
    if (opts.consignerFichier) _consignerFichier = opts.consignerFichier

    // Configurer httpsAgent avec les certificats/cles
    const pki = amqpdao.pki
    const { chainePEM: cert, cle: key, ca } = pki
    if (!cert) throw new Error("fichiersTransfertBackingstore.configurerThreadPutFichiersConsignation Certificat non disponible")
    if (!key) throw new Error("fichiersTransfertBackingstore.configurerThreadPutFichiersConsignation Cle non disponible")
    debug("configurerThreadPutFichiersConsignation _https.Agent cert : \n%s\n%s\n%s", cert, key, ca)
    _httpsAgent = new https.Agent({
        //keepAlive: true,
        //maxSockets: 10,
        rejectUnauthorized: false,
        cert, key,
        ca,
    })

    // Premiere execution apres redemarrage, delai court
    _timerPutFichiers = setTimeout(() => {
        _timerPutFichiers = null
        _threadPutFichiersConsignation().catch(err => console.error("Erreur run _threadPutFichiersConsignation: %O", err))
    }, 20_000)

}

function ajouterFichierConsignation(item) {
    _queueItems.push(item)
    if (_timerPutFichiers) {
        _threadPutFichiersConsignation().catch(err => console.error("Erreur run _threadPutFichiersConsignation: %O", err))
    }
}

async function _threadPutFichiersConsignation() {
    try {
        debug(new Date() + " Run threadPutFichiersConsignation")
        // Clear timer si present
        if (_timerPutFichiers) clearTimeout(_timerPutFichiers)
        _timerPutFichiers = null

        const dateUrlExpire = new Date().getTime() - CONST_INTERVALLE_REFRESH_URL
        if (!_disableRefreshUrlTransfert && (!_urlConsignationTransfert || _urlTimestamp < dateUrlExpire)) {
            // Url consignation vide, on fait une requete pour la configuration initiale
            try {
                const reponse = await chargerUrlRequete({ primaire: true })
                _urlConsignationTransfert = reponse.url
                _instance_id_consignationTransfer = reponse.instance_id
            } catch (err) {
                console.error("Erreur reload URL transfert fichiers : ", err)
                if (!_urlConsignationTransfert) throw err    // Aucun URL par defaut
            }
            _urlTimestamp = new Date().getTime()
            debug("Nouveau URL transfert fichiers : ", _urlConsignationTransfert)
        }

        const pathReady = path.join(_pathStaging, PATH_STAGING_READY)

        // Verifier si on a des items dans la Q (prioritaires)
        if (_queueItems.length === 0) {
            debug("_threadPutFichiersConsignation Queue vide, on parcours le repertoire %s", pathReady)
            // Traiter le contenu du repertoire
            const promiseReaddirp = readdirp(pathReady, {
                type: 'directories',
                depth: 1,
            })

            for await (const entry of promiseReaddirp) {
                // debug("Entry path : %O", entry);
                const batchId = entry.basename
                debug("Traiter PUT pour item %s", batchId)
                await transfererBatchVersConsignation(_amqpdao, pathReady, batchId)
            }
        }

        // Process les items recus durant le traitement
        debug("_threadPutFichiersConsignation Queue avec %d items", _queueItems.length, pathReady)
        while (_queueItems.length > 0) {
            const batchId = _queueItems.shift()  // FIFO
            debug("Traiter PUT pour batchId %s", batchId)
            await transfererBatchVersConsignation(_amqpdao, pathReady, batchId)
        }

    } catch (err) {
        console.error(new Date() + ' _threadPutFichiersConsignation Erreur execution cycle : %O', err)
    } finally {
        _traitementPutFichiersEnCours = false
        debug("_threadPutFichiersConsignation Fin execution cycle, attente %s ms", INTERVALLE_PUT_CONSIGNATION)
        // Redemarrer apres intervalle
        _timerPutFichiers = setTimeout(() => {
            _timerPutFichiers = null
            _threadPutFichiersConsignation().catch(err => console.error("Erreur run _threadPutFichiersConsignation: %O", err))
        }, INTERVALLE_PUT_CONSIGNATION)

    }
}

async function chargerUrlRequete(opts) {
    opts = opts || {}
    const primaire = opts.primaire || false
    const requete = { primaire }

    if (!_amqpdao) throw new Error("_amqpdao absent")

    const reponse = await _amqpdao.transmettreRequete(
        'CoreTopologie',
        requete,
        { action: 'getConsignationFichiers', exchange: '2.prive', attacherCertificat: true }
    )

    if (!reponse.ok) {
        throw new Error("Erreur configuration URL transfert (reponse MQ): ok = false")
    }

    const { instance_id, consignation_url } = reponse

    const consignationURL = new URL(consignation_url)
    consignationURL.pathname = '/fichiers_transfert'

    debug("Consignation URL : %s", consignationURL.href)

    return { url: consignationURL.href, instance_id }
}

async function transfererBatchVersConsignation(amqpdao, pathReady, batchId) {
    const pathBatch = path.join(pathReady, batchId)
    const promiseReaddirp = readdirp(pathBatch, {
        type: 'directories',
        depth: 1,
    })

    for await (const entry of promiseReaddirp) {
        // debug("Entry path : %O", entry);
        const correlation = entry.basename
        debug("Traiter PUT pour correlation %s", correlation)
        await _consignerFichier(amqpdao, pathBatch, correlation)
    }

    debug("Batch %s transfere avec succes vers consignation", batchId)
    fsPromises.rm(pathBatch, { recursive: true })
        .catch(err => console.error("Erreur suppression repertoire %s apres consignation reussie : %O", pathBatch, err))
}

async function transfererFichierVersConsignation(amqpdao, pathReady, item) {
    const pathReadyItem = path.join(pathReady, item)
    debug("Traiter transfert vers consignation de %s", pathReadyItem)

    let etat = null
    {
        // Charger etat et maj dates/retry count
        const pathEtat = path.join(pathReadyItem, FICHIER_ETAT)
        etat = JSON.parse(await fsPromises.readFile(pathEtat))
        etat.lastProcessed = new Date().getTime()
        etat.retryCount++
        await fsPromises.writeFile(pathEtat, JSON.stringify(etat), { mode: 0o600 })
    }
    const hachage = etat.hachage

    // Verifier les transactions (signature)
    //const transactions = await traiterTransactions(amqpdao, pathReady, item)

    // Verifier le fichier (hachage)
    //await verifierFichier(hachage, pathReadyItem)

    const pathFichier = path.join(pathReadyItem, hachage),
          infoFichier = await fsPromises.stat(pathFichier)
    
    debug("Transactions et fichier OK : %s, %O", pathReadyItem, infoFichier)
    let tailleFichier = infoFichier.size

    for(let position=0; position<tailleFichier; position += _tailleMaxTransfert) {
        const endPosition = Math.min(position+_tailleMaxTransfert, tailleFichier) - 1  // position end inclusive

        const readStream = fs.createReadStream(pathFichier, { start: position, end: endPosition })

        const contentLength = endPosition - position + 1
        const urlPosition = new URL(''+_urlConsignationTransfert)
        urlPosition.pathname = path.join(urlPosition.pathname, item, ''+position)
        debug("transfererFichierVersConsignation url %s taille %s", urlPosition, contentLength)
     
        if(!_httpsAgent) throw new Error("putAxios: httpsAgent n'est pas initialise (utiliser : configurerThreadPutFichiersConsignation)")
    
        try {
            await axios({
                method: 'PUT',
                httpsAgent: _httpsAgent,
                maxRedirects: 0,
                url: urlPosition.href,
                headers: {
                    'content-length': 4,
                    'content-type': 'application/stream'
                },
                data: readStream,
                // data: 'Allo',
                timeout: 600_000,
            })
        } catch(err) {
            const response = err.response || {}
            const status = response.status
            console.error("Erreur PUT fichier (status %d) %O", status, err)
            if (status === 409 && response.headers['x-position']) {
                position = response.headers['x-position']
            } else {
                throw err
            }
        }
    }

    // Faire POST pour confirmer upload, acheminer transactions
    const urlPost = new URL('' + _urlConsignationTransfert)
    urlPost.pathname = path.join(urlPost.pathname, item)
    const reponsePost = await axios({
        method: 'POST',
        httpsAgent: _httpsAgent,
        url: urlPost.href,
        // data: transactions,
    })

    // Le fichier a ete transfere avec succes (aucune exception)
    // On peut supprimer le repertoire ready local
    debug("Fichier %s transfere avec succes vers consignation, reponse %O", item, reponsePost)
    fsPromises.rm(pathReadyItem, { recursive: true })
        .catch(err => console.error("Erreur suppression repertoire %s apres consignation reussie : %O", item, err))

    // if (_primaire === true) {
    //     // Emettre un message
    //     await evenementFichierPrimaire(hachage)
    // }

}

module.exports = {
    configurerThreadPutFichiersConsignation,
    ajouterFichierConsignation,
}
