// Transfert de fichiers vers un backing store
const debug = require('debug')('nodesjs:messageQueueBackingStore')
const fsPromises = require('fs/promises')
const path = require('path')
const readdirp = require('readdirp')
const https = require('https')

const PATH_STAGING_DEFAUT = '/var/opt/millegrilles/consignation/messageStaging',
      INTERVALLE_PUT_CONSIGNATION = 300_000

const CODE_HACHAGE_MISMATCH = 1,
      CODE_TRANSACTION_SIGNATURE_INVALIDE = 3

let _timerPutFichiers = null,
    _amqpdao = null,
    _pathStaging = null,
    _cbProcessMessage = null,
    _optsThread = null

// Queue de fichiers a telecharger
const _queueItems = []

function configurerThreadTransfertMessages(amqpdao, cbProcessMessage, opts) {
    opts = opts || {}

    if(!amqpdao) throw new Error("amqdao est vide")
    if(!cbProcessMessage) throw new Error("cbProcessMessage est vide")

    _amqpdao = amqpdao
    _cbProcessMessage = cbProcessMessage
    _optsThread = opts

    _pathStaging = opts.PATH_STAGING || PATH_STAGING_DEFAUT

    // Configurer httpsAgent avec les certificats/cles
    const pki = amqpdao.pki
    _httpsAgent = new https.Agent({
        rejectUnauthorized: false,
        // ca: pki.ca,
        cert: pki.chainePEM,
        key: pki.cle,
    })

    // Premiere execution apres redemarrage, delai court
    _timerPutFichiers = setTimeout(()=>{
        _timerPutFichiers = null
        _threadPutFichiersConsignation().catch(err=>console.error("Erreur run _threadPutFichiersConsignation: %O", err))
    }, 5_000)

}

function ajouterFichierConsignation(item) {
    _queueItems.push(item)
    if(_timerPutFichiers) {
        _threadPutFichiersConsignation().catch(err=>console.error("Erreur run _threadPutFichiersConsignation: %O", err))
    }
}

async function _threadPutFichiersConsignation() {
    try {
        debug(new Date() + " Run threadPutFichiersConsignation")
        // Clear timer si present
        if(_timerPutFichiers) clearTimeout(_timerPutFichiers)
        _timerPutFichiers = null

        // Verifier si on a des items dans la Q (prioritaires)
        if(_queueItems.length === 0) {
            debug("_threadPutFichiersConsignation Queue vide, on parcours le repertoire %s", _pathStaging)
            // Traiter le contenu du repertoire
            const promiseReaddirp = readdirp(_pathStaging, {
                type: 'files',
                filter: '*.json',
                depth: 1,
            })

            for await (const entry of promiseReaddirp) {
                // debug("Entry path : %O", entry);
                const item = entry.basename
                debug("Traiter PUT pour item %s", item)
                await transfererFichierVersConsignation(_amqpdao, _pathStaging, item, _cbProcessMessage, _optsThread)
            }
        }

        // Process les items recus durant le traitement
        debug("_threadPutFichiersConsignation Queue avec %d items", _queueItems.length, _pathStaging)
        while(_queueItems.length > 0) {
            let item = _queueItems.shift()  // FIFO
            debug("Traiter PUT pour item %s", item)
            if(!item.endsWith('.json')) item = item + '.json'
            await transfererFichierVersConsignation(_amqpdao, _pathStaging, item, _cbProcessMessage, _optsThread)
        }

    } catch(err) {
        console.error(new Date() + ' _threadPutFichiersConsignation Erreur execution cycle : %O', err)
    } finally {
        _traitementPutFichiersEnCours = false
        debug("_threadPutFichiersConsignation Fin execution cycle, attente %s ms", INTERVALLE_PUT_CONSIGNATION)
        // Redemarrer apres intervalle
        _timerPutFichiers = setTimeout(()=>{
            _timerPutFichiers = null
            _threadPutFichiersConsignation().catch(err=>console.error("Erreur run _threadPutFichiersConsignation: %O", err))
        }, INTERVALLE_PUT_CONSIGNATION)

    }
}

/**
 * Verifie un message et le met dans la Q de transfert interne.
 * Appelle next() sur succes, status 500 sur erreur.
 * @param {*} opts 
 *            - successStatus : Code de retour si succes, empeche call next()
 */
function middlewareRecevoirMessage(opts) {
    opts = opts || {}

    // Preparer directories
    const pathStaging = opts.PATH_STAGING || PATH_STAGING_DEFAUT
    fsPromises.mkdir(pathStaging, {recursive: true, mode: 0o750}).catch(err=>console.error("Erreur preparer path staging ready : %O", err))
    
    return async (req, res, next) => {
        const amqpdao = req.amqpdao
       
        try {
            const transaction = req.body || {}
            const uuid_transaction = transaction['en-tete'].uuid_transaction
            debug("middlewareReadyFichier Traitement post %s transferer\n%O", uuid_transaction, transaction)

            await conserverMessageStaging(amqpdao, pathStaging, transaction, opts)
            
            if(opts.successStatus) {
                return res.status(opts.successStatus).send({ok: true})
            } else {
                return next()
            }

        } catch(err) {
            console.error("middlewareReadyFichier Erreur traitement fichier %O", err)
            
            // Tenter cleanup
            try { if(opts.clean) await opts.clean(err) } 
            catch(err) { console.error("middlewareReadyFichier Erreur clean %s : %O", err) }

            switch(err.code) {
                case CODE_HACHAGE_MISMATCH:
                    return res.send({ok: false, err: 'HACHAGE MISMATCH', code: err.code})
                case CODE_CLES_SIGNATURE_INVALIDE:
                    return res.send({ok: false, err: 'CLES SIGNATURE INVALIDE', code: err.code})
                case CODE_TRANSACTION_SIGNATURE_INVALIDE:
                    return res.send({ok: false, err: 'TRANSACTION SIGNATURE INVALIDE', code: err.code})
            }

            res.status(500).send({ok: false, err: ''+err})
        }
    }
}

/**
 * Verifie le contenu de l'upload, des transactions (opts) et transfere le repertoire sous /ready
 * @param {*} pathStaging 
 * @param {*} item 
 * @param {*} hachage 
 * @param {*} opts 
 */
async function conserverMessageStaging(amqpdao, pathStaging, transaction, opts) {
    opts = opts || {}
    const pki = amqpdao.pki
    const uuid_transaction = transaction['en-tete'].uuid_transaction

    debug("readyStaging item %s", uuid_transaction)

    // contenu.corrompre = true
    if(opts.novalid !== true) {
        try { await pki.verifierMessage(transaction) } 
        catch(err) {
            err.code = CODE_TRANSACTION_SIGNATURE_INVALIDE
            throw err
        }
    }

    // Sauvegarder la transaction de contenu
    const pathContenu = path.join(pathStaging, uuid_transaction + '.json')
    await fsPromises.writeFile(pathContenu, JSON.stringify(transaction), {mode: 0o600})

    // Fichier pret, on l'ajoute a la liste de transfert
    ajouterFichierConsignation(uuid_transaction)
}

async function transfererFichierVersConsignation(amqpdao, pathReady, item, cb, opts) {
    const pathReadyItem = path.join(pathReady, item)
    debug("Traiter transfert vers consignation de %s", pathReadyItem)

    // Verifier les transactions (signature)
    const transaction = JSON.parse(await fsPromises.readFile(pathReadyItem))

    if(opts.novalid !== true) {
        const pki = amqpdao.pki
        await pki.verifierMessage(transaction)        
        debug("Transaction OK : %s", pathReadyItem)
    }
    
    // Callback pour traiter la transaction
    await cb(transaction)

    // Le fichier a ete transfere avec succes (aucune exception)
    // On peut supprimer le repertoire ready local
    debug("Fichier %s transfere avec succes vers consignation", item)
    fsPromises.rm(pathReadyItem)
        .catch(err=>console.error("Erreur suppression repertoire %s apres consignation reussie : %O", item, err))
    
}

module.exports = { configurerThreadTransfertMessages, middlewareRecevoirMessage }
