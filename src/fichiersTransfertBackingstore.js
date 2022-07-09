// Transfert de fichiers vers un backing store
const debug = require('debug')('nodesjs:fichiersTransfertBackingstore')
const fs = require('fs')
const fsPromises = require('fs/promises')
const path = require('path')
const readdirp = require('readdirp')
const https = require('https')
const axios = require('axios')

const { VerificateurHachage } = require('./hachage')

const PATH_STAGING_DEFAUT = '/var/opt/millegrilles/consignation/staging/commun',
      PATH_STAGING_UPLOAD = 'upload',
      PATH_STAGING_READY = 'ready',
      FICHIER_TRANSACTION_CLES = 'transactionCles.json',
      FICHIER_TRANSACTION_CONTENU = 'transactionContenu.json',
      FICHIER_ETAT = 'etat.json',
      INTERVALLE_PUT_CONSIGNATION = 900_000

const CODE_HACHAGE_MISMATCH = 1,
      CODE_CLES_SIGNATURE_INVALIDE = 2,
      CODE_TRANSACTION_SIGNATURE_INVALIDE = 3

let _timerPutFichiers = null,
    _amqpdao = null,
    _urlConsignationTransfert = null,
    _httpsAgent = null,
    _pathStaging = null,
    _consignerFichier = transfererFichierVersConsignation

// Queue de fichiers a telecharger
const _queueItems = []

function configurerThreadPutFichiersConsignation(url, amqpdao, opts) {
    opts = opts || {}

    _urlConsignationTransfert = new URL(''+url)
    _amqpdao = amqpdao

    _pathStaging = opts.PATH_STAGING || PATH_STAGING_DEFAUT
    if(opts.consignerFichier) _consignerFichier = opts.consignerFichier

    // Configurer httpsAgent avec les certificats/cles
    const pki = amqpdao.pki
    const {chainePEM: cert, cle: key } = pki
    if(!cert) throw new Error("fichiersTransfertBackingstore.configurerThreadPutFichiersConsignation Certificat non disponible")
    if(!key) throw new Error("fichiersTransfertBackingstore.configurerThreadPutFichiersConsignation Cle non disponible")
    debug("configurerThreadPutFichiersConsignation _https.Agent cert : %s", '\n' + cert)
    _httpsAgent = new https.Agent({
        rejectUnauthorized: false,
        cert, key,
        ca: pki.ca,
    })

    // Premiere execution apres redemarrage, delai court
    _timerPutFichiers = setTimeout(()=>{
        _timerPutFichiers = null
        _threadPutFichiersConsignation().catch(err=>console.error("Erreur run _threadPutFichiersConsignation: %O", err))
    }, 20_000)

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

        const pathReady = path.join(_pathStaging, PATH_STAGING_READY)

        // Verifier si on a des items dans la Q (prioritaires)
        if(_queueItems.length === 0) {
            debug("_threadPutFichiersConsignation Queue vide, on parcours le repertoire %s", pathReady)
            // Traiter le contenu du repertoire
            const promiseReaddirp = readdirp(pathReady, {
                type: 'directories',
                depth: 1,
            })

            for await (const entry of promiseReaddirp) {
                // debug("Entry path : %O", entry);
                const item = entry.basename
                debug("Traiter PUT pour item %s", item)
                await _consignerFichier(_amqpdao, pathReady, item)
            }
        }

        // Process les items recus durant le traitement
        debug("_threadPutFichiersConsignation Queue avec %d items", _queueItems.length, pathReady)
        while(_queueItems.length > 0) {
            const item = _queueItems.shift()  // FIFO
            debug("Traiter PUT pour item %s", item)
            await _consignerFichier(_amqpdao, pathReady, item)
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
 * Recoit une partie de fichier.
 * Configurer params :position et :correlation dans path expressjs.
 * @param {*} opts 
 * @returns 
 */
function middlewareRecevoirFichier(opts) {
    opts = opts || {}

    // Preparer directories
    const pathStaging = opts.PATH_STAGING || _pathStaging  // PATH_STAGING_DEFAUT
    const pathUpload = path.join(pathStaging, PATH_STAGING_UPLOAD)
    fsPromises.mkdir(pathUpload, {recursive: true, mode: 0o750}).catch(err=>console.error("Erreur preparer path staging upload : %O", err))

    // Retourner fonction middleware pour recevoir un fichier (part)
    return async (req, res, next) => {
        // const {position, correlation} = req.params
        const correlation = req.params.correlation,
              position = req.params.position || 0
        debug("middlewareRecevoirFichier PUT %s position %d", correlation, position)
        
        try {
            await stagingPut(req, correlation, position, opts)
        } catch(err) {
            console.error("middlewareRecevoirFichier Erreur PUT: %O", err)
            return res.sendStatus(500)
        }

        if(opts.chainOnSuccess === true) {
            // Chainage
            debug("middlewareRecevoirFichier chainage next")
            next()
        } else {
            res.sendStatus(200)
        }
    }
}

/**
 * Verifie un fichier et le met dans la Q de transfert interne vers consignation.
 * Verifie et conserve opts.cles et opts.transaction si fournis (optionnels).
 * Appelle next() sur succes, status 500 sur erreur.
 * @param {*} opts 
 *            - successStatus : Code de retour si succes, empeche call next()
 *            - cles : JSON de transaction de cles
 *            - transaction : JSON de transaction de contenu
 *            - writeStream : Conserve le fichier reassemble
 *            - clean(err) : Nettoyage (err : si erreur)
 */
function middlewareReadyFichier(amqpdao, opts) {
    opts = opts || {}
    if(!amqpdao || !amqpdao.pki) throw new Error("Parametre amqpdao ou amqpdao.pki pas initialise")

    // Preparer directories
    const pathStaging = opts.PATH_STAGING || _pathStaging  // PATH_STAGING_DEFAUT
    const pathReadyItem = path.join(pathStaging, PATH_STAGING_READY)
    fsPromises.mkdir(pathReadyItem, {recursive: true, mode: 0o750}).catch(err=>console.error("Erreur preparer path staging ready : %O", err))
    
    return async (req, res, next) => {
        const correlation = req.params.correlation
        const informationFichier = req.body || {}
        debug("middlewareReadyFichier Traitement post %s upload %O", correlation, informationFichier)
      
        const commandeMaitreCles = informationFichier.cles
        const transactionContenu = informationFichier.transaction
        const hachage = commandeMaitreCles?commandeMaitreCles.hachage_bytes:correlation
        
        const optsReady = {...opts, cles: commandeMaitreCles, transaction: transactionContenu}

        try {
            
            await readyStaging(amqpdao, pathStaging, correlation, hachage, optsReady)
            
            if(opts.clean) await opts.clean()

            if(opts.passthroughOnSuccess !== true) {
                return res.status(202).send({ok: true})
            } else {
                return next()
            }

        } catch(err) {
            console.error("middlewareReadyFichier Erreur traitement fichier %s : %O", correlation, err)
            
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
 * Supprime le repertoire de staging (upload et/ou ready)
 * @param {*} opts 
 * @returns 
 */
function middlewareDeleteStaging(opts) {

    const pathStaging = opts.PATH_STAGING || _pathStaging  // PATH_STAGING_DEFAUT

    return async (req, res) => {
        const correlation = req.params.correlation
        try {
            await deleteStaging(pathStaging, correlation)
            res.sendStatus(200)
        } catch(err) {
            console.error("middlewareReadyFichier Erreur traitement fichier %s : %O", correlation, err)
            res.sendStatus(500)
        }
    }
}

async function getPathRecevoir(pathStaging, item, position) {
    const pathUpload = path.join(pathStaging, PATH_STAGING_UPLOAD, item)
    const pathUploadItem = path.join(pathUpload, '' + position + '.part')

    await fsPromises.mkdir(pathUpload, {recursive: true, mode: 0o700})

    return pathUploadItem
}

/**
 * Verifie le contenu de l'upload, des transactions (opts) et transfere le repertoire sous /ready
 * @param {*} pathStaging 
 * @param {*} item 
 * @param {*} hachage 
 * @param {*} opts 
 */
async function readyStaging(amqpdao, pathStaging, item, hachage, opts) {
    opts = opts || {}
    debug("readyStaging item %s, hachage: %s", item, hachage)
    const pki = amqpdao.pki
    const pathUploadItem = path.join(pathStaging, PATH_STAGING_UPLOAD, item)

    if(opts.cles) {
        // On a une commande de maitre des cles. Va etre acheminee et geree par le serveur de consignation.
        let contenu = opts.cles
        // contenu.corrompre = true
        try { await validerMessage(pki, contenu) } 
        catch(err) {
            err.code = CODE_CLES_SIGNATURE_INVALIDE
            throw err
        }

        // Sauvegarder la transaction de cles
        const pathCles = path.join(pathUploadItem, FICHIER_TRANSACTION_CLES)
        if(typeof(contenu) !== 'string') contenu = JSON.stringify(contenu)
        await fsPromises.writeFile(pathCles, contenu, {mode: 0o600})
    }

    if(opts.transaction) {
        // On a une commande de transaction. Va etre acheminee et geree par le serveur de consignation.
        let contenu = opts.transaction
        // contenu.corrompre = true
        try { await validerMessage(pki, contenu) } 
        catch(err) {
            err.code = CODE_TRANSACTION_SIGNATURE_INVALIDE
            throw err
        }

        // Sauvegarder la transaction de contenu
        const pathContenu = path.join(pathUploadItem, FICHIER_TRANSACTION_CONTENU)
        if(typeof(contenu) !== 'string') contenu = JSON.stringify(contenu)
        await fsPromises.writeFile(pathContenu, contenu, {mode: 0o600})
    }

    try {
        await verifierFichier(hachage, pathUploadItem, opts)
    } catch(err) {
        err.code = CODE_HACHAGE_MISMATCH
        throw err
    }

    // Conserver information d'etat/work
    const etat = {
        hachage,
        created: new Date().getTime(),
        lastProcessed: new Date().getTime(),
        retryCount: 0,
    }
    const pathEtat = path.join(pathUploadItem, FICHIER_ETAT)
    await fsPromises.writeFile(pathEtat, JSON.stringify(etat), {mode: 0o600})

    const pathReadyItem = path.join(pathStaging, PATH_STAGING_READY, item)

    try {
        // Tenter un rename de repertoire (rapide)
        await fsPromises.rename(pathUploadItem, pathReadyItem)
    } catch(err) {
        // Echec du rename, on copie le contenu (long)
        console.warn("WARN : Erreur deplacement fichier, on copie : %O", err)
        await fsPromises.cp(pathUploadItem, pathReadyItem, {recursive: true})
        await fsPromises.rm(pathUploadItem, {recursive: true})
    }

    // Fichier pret, on l'ajoute a la liste de transfert
    ajouterFichierConsignation(item)
}

function deleteStaging(pathStaging, item) {
    const pathUploadItem = path.join(pathStaging, PATH_STAGING_UPLOAD, item)
    const pathReadyItem = path.join(pathStaging, PATH_STAGING_READY, item)

    // Ok si une des deux promises reussi
    return Promise.any([
        fsPromises.rm(pathUploadItem, {recursive: true}),
        fsPromises.rm(pathReadyItem, {recursive: true}),
    ])
}

async function validerMessage(pki, message) {
    debug("validerMessage pki : %O", pki)
    debug("validerMessage message : %O", message)
    return await pki.verifierMessage(message)
}

/**
 * Verifier le hachage. Supporte opts.writeStream pour reassembler le fichier en output.
 * @param {*} hachage 
 * @param {*} pathUploadItem 
 * @param {*} opts 
 *            - writeStream : Output stream, ecrit le resultat durant verification du hachage.
 * @returns 
 */
async function verifierFichier(hachage, pathUploadItem, opts) {
    opts = opts || {}

    const verificateurHachage = new VerificateurHachage(hachage)
    const files = await readdirp.promise(pathUploadItem, {fileFilter: '*.part'})

    // Extraire noms de fichiers, cast en Number (pour trier)
    const filesNumero = files.map(file=>{
        return Number(file.path.split('.')[0])
    })

    // Trier en ordre numerique
    filesNumero.sort((a,b)=>{return a-b})

    let total = 0
    for(let idx in filesNumero) {
        const fileNumero = filesNumero[idx]
        debug("Charger fichier %s position %d", pathUploadItem, fileNumero)
        const pathFichier = path.join(pathUploadItem, fileNumero + '.part')
        const fileReader = fs.createReadStream(pathFichier)

        // verificateurHachage.update(Buffer.from([0x1]))  // Corrompre (test)

        fileReader.on('data', chunk=>{
            // Verifier hachage
            verificateurHachage.update(chunk)
            total += chunk.length

            if(opts.writeStream) {
                opts.writeStream.write(chunk)
            }
        })

        const promise = new Promise((resolve, reject)=>{
            fileReader.on('end', _=>{
                try {
                    if(opts.writeStream) { opts.writeStream.close() }
                } catch(err) {
                    console.error("fichiersTransfertBackingstore.verifierFichier ERREUR fermeture writeStream : %O", err)
                }
                resolve()
            })
            fileReader.on('error', err=>reject(err))
        })

        await promise
        debug("Taille cumulative fichier %s : %d", pathUploadItem, total)
    }

    // Verifier hachage - lance une exception si la verification echoue
    await verificateurHachage.verify()
    // Aucune exception, hachage OK

    debug("Fichier correlation %s OK, hachage %s", pathUploadItem, hachage)
    return true
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
        await fsPromises.writeFile(pathEtat, JSON.stringify(etat), {mode: 0o600})
    }
    const hachage = etat.hachage

    // Verifier les transactions (signature)
    const transactions = await traiterTransactions(amqpdao, pathReady, item)

    // Verifier le fichier (hachage)
    await verifierFichier(hachage, pathReadyItem)

    debug("Transactions et fichier OK : %s", pathReadyItem)

    const promiseReaddirp = readdirp(pathReadyItem, {
        type: 'files',
        fileFilter: '*.part',
        depth: 1,
    })
    for await (const entry of promiseReaddirp) {
        // debug("Entry path : %O", entry);
        const fichierPart = entry.basename
        const position = Number(fichierPart.split('.').shift())
        debug("Traiter PUT pour item %s position %d", item, position)
        const streamReader = fs.createReadStream(entry.fullPath)
        await putAxios(_urlConsignationTransfert, item, position, streamReader)
    }

    // Faire POST pour confirmer upload, acheminer transactions
    const urlPost = new URL(''+_urlConsignationTransfert)
    urlPost.pathname = path.join(urlPost.pathname, item)
    const reponsePost = await axios({
        method: 'POST',
        httpsAgent: _httpsAgent,
        url: urlPost.href,
        data: transactions,
    })

    // Le fichier a ete transfere avec succes (aucune exception)
    // On peut supprimer le repertoire ready local
    debug("Fichier %s transfere avec succes vers consignation", item)
    fsPromises.rm(pathReadyItem, {recursive: true})
        .catch(err=>console.error("Erreur suppression repertoire %s apres consignation reussie : %O", item, err))
    
}

async function traiterTransactions(amqpdao, pathReady, item) {
    let transactionContenu = null, transactionCles = null
    const pki = amqpdao.pki

    const transactions = {}

    try {
        const pathEtat = path.join(pathReady, item, FICHIER_ETAT)
        const etat = JSON.parse(await fsPromises.readFile(pathEtat))
        transactions.etat = etat
    } catch(err) {
        console.warn("ERROR durant chargement de l'etat de consignation de %O", item)
    }

    try {
        const pathReadyItemCles = path.join(pathReady, item, FICHIER_TRANSACTION_CLES)
        transactionCles = JSON.parse(await fsPromises.readFile(pathReadyItemCles))
    } catch(err) {
        // Pas de cles
    }
    try {
        const pathReadyItemTransaction = path.join(pathReady, item, FICHIER_TRANSACTION_CONTENU)
        transactionContenu = JSON.parse(await fsPromises.readFile(pathReadyItemTransaction))
    } catch(err) {
        // Pas de transaction de contenu
    }

    if(transactionCles) {
        try { 
            await validerMessage(pki, transactionCles) 
            transactions.cles = transactionCles
        } 
        catch(err) {
            err.code = CODE_CLES_SIGNATURE_INVALIDE
            throw err
        }
    }
    
    if(transactionContenu) {
        try { 
            await validerMessage(pki, transactionContenu) 
            transactions.transaction = transactionContenu
        } 
        catch(err) {
            err.code = CODE_TRANSACTION_SIGNATURE_INVALIDE
            throw err
        }
    }

    return transactions
}

/**
 * PUT un fichier (part) avec axios vers consignationfichiers.
 * @param {*} url 
 * @param {*} uuidCorrelation 
 * @param {*} position 
 * @param {*} dataBuffer 
 */
async function putAxios(url, item, position, dataBuffer) {
    // Emettre POST avec info maitredescles
    const urlPosition = new URL(''+url)
    urlPosition.pathname = path.join(urlPosition.pathname, item, ''+position)
  
    debug("putAxios url %s taille %s", urlPosition, dataBuffer.length)
 
    if(!_httpsAgent) throw new Error("putAxios: httpsAgent n'est pas initialise (utiliser : configurerThreadPutFichiersConsignation)")

    const reponsePut = await axios({
        method: 'PUT',
        httpsAgent: _httpsAgent,
        url: urlPosition.href,
        headers: {'content-type': 'application/stream'},
        data: dataBuffer,
    })
  
    debug("Reponse put %s : %s", urlPosition.href, reponsePut.status)

    return reponsePut
}

// Les methodes suivantes permettent de conserver un fichier localement (simuler upload tiers)
// Utile pour fichiers generes (e.g. transcodage media)

/**
 * Conserver une partie de fichier provenant d'un inputStream (e.g. req)
 * @param {*} inputStream 
 * @param {*} correlation 
 * @param {*} position 
 * @param {*} opts 
 * @returns 
 */
 async function stagingPut(inputStream, correlation, position, opts) {
    opts = opts || {}

    // Preparer directories
    const pathStaging = opts.PATH_STAGING || _pathStaging  // PATH_STAGING_DEFAUT

    // Verifier si le repertoire existe, le creer au besoin
    const pathFichierPut = await getPathRecevoir(pathStaging, correlation, position)
    debug("PUT fichier %s", pathFichierPut)

    // Creer output stream
    const writer = fs.createWriteStream(pathFichierPut)

    if(ArrayBuffer.isView(inputStream)) {
        // Traiter buffer directement
        writer.write(inputStream)
    } else if(typeof(inputStream._read === 'function')) {
        // Assumer stream
        const promise = new Promise((resolve, reject)=>{
            inputStream.on('end', ()=>{ resolve() })
            inputStream.on('error', err=>{ reject(err) })
        })
        inputStream.pipe(writer)
        
        return promise
    } else {
        throw new Error("Type inputstream non supporte")
    }
}

/**
 * Conserver une partie de fichier provenant d'un inputStream (e.g. req)
 * @param {*} inputStream 
 * @param {*} correlation 
 * @param {*} position 
 * @param {*} opts 
 * @returns 
 */
 async function stagingStream(inputStream, correlation, opts) {
    opts = opts || {}
    const TAILLE_SPLIT = opts.TAILLE_SPLIT || 1 * 1024 * 1024

    // Preparer directories
    const pathStaging = opts.PATH_STAGING || _pathStaging  // PATH_STAGING_DEFAUT

    // Verifier si le repertoire existe, le creer au besoin
    let position = 0
    let pathFichierPut = await getPathRecevoir(pathStaging, correlation, position)
    debug("stagingStream PUT fichier %s", pathFichierPut)

    // Creer output stream
    let writer = await fsPromises.open(pathFichierPut, 'w')

    if(typeof(inputStream._read === 'function')) {
        // Assumer stream
        let compteur = 0
        const promise = new Promise((resolve, reject)=>{
            inputStream.on('data', async chunk => {
                inputStream.pause()
                
                if(chunk.length + compteur > TAILLE_SPLIT) {
                    debug("stagingStream split %s", pathFichierPut)
                    const bytesComplete = TAILLE_SPLIT - compteur
                    await writer.write(chunk.slice(0, bytesComplete))  // Ecrire partie du chunk
                    await writer.close()  // Fermer le fichier

                    pathFichierPut = await getPathRecevoir(pathStaging, correlation, position + bytesComplete)
                    debug("stagingStream Ouvrir nouveau fichier %s", pathFichierPut)
                    writer = await fsPromises.open(pathFichierPut, 'w')

                    position += chunk.length       // Aller a la fin du chunk
                    compteur = chunk.length - bytesComplete   // Reset compteur
                    await writer.write(chunk.slice(bytesComplete))  // Continuer a la suite dans le chunk
                } else {
                    debug("stagingStream chunk %d dans %s", chunk.length, pathFichierPut)
                    compteur += chunk.length
                    position += chunk.length
                    await writer.write(chunk)
                }
                
                inputStream.resume()
            })
            inputStream.on('end', async () => { 
                await writer.close()
                resolve()
            })
            inputStream.on('error', err=>{ reject(err) })
        })
        //inputStream.pipe(writer)
        inputStream.read()  // Lancer la lecture
        
        return promise
    } else {
        throw new Error("Type input non supporte")
    }
}

async function stagingReady(amqpdao, transactionContenu, commandeMaitreCles, correlation, opts) {
    opts = opts || {}
    const pathStaging = opts.PATH_STAGING || _pathStaging  // PATH_STAGING_DEFAUT
    const hachage = commandeMaitreCles.hachage_bytes

    const optsReady = {...opts, cles: commandeMaitreCles, transaction: transactionContenu}

    await readyStaging(amqpdao, pathStaging, correlation, hachage, optsReady)
}

async function stagingDelete(correlation, opts) {
    opts = opts || {}
    const pathStaging = opts.PATH_STAGING || _pathStaging  // PATH_STAGING_DEFAUT
    await deleteStaging(pathStaging, correlation)
}

module.exports = { 
    configurerThreadPutFichiersConsignation,

    middlewareRecevoirFichier, 
    middlewareReadyFichier, 
    middlewareDeleteStaging,

    stagingPut, stagingReady, stagingDelete, stagingStream,

    traiterTransactions,
}
