// Middleware pour reception de fichiers des clients
const fs = require('fs')
const fsPromises = require('fs/promises')
const path = require('path')
const readdirp = require('readdirp')

const { VerificateurHachage } = require('./hachage')

const debug = require('debug')('nodesjs:fichiersTransfert')

const PATH_STAGING_DEFAUT = '/var/opt/millegrilles/consignation/staging/commun',
      PATH_STAGING_UPLOAD = 'upload',
      PATH_STAGING_READY = 'ready',
      FICHIER_TRANSACTION_CLES = 'transactionCles.json',
      FICHIER_TRANSACTION_CONTENU = 'transactionContenu.json',
      FICHIER_ETAT = 'etat.json',
      CONST_EXPIRATION_UPLOAD = 86_400_000,
      CONST_INTERVALLE_ENTRETIEN = 3_600_000

const CODE_HACHAGE_MISMATCH = 1,
      CODE_CLES_SIGNATURE_INVALIDE = 2,
      CODE_TRANSACTION_SIGNATURE_INVALIDE = 3

class FichiersMiddleware {

    constructor(mq, opts) {
        opts = opts || {}

        if(!mq || !mq.pki) throw new Error("Parametre mq ou mq.pki pas initialise")

        this._mq = mq
        this._pathStaging = opts.PATH_STAGING || PATH_STAGING_DEFAUT

        this._intervalCleanup = setInterval(()=>{
            this.cleanupUpload()
                .catch(err=>console.error(new Date() + ' FichiersMiddleware ERROR Erreur cleanup upload', err))
        }, CONST_INTERVALLE_ENTRETIEN)
    
        this.cleanupUpload()
            .catch(err=>console.error(new Date() + ' FichiersMiddleware ERROR Erreur cleanup upload', err))
    }

    /**
     * Recoit une partie de fichier.
     * Configurer params :position et :correlation dans path expressjs.
     * @param {*} opts 
     * @returns 
     */
    middlewareRecevoirFichier(opts) {
        opts = opts || {}

        // Preparer directories
        const pathUpload = path.join(this._pathStaging, PATH_STAGING_UPLOAD)
        fsPromises.mkdir(pathUpload, {recursive: true, mode: 0o750})
            .catch(err=>console.error("Erreur preparer path staging upload : %O", err))

        // Retourner fonction middleware pour recevoir un fichier (part)
        return (req, res, next) => this.middlewareRecevoirFichierHandler(req, res, next, opts)
    }

    async middlewareRecevoirFichierHandler(req, res, next, opts) {
        opts = opts || {}
        debug("middlewareRecevoirFichierHandler DEBUG THIS ", this)
        const batchId = req.batchId
        if(!batchId) throw new Error('SERVER ERROR - batchId doit etre fournis dans req.batchId')

        const { correlation } = req.params
        const position = req.params.position || 0,
              fuuid = req.headers['x-fuuid'],
              hachagePart = req.headers['x-content-hash'],
              tokenSrc = req.jwtsrc
        debug("middlewareRecevoirFichier PUT fuuid %ss : %s/%s position %d (token %s)", fuuid, batchId, correlation, position, tokenSrc)
        
        if(!correlation) {
            debug("middlewareRecevoirFichier ERREUR correlation manquant")
            return res.sendStatus(400)
        }

        try {
            await stagingPut(this._pathStaging, req, batchId, correlation, position, {...opts, token: tokenSrc, hachagePart})
        } catch(err) {
            console.error("middlewareRecevoirFichier Erreur PUT: %O", err)
            const response = err.response
            if(response) {
                if(response.headers) {
                    for (const name of Object.keys(response.headers)) {
                        res.setHeader(name, response.headers[name])
                    }
                }
                if(response.status) res.status(response.status)
                return res.send(response.data)
            }
            return res.sendStatus(500)
        }

        if(opts.chainOnSuccess === true) {
            // Chainage
            debug("middlewareRecevoirFichier chainage next")
            next()
        } else {
            res.send({ok: true})
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
    middlewareReadyFichier(opts) {
        opts = opts || {}

        // // Preparer directories
        // const pathStaging = this._pathStaging
        // const pathReadyItem = path.join(pathStaging, PATH_STAGING_READY)
        // fsPromises.mkdir(pathReadyItem, {recursive: true, mode: 0o750})
        //     .catch(err=>console.error("Erreur preparer path staging ready : %O", err))
        
        return (req, res, next) => this.middlewareReadyFichierHandler(req, res, next, opts)
    }

    async middlewareReadyFichierHandler(req, res, next, opts) {
        opts = opts || {}
        const batchId = req.batchId
        if(!batchId) throw new Error('SERVER ERROR - batchId doit etre fournis dans req.batchId')

        const { correlation } = req.params
        const informationFichier = req.body || {}
        debug("middlewareReadyFichier Traitement post %s/%s upload %O", batchId, correlation, informationFichier)
    
        const commandeMaitreCles = informationFichier.cles
        const transactionContenu = informationFichier.transaction
        const etat = informationFichier.etat || {},
            hachage = etat.hachage || correlation
        
        const optsReady = {...opts, cles: commandeMaitreCles, transaction: transactionContenu}

        try {
            
            await readyStaging(this._mq, this._pathStaging, batchId, correlation, hachage, optsReady)

            if(opts.clean) await opts.clean()

            if(opts.passthroughOnSuccess !== true) {
                return res.status(202).send({ok: true})
            } else {
                res.hachage = hachage
                res.transaction = transactionContenu
                return next()
            }

        } catch(err) {
            console.error("middlewareReadyFichier Erreur traitement fichier %s/%s : %O", batchId, correlation, err)
            
            // Tenter cleanup
            try { 
                const pathCorrelation = path.join(this._pathStaging, PATH_STAGING_UPLOAD, batchId, correlation)
                await fsPromises.rm(pathCorrelation, {recursive: true})
                if(opts.clean) await opts.clean(err) 
            } 
            catch(err) { console.error("middlewareReadyFichier Erreur clean %s : %O", err) }

            switch(err.code) {
                case CODE_HACHAGE_MISMATCH:
                    return res.status(400).send({ok: false, err: 'HACHAGE MISMATCH', code: err.code})
                case CODE_CLES_SIGNATURE_INVALIDE:
                    return res.status(400).send({ok: false, err: 'CLES SIGNATURE INVALIDE', code: err.code})
                case CODE_TRANSACTION_SIGNATURE_INVALIDE:
                    return res.status(400).send({ok: false, err: 'TRANSACTION SIGNATURE INVALIDE', code: err.code})
            }

            res.status(500).send({ok: false, err: ''+err})
        }
    }

    /**
     * Supprime le repertoire de staging (upload et/ou ready)
     * @param {*} opts 
     * @returns 
     */
    middlewareDeleteStaging(opts) {
        opts = opts || {}
        return (req, res, next) => this.middlewareDeleteStagingHandler(req, res, next, opts)        
    }

    async middlewareDeleteStagingHandler(req, res, next, opts) {
        opts = opts || {}
        const batchId = req.batchId
        if(!batchId) throw new Error('SERVER ERROR - batchId doit etre fournis dans req.batchId')

        const { correlation } = req.params

        try {
            if(correlation) {
                await deleteFichierStaging(this._pathStaging, batchId, correlation)
            } else {
                await deleteBatchStaging(this._pathStaging, batchId)
            }
            return res.sendStatus(200)
        } catch(err) {
            console.error("middlewareReadyFichier Erreur traitement fichier %s/%s : %O", batchId, correlation, err)
            return res.sendStatus(500)
        }
    }

    getPathBatch(batchId) {
        return path.join(this._pathStaging, PATH_STAGING_UPLOAD, batchId)
    }

    async cleanupUpload() {
        const pathUpload = path.join(this._pathStaging, PATH_STAGING_UPLOAD)
        debug("Entretien du repertoire staging upload %s", pathUpload)

        const promiseReaddirp = readdirp(pathUpload, {
            type: 'directories',
            depth: 1,
            alwaysStat: true,
        })
    
        const expiration = new Date().getTime() - CONST_EXPIRATION_UPLOAD

        for await (const entry of promiseReaddirp) {
            debug("Entry path : %O", entry);
            const mtimeMs = entry.stats.mtimeMs
            if(mtimeMs < expiration) {
                debug("Entry upload expire - on supprime ", entry.fullPath)
                await fsPromises.rm(entry.fullPath, {recursive: true})
            }
        }
    }

}

async function getPathRecevoir(pathStaging, batchId, correlation, position) {
    const pathUpload = path.join(pathStaging, PATH_STAGING_UPLOAD, batchId, correlation)
    const pathUploadItem = path.join(pathUpload, '' + position + '.part')

    debug("getPathRecevoir mkdir path %s", pathUpload)
    await fsPromises.mkdir(pathUpload, {recursive: true, mode: 0o700})

    return pathUploadItem
}

async function majFichierEtatUpload(pathStaging, batchId, correlation, data) {
    const pathFichierEtat = path.join(pathStaging, PATH_STAGING_UPLOAD, batchId, correlation, FICHIER_ETAT)
    
    const contenuFichierStatusString = await fsPromises.readFile(pathFichierEtat)
    const contenuCourant = JSON.parse(contenuFichierStatusString)
    Object.assign(contenuCourant, data)
    await fsPromises.writeFile(pathFichierEtat, JSON.stringify(contenuCourant))

    return contenuCourant
}

/**
 * Conserver une partie de fichier provenant d'un inputStream (e.g. req)
 * @param {*} inputStream 
 * @param {*} correlation 
 * @param {*} position 
 * @param {*} opts 
 * @returns 
 */
async function stagingPut(pathStaging, inputStream, batchId, correlation, position, opts) {
    opts = opts || {}
    const hachagePart = opts.hachagePart
    if(typeof(position) === 'string') position = Number.parseInt(position)

    // Verifier si le repertoire existe, le creer au besoin
    const pathFichierPut = await getPathRecevoir(pathStaging, batchId, correlation, position)
    debug("PUT fichier %s", pathFichierPut)

    let verificateurHachage = null
    if(hachagePart) {
        verificateurHachage = new VerificateurHachage(hachagePart)
    }

    if(opts.token) {
        const dirToken = path.parse(path.parse(pathFichierPut).dir).dir
        const pathToken = dirToken + '/token.txt'
        fsPromises.writeFile(pathToken, opts.token, {flag: 'wx'})
            .catch(err=>{
                if(err.code === 'EEXIST') return // Ok, token deja sauvegarde
                debug("stagingPut Erreur sauvegarde token : ", err)
            })
    }

    const contenuStatus = await getFicherEtatUpload(pathStaging, batchId, correlation)
    debug("stagingPut Status upload courant : ", contenuStatus)

    if(contenuStatus.position != position && position !== 0) {
        debug("stagingPut Detecte resume fichier avec mauvaise position, on repond avec position courante")
        const err = new Error("stagingPut Detecte resume fichier, on repond avec position courante")
        err.response = {
            status: 409,
            headers: {'x-position': contenuStatus.position},
            json: {position: contenuStatus.position}
        }
        throw err
    } else if(position === 0) {
        debug("stagingPut Reset upload %s/%s a 0", batchId, correlation)
        await fsPromises.rm(path.join(pathStaging, batchId, correlation, '*.work'), {force: true})
        await fsPromises.rm(path.join(pathStaging, batchId, correlation, '*.part.work'), {force: true})
        contenuStatus.position = 0
    }

    // Creer output stream
    const pathFichierPutWork = pathFichierPut + '.work'
    const writer = fs.createWriteStream(pathFichierPutWork)
    debug("stagingPut Conserver fichier work upload ", pathFichierPutWork)

    if(ArrayBuffer.isView(inputStream)) {
        // Traiter buffer directement
        // writer.write(inputStream)
        if(verificateurHachage) {
            verificateurHachage.update(inputStream)
            try {
                await verificateurHachage.verify()  // Lance erreur si hachage invalide
            } catch(err) {
                debug("Erreur de hachage ", err)
                fsPromises.unlink(pathFichierPut).catch(err=>{
                    console.error("Erreur delete part incomplet %s : %O", pathFichierPut, err)
                })
                err.response = {status: 400, data: {ok: false, err: ''+err, code: 'Hash mismatch'}}
                throw err
            }
            debug("Hachage part OK")
        }
        await new Promise((resolve, reject)=>{
            writer.on('close', resolve)
            writer.on('error', err=>{ 
                fsPromises.unlink(pathFichierPut).catch(err=>{
                    console.error("Erreur delete part incomplet %s : %O", pathFichierPut, err)
                })
                reject(err)
            })
            writer.write(inputStream)
            writer.close()
        })        

        const nouvellePosition = inputStream.length + contenuStatus.position
        await majFichierEtatUpload(pathStaging, batchId, correlation, {position: nouvellePosition})
        await fsPromises.rename(pathFichierPutWork, pathFichierPut)

    } else if(typeof(inputStream._read === 'function')) {
        // Assumer stream
        let compteurTaille = 0
        const promise = new Promise((resolve, reject)=>{
            // Hook du writer pour fermer normalement (resolve)
            writer.on('close', async () => {
                // Resultat OK
                const nouvellePosition = compteurTaille + contenuStatus.position
                if(verificateurHachage) {
                    try {
                        await verificateurHachage.verify()
                    } catch(err) {
                        debug("Erreur de hachage ", err)
                        fsPromises.unlink(pathFichierPutWork).catch(err=>{
                            console.error("Erreur delete part incomplet %s : %O", pathFichierPutWork, err)
                        })
                        err.response = {status: 400, data: {ok: false, err: ''+err, code: 'fichierMiddleware Hash mismatch'}}
                        return reject(err)
                    }
    
                    debug("Hachage part OK")
                }
                await majFichierEtatUpload(pathStaging, batchId, correlation, {position: nouvellePosition})
                    .then(()=>{
                        debug("stagingPut Rename fichier work vers ", pathFichierPut)
                        return fsPromises.rename(pathFichierPutWork, pathFichierPut)
                    })
                    .then(()=>resolve())
                    .catch(err=>reject(err))
            })

            inputStream.on('data', chunk=>{ 
                compteurTaille += chunk.length
                if(verificateurHachage) {
                    verificateurHachage.update(chunk)
                }
                return chunk
            })

            // Cas d'erreur
            writer.on('error', err => { 
                fsPromises.unlink(pathFichierPut).catch(err=>{
                    console.error("Erreur delete part incomplet %s : %O", pathFichierPut, err)
                })
                reject(err)
            })
            inputStream.on('error', err => { 
                fsPromises.unlink(pathFichierPut).catch(err=>{
                    console.error("Erreur delete part incomplet %s : %O", pathFichierPut, err)
                })
                reject(err)
            })

            inputStream.on('end', () => { 
                debug("Inpustream end OK")
                // // Resultat OK
                // const nouvellePosition = compteurTaille + contenuStatus.position
                // if(verificateurHachage) {
                //     try {
                //         await verificateurHachage.verify()
                //     } catch(err) {
                //         debug("Erreur de hachage ", err)
                //         fsPromises.unlink(pathFichierPut).catch(err=>{
                //             console.error("Erreur delete part incomplet %s : %O", pathFichierPut, err)
                //         })
                //         err.response = {status: 400, data: {ok: false, err: ''+err, code: 'Hash mismatch'}}
                //         return reject(err)
                //     }
    
                //     debug("Hachage part OK")
                // }
                // await majFichierEtatUpload(pathStaging, batchId, correlation, {position: nouvellePosition})
                //     .then(()=>{
                //         debug("stagingPut Rename fichier work vers ", pathFichierPut)
                //         return fsPromises.rename(pathFichierPutWork, pathFichierPut)
                //     })
                //     .then(()=>resolve())
                //     .catch(err=>reject(err))
            })
        })
        inputStream.pipe(writer)
        
        return promise
    } else {
        throw new Error("Type inputstream non supporte")
    }
}

/**
 * Verifie le contenu de l'upload, des transactions (opts) et transfere le repertoire sous /ready
 * @param {*} pathStaging 
 * @param {*} item 
 * @param {*} hachage 
 * @param {*} opts 
 */
async function readyStaging(amqpdao, pathStaging, batchId, correlation, hachage, opts) {
    opts = opts || {}
    debug("readyStaging item %s/%s, hachage: %s", batchId, correlation, hachage)
    const pki = amqpdao.pki
    const pathUploadItem = path.join(pathStaging, PATH_STAGING_UPLOAD, batchId, correlation)

    if(opts.cles) {
        // On a une commande de maitre des cles. Va etre acheminee et geree par le serveur de consignation.
        let contenu = opts.cles
        // contenu.corrompre = true
        try { await pki.verifierMessage(contenu)} 
        catch(err) {
            debug("readyStaging ERROR readyStaging Message cles invalide")
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
        try { await pki.verifierMessage(contenu) } 
        catch(err) {
            debug("readyStaging ERROR readyStaging Message transaction invalide")
            err.code = CODE_TRANSACTION_SIGNATURE_INVALIDE
            throw err
        }

        // Sauvegarder la transaction de contenu
        const pathContenu = path.join(pathUploadItem, FICHIER_TRANSACTION_CONTENU)
        if(typeof(contenu) !== 'string') contenu = JSON.stringify(contenu)
        await fsPromises.writeFile(pathContenu, contenu, {mode: 0o600})
    }

    try {
        const pathOutput = path.join(pathUploadItem, ''+hachage)
        const writeStream = fs.createWriteStream(pathOutput)
        await verifierFichier(hachage, pathUploadItem, {...opts, writeStream, deleteParts: true})
    } catch(err) {
        debug("readyStaging ERROR Fichier hachage mismatch")
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

}

function deleteFichierStaging(pathStaging, batchId, correlation) {
    const pathUploadItem = path.join(pathStaging, PATH_STAGING_UPLOAD, batchId, correlation)
    const pathReadyItem = path.join(pathStaging, PATH_STAGING_READY, batchId, correlation)

    // Ok si une des deux promises reussi
    return Promise.any([
        fsPromises.rm(pathUploadItem, {recursive: true}),
        fsPromises.rm(pathReadyItem, {recursive: true}),
    ])
}

function deleteBatchStaging(pathStaging, batchId) {
    const pathUploadItem = path.join(pathStaging, PATH_STAGING_UPLOAD, batchId)
    const pathReadyItem = path.join(pathStaging, PATH_STAGING_READY, batchId)

    // Ok si une des deux promises reussi
    return Promise.any([
        fsPromises.rm(pathUploadItem, {recursive: true}),
        fsPromises.rm(pathReadyItem, {recursive: true}),
    ])
}

async function getFicherEtatUpload(pathStaging, batchId, correlation) {
    const pathFichierEtat = path.join(pathStaging, PATH_STAGING_UPLOAD, batchId, correlation, FICHIER_ETAT)

    try {
        const contenuFichierStatusString = await fsPromises.readFile(pathFichierEtat)
        return JSON.parse(contenuFichierStatusString)
    } catch(err) {
        // Fichier n'existe pas, on le genere
        const contenu = {
            "creation": Math.floor(new Date().getTime() / 1000),
            "position": 0,
        }
        await fsPromises.writeFile(pathFichierEtat, JSON.stringify(contenu))
        return contenu
    }
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
        debug("verifierFichier Part ", file.path)
        return Number.parseInt(file.path.split('.')[0])
    })

    // Trier en ordre numerique
    filesNumero.sort((a,b)=>{return a-b})

    debug("verifierFichier parts triees\n", filesNumero)

    const writerPromise = new Promise((resolve, reject)=>{
        if(opts.writeStream) {
            opts.writeStream.on('close', resolve)
            opts.writeStream.on('error', reject)
        } else {
            resolve()
        }
    })

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
            fileReader.on('end', resolve)
            fileReader.on('error', reject)
            fileReader.read()
        })
        await promise
        debug("Taille cumulative fichier %s : %d", pathUploadItem, total)

        if(opts.deleteParts === true) await fsPromises.unlink(pathFichier)
    }

    if(opts.writeStream) opts.writeStream.close()

    await writerPromise  // Si on a un writer, attendre l'ecriture complete

    // Verifier hachage - lance une exception si la verification echoue
    await verificateurHachage.verify()
    // Aucune exception, hachage OK

    debug("Fichier correlation %s OK, hachage %s", pathUploadItem, hachage)
    return true
}

module.exports = FichiersMiddleware
