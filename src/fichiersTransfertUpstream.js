// Transfert de fichiers entre serveurs vers consignation
const debug = require('debug')('nodesjs:fichiersTransfertUpstream')
const fs = require('fs')
const fsPromises = require('fs/promises')
const path = require('path')
const readdirp = require('readdirp')
const https = require('node:https')
const axios = require('axios')

const PATH_STAGING_DEFAUT = '/var/opt/millegrilles/consignation/staging/commun',
    PATH_STAGING_READY = 'ready',
    FICHIER_TRANSACTION_CONTENU = 'transactionContenu.json',
    FICHIER_ETAT = 'etat.json',
    INTERVALLE_PUT_CONSIGNATION = 900_000,
    CONST_TAILLE_SPLIT_MAX_DEFAULT = 100 * 1024 * 1024,
    // CONST_TAILLE_SPLIT_MAX_DEFAULT = 5 * 1024 * 1024,
    CONST_INTERVALLE_REFRESH_URL = 900_000

const CODE_HACHAGE_MISMATCH = 1,
    CODE_CLES_SIGNATURE_INVALIDE = 2,
    CODE_TRANSACTION_SIGNATURE_INVALIDE = 3

class FichiersTransfertUpstream {

    constructor(mq, opts) {
        opts = opts || {}
        this._mq = mq
        this._timerPutFichiers = null
        this._urlConsignationTransfert = null
        this._urlTimestamp = 0
        this._httpsAgent = null
        this._pathStaging = opts.PATH_STAGING || PATH_STAGING_DEFAUT
        this._consignerFichier = null
        this._instance_id_consignationTransfer = null
        this._tailleMaxTransfert = CONST_TAILLE_SPLIT_MAX_DEFAULT

        // Option pour indiquer que le URL de transfert est statique
        this._disableRefreshUrlTransfert = opts.DISABLE_REFRESH || false

        // Liste de batch a transferer (batchIds)
        this._queueItems = []

        this._listenersChangement = []  // Listeners de changement consignation (id, url)
    
        try {
            const url = opts.url
            if (url) this._urlConsignationTransfert = new URL('' + url)
        } catch (err) {
            console.error("Erreur configuration URL upload : ", err)
            if (this._disableRefreshUrlTransfert) throw err  // L'URL est invalide et on ne doit pas le rafraichir
        } finally {
            // Tenter chargement initial
            if (!this._disableRefreshUrlTransfert) {
                this.chargerUrlRequete({ primaire: true })
                    .then(reponse => {
                        debug("Chargement initial URL transfert : ", reponse)
                        const changement = this._urlConsignationTransfert !== reponse.url || this._instance_id_consignationTransfer !== reponse.instance_id
                        this._urlConsignationTransfert = reponse.url
                        this._instance_id_consignationTransfer = reponse.instance_id
                        if(changement) this.emettreEvenement()
                    })
                    .catch(err => console.warn("FichiersTransfertUpstream Erreur chargement initial URL transfert ", err))
            }
        }

        // Preparer directories
        const pathReadyItem = path.join(this._pathStaging, PATH_STAGING_READY)
        fsPromises.mkdir(pathReadyItem, {recursive: true, mode: 0o750})
            .catch(err=>console.error("Erreur preparer path staging ready : %O", err))

        if (opts.consignerFichier) {
            this._consignerFichier = opts.consignerFichier
        } else {
            this._consignerFichier = this.transfererFichierVersConsignation
        }
    
        // Configurer httpsAgent avec les certificats/cles
        const pki = mq.pki
        const { chainePEM: cert, cle: key, ca } = pki
        if (!cert) throw new Error("fichiersTransfertUpstream Certificat non disponible")
        if (!key) throw new Error("fichiersTransfertUpstream Cle non disponible")
        debug("fichiersTransfertUpstream _https.Agent cert : \n%s\n%s\n%s", cert, key, ca)
        this._httpsAgent = new https.Agent({
            //keepAlive: true,
            //maxSockets: 10,
            rejectUnauthorized: false,
            cert, key,
            ca,
        })
    
        // Premiere execution apres redemarrage, delai court
        this._timerPutFichiers = setTimeout(() => {
            this._timerPutFichiers = null
            this._threadPutFichiersConsignation().catch(err => console.error("Erreur run _threadPutFichiersConsignation: %O", err))
        }, 20_000)
    }

    async takeTransfertBatch(batchId, source) {
        const pathReady = path.join(this._pathStaging, PATH_STAGING_READY)
        await fsPromises.mkdir(pathReady, {recursive: true})
        const pathReadyBatch = path.join(pathReady, batchId)
        try {
            await fsPromises.rename(source, pathReadyBatch)
        } catch(err) {
            if(err.code === 'ENOTEMPTY') {
                debug("Destination %s n'est pas vide, on supprime pour re-appliquer ready du nouvel upload", pathReadyBatch)
                await fsPromises.rm(pathReadyBatch, {recursive: true})
                await fsPromises.rename(source, pathReadyBatch)
            } else {
                throw err
            }
        }
        return pathReadyBatch
    }

    ajouterFichierConsignation(item) {
        this._queueItems.push(item)
        if (this._timerPutFichiers) {
            this._threadPutFichiersConsignation()
                .catch(err => console.error("Erreur run _threadPutFichiersConsignation: %O", err))
        }
    }
    
    async _threadPutFichiersConsignation() {
        try {
            debug(new Date() + " Run threadPutFichiersConsignation")
            // Clear timer si present
            if (this._timerPutFichiers) clearTimeout(this._timerPutFichiers)
            this._timerPutFichiers = null
    
            const dateUrlExpire = new Date().getTime() - CONST_INTERVALLE_REFRESH_URL
            if (!this._disableRefreshUrlTransfert && (!this._urlConsignationTransfert || this._urlTimestamp < dateUrlExpire)) {
                // Url consignation vide, on fait une requete pour la configuration initiale
                try {
                    const reponse = await this.chargerUrlRequete({ primaire: true })
                    const changement = this._urlConsignationTransfert !== reponse.url || this._instance_id_consignationTransfer !== reponse.instance_id
                    this._urlConsignationTransfert = reponse.url
                    this._instance_id_consignationTransfer = reponse.instance_id
                    if(changement) this.emettreEvenement()
                } catch (err) {
                    console.error("Erreur reload URL transfert fichiers : ", err)
                    if (!this._urlConsignationTransfert) throw err    // Aucun URL par defaut
                }
                this._urlTimestamp = new Date().getTime()
                debug("Nouveau URL transfert fichiers : ", this._urlConsignationTransfert)
            }
    
            const pathReady = path.join(this._pathStaging, PATH_STAGING_READY)
    
            // Verifier si on a des items dans la Q (prioritaires)
            if (this._queueItems.length === 0) {
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
                    await this.transfererBatchVersConsignation(batchId)
                }
            }
    
            // Process les items recus durant le traitement
            debug("_threadPutFichiersConsignation Queue avec %d items", this._queueItems.length, pathReady)
            while (this._queueItems.length > 0) {
                const batchId = this._queueItems.shift()  // FIFO
                debug("Traiter PUT pour batchId %s", batchId)
                await this.transfererBatchVersConsignation(batchId)
            }
    
        } catch (err) {
            console.error(new Date() + ' _threadPutFichiersConsignation Erreur execution cycle : %O', err)
        } finally {
            this._traitementPutFichiersEnCours = false
            debug("_threadPutFichiersConsignation Fin execution cycle, attente %s ms", INTERVALLE_PUT_CONSIGNATION)
            // Redemarrer apres intervalle
            this._timerPutFichiers = setTimeout(() => {
                this._timerPutFichiers = null
                this._threadPutFichiersConsignation().catch(err => console.error("Erreur run _threadPutFichiersConsignation: %O", err))
            }, INTERVALLE_PUT_CONSIGNATION)
    
        }
    }

    async transfererBatchVersConsignation(batchId) {
        const pathBatch = path.join(this._pathStaging, PATH_STAGING_READY, batchId)
        const promiseReaddirp = readdirp(pathBatch, {
            type: 'directories',
            depth: 1,
        })
    
        for await (const entry of promiseReaddirp) {
            // debug("Entry path : %O", entry);
            const correlation = entry.basename
            debug("Traiter PUT pour correlation %s", correlation)
            await this._consignerFichier(pathBatch, correlation)
        }
    
        debug("Batch %s transfere avec succes vers consignation", batchId)
        fsPromises.rm(pathBatch, { recursive: true })
            .catch(err => console.error("Erreur suppression repertoire %s apres consignation reussie : %O", pathBatch, err))
    }
    
    async chargerUrlRequete(opts) {
        opts = opts || {}
        const requete = { /*primaire: this._primaire*/ }
    
        if (!this._mq) throw new Error("_mq absent")
    
        const reponse = await this._mq.transmettreRequete(
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

    async transfererFichierVersConsignation(pathBatch, item) {
        const pathReadyItem = path.join(pathBatch, item)
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
        const transactions = await traiterTransactions(this._mq, pathReadyItem)
    
        const pathFichier = path.join(pathReadyItem, hachage),
              infoFichier = await fsPromises.stat(pathFichier)
        
        debug("Transactions et fichier OK : %s, %O", pathReadyItem, infoFichier)
        let tailleFichier = infoFichier.size
    
        for(let position=0; position<tailleFichier; position += this._tailleMaxTransfert) {
            const endPosition = Math.min(position+this._tailleMaxTransfert, tailleFichier) - 1  // position end inclusive
    
            const readStream = fs.createReadStream(pathFichier, { start: position, end: endPosition })
    
            const contentLength = endPosition - position + 1
            const urlPosition = new URL(''+this._urlConsignationTransfert)
            urlPosition.pathname = path.join(urlPosition.pathname, hachage, ''+position)
            debug("transfererFichierVersConsignation url %s taille %s", urlPosition, contentLength)
         
            if(!this._httpsAgent) throw new Error("putAxios: httpsAgent n'est pas initialise (utiliser : configurerThreadPutFichiersConsignation)")
        
            try {
                await axios({
                    method: 'PUT',
                    httpsAgent: this._httpsAgent,
                    maxRedirects: 0,
                    url: urlPosition.href,
                    headers: {
                        'content-length': contentLength,
                        'content-type': 'application/stream'
                    },
                    data: readStream,
                    timeout: 1_200_000,
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
        const urlPost = new URL('' + this._urlConsignationTransfert)
        urlPost.pathname = path.join(urlPost.pathname, hachage)
        const reponsePost = await axios({
            method: 'POST',
            httpsAgent: this._httpsAgent,
            url: urlPost.href,
            data: transactions,
            timeout: 1_200_000,
        })
    
        // Le fichier a ete transfere avec succes (aucune exception)
        // On peut supprimer le repertoire ready local
        debug("Fichier %s transfere avec succes vers consignation, reponse %O", item, reponsePost)
        fsPromises.rm(pathReadyItem, { recursive: true })
            .catch(err => console.error("Erreur suppression repertoire %s apres consignation reussie : %O", item, err))
    
    }

    getUrlConsignationTransfert() {
        return this._urlConsignationTransfert
    }

    getIdConsignation() {
        return this._instance_id_consignationTransfer
    }

    ajouterListener(listenerCb) {
        this._listenersChangement.push(listenerCb)
    }

    retirerListener(listenerCb) {
        this._listenersChangement = this._listenersChangement.filter(item=>item!==listenerCb)
    }

    emettreEvenement() {
        for(const listener of this._listenersChangement) {
            try {
                listener(this)
            } catch(err) {
                console.error("fichiersTransfertUpstream Erreur evenementListeners ", err)
            }
        }
    }
}

async function traiterTransactions(amqpdao, pathReady) {
    let transactionContenu = null
    const pki = amqpdao.pki

    const transactions = {}

    try {
        const pathEtat = path.join(pathReady, FICHIER_ETAT)
        const etat = JSON.parse(await fsPromises.readFile(pathEtat))
        transactions.etat = etat
    } catch(err) {
        console.warn("ERROR durant chargement de l'etat de consignation de %s : %O", pathReady, err)
    }

    try {
        const pathReadyItemTransaction = path.join(pathReady, FICHIER_TRANSACTION_CONTENU)
        transactionContenu = JSON.parse(await fsPromises.readFile(pathReadyItemTransaction))
    } catch(err) {
        // Pas de transaction de contenu
    }

    if(transactionContenu) {
        try { 
            await pki.verifierMessage(transactionContenu) 
            transactions.transaction = transactionContenu

            const transactionCles = transactionContenu['_cles']
            if(transactionCles) {
                await pki.verifierMessage(transactionCles) 
            }            
        } 
        catch(err) {
            debug("Erreur validation transaction : ", err)
            err.code = CODE_TRANSACTION_SIGNATURE_INVALIDE
            throw err
        }

    }

    return transactions
}

module.exports = FichiersTransfertUpstream
