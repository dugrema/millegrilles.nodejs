// Reception de fichiers
const fichiersMiddleware = require('./fichiersMiddleware')

const PATH_STAGING_DEFAUT = '/var/opt/millegrilles/consignation/staging/commun',
      PATH_STAGING_UPLOAD = 'upload'

class FichiersTransfertDownstream {
    
    constructor(mq, opts) {
        opts = opts || {}
        this.mq = mq
        this._pathStaging = opts.PATH_STAGING || PATH_STAGING_DEFAUT
    }

    middlewareRecevoirFichier(opts) {
        opts = opts || {}
        opts = {...opts, PATH_STAGING: this._pathStaging}
        return fichiersMiddleware.middlewareRecevoirFichier(opts)
    }

    middlewareReadyFichier(opts) {
        opts = opts || {}
        opts = {...opts, PATH_STAGING: this._pathStaging}
        return fichiersMiddleware.middlewareReadyFichier(this.mq, opts)
    }

    middlewareDeleteStaging(opts) {
        opts = opts || {}
        opts = {...opts, PATH_STAGING: this._pathStaging}
        return fichiersMiddleware.middlewareDeleteStaging(opts)
    }

}

module.exports = FichiersTransfertDownstream
