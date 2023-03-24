// Transfert de fichiers entre serveurs vers consignation
const debug = require('debug')('nodesjs:fichiersTransfert')
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
      INTERVALLE_PUT_CONSIGNATION = 900_000,
      CONST_TAILLE_SPLIT_MAX_DEFAULT = 5 * 1024 * 1024,
      CONST_INTERVALLE_REFRESH_URL = 900_000

const CODE_HACHAGE_MISMATCH = 1,
      CODE_CLES_SIGNATURE_INVALIDE = 2,
      CODE_TRANSACTION_SIGNATURE_INVALIDE = 3

var _timerPutFichiers = null,
      _amqpdao = null,
      _urlConsignationTransfert = null,
      _disableRefreshUrlTransfert = false,
      _urlTimestamp = 0,
      _httpsAgent = null,
      _pathStaging = null,
      _consignerFichier = transfererFichierVersConsignation,
      _primaire = false,
      _instance_id_consignationTransfer = null

