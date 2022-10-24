const EventEmitter = require('node:events')
const chalk = require('chalk')
const dayjs = require('dayjs')
const axios = require('axios')
const WebSocket = require('ws')

exports.log = (debug) => {
  return {
    step: (msg) => console.log(chalk.blue.bold.underline(`[${dayjs().format('LTS')}] ${msg}`)),
    error: (msg, extra) => console.log(chalk.red.bold(`[${dayjs().format('LTS')}] ${msg}`), extra),
    warning: (msg, extra) => console.log(chalk.red(`[${dayjs().format('LTS')}] ${msg}`), extra),
    info: (msg, extra) => console.log(chalk.blue(`[${dayjs().format('LTS')}] ${msg}`), extra),
    debug: (msg, extra) => {
      if (debug) console.log(`[${dayjs().format('LTS')}] debug - ${msg}`, extra)
    }
  }
}

exports.axios = (config) => {
  const headers = { 'x-apiKey': config.dataFairAPIKey }
  const axiosInstance = axios.create({
    // this is necessary to prevent excessive memory usage during large file uploads, see https://github.com/axios/axios/issues/1045
    maxRedirects: 0
  })
  // apply default base url and send api key when relevant
  axiosInstance.interceptors.request.use(cfg => {
    if (!/^https?:\/\//i.test(cfg.url)) {
      if (cfg.url.startsWith('/')) cfg.url = config.dataFairUrl + cfg.url
      else cfg.url = config.dataFairUrl + '/' + cfg.url
    }
    if (cfg.url.startsWith(config.dataFairUrl)) Object.assign(cfg.headers, headers)
    return cfg
  }, error => Promise.reject(error))
  // customize axios errors for shorter stack traces when a request fails
  axiosInstance.interceptors.response.use(response => response, error => {
    if (!error.response) return Promise.reject(error)
    delete error.response.request
    delete error.response.headers
    error.response.config = { method: error.response.config.method, url: error.response.config.url, data: error.response.config.data }
    if (error.response.config.data && error.response.config.data._writableState) delete error.response.config.data
    if (error.response.data && error.response.data._readableState) delete error.response.data
    return Promise.reject(error.response)
  })
  return axiosInstance
}

exports.ws = (config, log) => {
  const ws = new EventEmitter()
  ws._channels = []
  ws._connect = async () => {
    return new Promise((resolve, reject) => {
      const wsUrl = config.dataFairUrl.replace('http://', 'ws://').replace('https://', 'wss://') + '/'
      log.debug(`connect Web Socket to ${wsUrl}`)
      ws._ws = new WebSocket(wsUrl)
      ws._ws.on('error', err => {
        log.debug('WS encountered an error', err.message)
        ws._reconnect()
        reject(err)
      })
      ws._ws.once('open', () => {
        log.debug('WS is opened')
        resolve(ws._ws)
      })
      ws._ws.on('message', (message) => {
        message = JSON.parse(message.toString())
        log.debug('received message', message)
        ws.emit('message', message)
      })
    })
  }
  ws._reconnect = async () => {
    log.debug('reconnect')
    ws._ws.terminate()
    await ws._connect()
    for (const channel of ws._channels) {
      await ws.subscribe(channel, true)
    }
  }
  ws.subscribe = async (channel, force = false, timeout = 2000) => {
    if (ws._channels.includes(channel) && !force) return
    if (!ws._ws) await ws._connect()
    return new Promise((resolve, reject) => {
      const _timeout = setTimeout(() => reject(new Error('timeout')), timeout)
      log.debug('subscribe to channel', channel)
      ws._ws.send(JSON.stringify({ type: 'subscribe', channel, apiKey: config.dataFairAPIKey }))
      ws.once('message', (message) => {
        if (message.channel && message.channel !== channel) return
        clearTimeout(_timeout)
        log.debug('received response to subscription', message)
        if (message.type === 'error') return reject(new Error(message))
        else if (message.type === 'subscribe-confirm') return resolve()
        else return reject(new Error('expected a subscription confirmation, got ' + JSON.stringify(message)))
      })
      if (ws._channels.includes(channel)) ws._channels.push(channel)
    })
  }
  ws.waitFor = async (channel, filter, timeout = 300000) => {
    await ws.subscribe(channel)
    return new Promise((resolve, reject) => {
      const _timeout = setTimeout(() => reject(new Error('timeout')), timeout)
      const messageCb = (message) => {
        if (message.channel === channel && (!filter || filter(message.data))) {
          clearTimeout(_timeout)
          ws.off('message', messageCb)
          resolve(message.data)
        }
      }
      ws.on('message', messageCb)
    })
  }
  ws.waitForJournal = async (datasetId, eventType, timeout = 300000) => {
    log.info(`attend l'évènement du journal ${datasetId} / ${eventType}`)
    return ws.waitFor(`datasets/${datasetId}/journal`, (e) => e.type === eventType, timeout)
  }
  return ws
}

exports.context = (initialContext, config, debug) => {
  const context = { ...initialContext }
  context.processingConfig = context.processingConfig || {}
  context.pluginConfig = context.pluginConfig || {}
  context.log = exports.log(debug)
  context.axios = exports.axios(config)
  context.ws = exports.ws(config, context.log)
  context.patchConfig = async (patch) => {
    console.log('received config patch', patch)
    Object.assign(context.processingConfig, patch)
  }
  return context
}
