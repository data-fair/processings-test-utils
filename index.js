const EventEmitter = require('node:events')
const chalk = require('chalk')
const dayjs = require('dayjs')
const localizedFormat = require('dayjs/plugin/localizedFormat')
dayjs.extend(localizedFormat)
const axios = require('axios')
const WebSocket = require('ws')
require('draftlog').into(console).addLineListener(process.stdin)

const tasksDraftLog = {}

const denseInspect = (arg) => {
  if (arg === undefined) return ''
  if (typeof arg === 'object') {
    try {
      const str = JSON.stringify(arg)
      if (str.length < 200) return str
    } catch (err) {
      // nothing to do, maybe circular object, etc
    }
  }
  return arg
}

exports.log = (debug, testDebug) => {
  return {
    step: (msg) => console.log(chalk.blueBright.bold.underline(`[${dayjs().format('LTS')}] ${msg}`)),
    error: (msg, extra) => console.log(chalk.red.bold(`[${dayjs().format('LTS')}] ${msg}`), denseInspect(extra)),
    warning: (msg, extra) => console.log(chalk.red(`[${dayjs().format('LTS')}] ${msg}`), denseInspect(extra)),
    info: (msg, extra) => console.log(chalk.blueBright(`[${dayjs().format('LTS')}] ${msg}`), denseInspect(extra)),
    debug: (msg, extra) => debug && console.log(`[${dayjs().format('LTS')}][debug] ${msg}`, denseInspect(extra)),
    task: (name) => {
      tasksDraftLog[name] = console.draft()
      tasksDraftLog[name](chalk.yellow(name))
    },
    progress: (taskName, progress, total) => {
      const msg = `[${dayjs().format('LTS')}][task] ${taskName} - ${progress} / ${total}`
      if (progress === 0) tasksDraftLog[taskName](chalk.yellow(msg))
      else if (progress >= total) tasksDraftLog[taskName](chalk.greenBright(msg))
      else tasksDraftLog[taskName](chalk.greenBright.bold(msg))
    },
    testInfo: (msg, extra) => console.log(chalk.yellowBright.bold(`[${dayjs().format('LTS')}][test] - ${msg}`), denseInspect(extra)),
    testDebug: (msg, extra) => testDebug && console.log(chalk.yellowBright(`[${dayjs().format('LTS')}][test][debug] - ${msg}`), denseInspect(extra))
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
    const headers = {}
    if (error.response.headers.location) headers.location = error.response.headers.location
    error.response.headers = headers
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
      log.testDebug(`connect Web Socket to ${wsUrl}`)
      ws._ws = new WebSocket(wsUrl)
      ws._ws.on('error', err => {
        log.testDebug('WS encountered an error', err.message)
        ws._reconnect()
        reject(err)
      })
      ws._ws.once('open', () => {
        log.testDebug('WS is opened')
        resolve(ws._ws)
      })
      ws._ws.on('message', (message) => {
        message = JSON.parse(message.toString())
        log.testDebug('received message', message)
        ws.emit('message', message)
      })
    })
  }
  ws._reconnect = async () => {
    log.testDebug('reconnect')
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
      log.testDebug('subscribe to channel', channel)
      ws._ws.send(JSON.stringify({ type: 'subscribe', channel, apiKey: config.dataFairAPIKey }))
      const messageCb = (message) => {
        if (message.channel && message.channel !== channel) return
        clearTimeout(_timeout)
        log.testDebug('received response to subscription', message)
        if (message.type === 'error') {
          ws.off('message', messageCb)
          return reject(new Error(message))
        } else if (message.type === 'subscribe-confirm') {
          ws.off('message', messageCb)
          return resolve()
        }
      }
      ws.on('message', messageCb)
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
    log.testInfo(`attend l'évènement du journal ${datasetId} / ${eventType}`)
    const event = await ws.waitFor(`datasets/${datasetId}/journal`, (e) => e.type === eventType || e.type === 'error', timeout)
    if (event.type === 'error') throw new Error(event.data)
    return event
  }
  return ws
}

exports.context = (initialContext, config, debug, testDebug) => {
  const context = { ...initialContext }
  context.processingConfig = context.processingConfig || {}
  context.pluginConfig = context.pluginConfig || {}
  context.log = exports.log(debug, testDebug)
  context.axios = exports.axios(config)
  context.ws = exports.ws(config, context.log)
  let createdDataset
  context.patchConfig = async (patch) => {
    context.log.testInfo('received config patch', patch)
    if (patch.datasetMode === 'update' && patch.dataset) createdDataset = patch.dataset
    Object.assign(context.processingConfig, patch)
  }
  context.cleanup = async () => {
    if (context.ws._ws) context.ws._ws.terminate()
    if (createdDataset) {
      context.log.testInfo('delete test dataset', createdDataset)
      await context.axios.delete('api/v1/datasets/' + createdDataset.id)
    }
  }
  return context
}
