const { EventEmitter } = require('events')

const ResourceState = Object.freeze({
  ALLOCATED: 'ALLOCATED',
  IDLE: 'IDLE',
  INVALID: 'INVALID',
  VALIDATION: 'VALIDATION'
})

class PooledResource {
  constructor(resource) {
    this.obj = resource
    this.creationTime = Date.now()
    this.lastIdleTime = null
    this.state = ResourceState.IDLE
  }

  updateState(newState) {
    if (newState === ResourceState.IDLE) {
      this.lastIdleTime = Date.now()
    }
    this.state = newState
  }
}

class ResourceRequest {
  constructor(ttl) {
    this._ttl = ttl || null
    this._state = 'pending'
    this.promise = new Promise((resolve, reject) => {
      this._resolve = resolve
      this._reject = reject
      if (ttl !== undefined) this.setTimeout(ttl)
    })
  }

  setTimeout(delay) {
    if (this._state !== 'pending') return
    const ttl = parseInt(delay, 10)
    this.removeTimeout()
    this._ttl = ttl
    this._timeout = setTimeout(() => this.reject(new Error('ResourceRequest timed out')), ttl)
  }

  removeTimeout() {
    if (this._timeout) {
      clearTimeout(this._timeout)
      this._timeout = null
    }
  }

  reject(reason) {
    if (this._state !== 'pending') return
    this.removeTimeout()
    this._state = 'rejected'
    this._reject(reason)
  }

  resolve(value) {
    if (this._state !== 'pending') return
    this.removeTimeout()
    this._state = 'fulfilled'
    this._resolve(value)
  }
}

class WaitingClientsQueue {
  constructor() {
    this._queue = []
  }

  enqueue(request, priority = 0) {
    this._queue.push({ request, priority })
    this._queue.sort((a, b) => b.priority - a.priority)
  }

  dequeue() {
    return this._queue.shift()?.request || null
  }

  get length() {
    return this._queue.length
  }

  get tail() {
    return this._queue[this._queue.length - 1]?.request || null
  }
}

class Pool extends EventEmitter {
  constructor(factory, options = {}) {
    super()
    this.factory = factory
    this.options = Object.assign({
      testOnBorrow: false,
      evictionRunIntervalMillis: 100000,
      numTestsPerEvictionRun: 3,
      softIdleTimeoutMillis: -1,
      idleTimeoutMillis: 30000,
      acquireTimeoutMillis: null,
      destroyTimeoutMillis: null,
      maxWaitingClients: null,
      min: 0,
      max: 10
    }, options)
    this._draining = false
    this._started = false
    this._availableObjects = new Set()
    this._resourceLoans = new Map()
    this._allObjects = new Set()
    this._factoryCreateOperations = new Set()
    this._waitingClientsQueue = new WaitingClientsQueue()
    this.start()
  }

  _destroy(resource) {
    resource.updateState(ResourceState.INVALID)
    this._allObjects.delete(resource)
    const destroyPromise = this.factory.destroy(resource.obj)
    const wrappedDestroyPromise = this.options.destroyTimeoutMillis ? Promise.race([
          new Promise((_, reject) =>
            setTimeout(() => reject(new Error('destroy timed out')), this.options.destroyTimeoutMillis).unref()
          ),
          destroyPromise
        ])
      : destroyPromise
    wrappedDestroyPromise.catch(reason => this.emit('factoryDestroyError', reason))
    this._ensureMinimum()
  }

  _createResource() {
    const factoryPromise = this.factory.create()
    this._factoryCreateOperations.add(factoryPromise)
    factoryPromise.then(resource => {
        this._factoryCreateOperations.delete(factoryPromise)
        const pooledResource = new PooledResource(resource)
        this._allObjects.add(pooledResource)
        pooledResource.updateState(ResourceState.IDLE)
        this._availableObjects.add(pooledResource)
        this._dispense()
      })
      .catch(reason => {
        this._factoryCreateOperations.delete(factoryPromise)
        this.emit('factoryCreateError', reason)
        this._dispense()
      })
  }

  _dispatchPooledResourceToNextWaitingClient(resource) {
    const clientResourceRequest = this._waitingClientsQueue.dequeue()
    if (!clientResourceRequest) {
      resource.updateState(ResourceState.IDLE)
      this._availableObjects.add(resource)
      return false
    }
    this._resourceLoans.set(resource.obj, { pooledResource: resource })
    resource.updateState(ResourceState.ALLOCATED)
    clientResourceRequest.resolve(resource.obj)
    return true
  }

  _ensureMinimum() {
    if (this._draining) return
    const minShortfall = this.options.min - this.size
    for (let i = 0; i < minShortfall; i++) this._createResource()
  }

  _dispense() {
    const numWaitingClients = this.pending
    if (numWaitingClients < 1) return
    const _potentiallyAllocableResourceCount = this._availableObjects.size + this._factoryCreateOperations.size
    const resourceShortfall = numWaitingClients - _potentiallyAllocableResourceCount
    const actualNumberOfResourcesToCreate = Math.min(this.spareResourceCapacity, resourceShortfall)
    for (let i = 0; i < actualNumberOfResourcesToCreate; i++) {
      this._createResource()
    }
    if (this.options.testOnBorrow) {
      const resourcesToTest = Math.min(this._availableObjects.size, numWaitingClients)
      for (let i = 0; i < resourcesToTest; i++) {
        if (this._availableObjects.size < 1) return false
        const resource = this._availableObjects.values().next().value
        this._availableObjects.delete(resource)
        resource.updateState(ResourceState.VALIDATION)
        this.factory.validate(resource.obj)
          .then(isValid => {
            if (!isValid) {
              resource.updateState(ResourceState.INVALID)
              this._destroy(resource)
              this._dispense()
              return
            }
            this._dispatchPooledResourceToNextWaitingClient(resource)
          })
        return true
      }
    } else {
      const resourcesToDispatch = Math.min(this._availableObjects.size, numWaitingClients)
      for (let i = 0; i < resourcesToDispatch; i++) {
        if (this._availableObjects.size < 1) return false
        const resource = this._availableObjects.values().next().value
        this._availableObjects.delete(resource)
        this._dispatchPooledResourceToNextWaitingClient(resource)
        return true
      }
    }
  }

  _evict() {
    const evictionConfig = {
      softIdleTimeoutMillis: this.options.softIdleTimeoutMillis,
      idleTimeoutMillis: this.options.idleTimeoutMillis,
      min: this.options.min
    }
    const resourcesToEvict = Array.from(this._availableObjects)
      .slice(0, this.options.numTestsPerEvictionRun)
      .filter(resource => {
        const idleTime = Date.now() - resource.lastIdleTime
        return (evictionConfig.softIdleTimeoutMillis > 0 && evictionConfig.softIdleTimeoutMillis < idleTime && evictionConfig.min < this._availableObjects.size) ||
          evictionConfig.idleTimeoutMillis < idleTime
      })

    resourcesToEvict.forEach(resource => {
      this._availableObjects.delete(resource)
      this._destroy(resource)
    })
  }

  _scheduleEvictorRun() {
    if (this.options.evictionRunIntervalMillis > 0) {
      this._scheduledEviction = setTimeout(() => {
        this._evict()
        this._scheduleEvictorRun()
      }, this.options.evictionRunIntervalMillis).unref()
    }
  }

  start() {
    if (this._draining || this._started) return
    this._started = true
    this._scheduleEvictorRun()
    this._ensureMinimum()
  }

  acquire(priority) {
    if (!this._started && !this.options.autostart) this.start()
    if (this._draining) return Promise.reject(new Error('pool is draining and cannot accept work'))
    if (
      this.spareResourceCapacity < 1 &&
      this._availableObjects.size < 1 &&
      this.options.maxWaitingClients !== undefined &&
      this.pending >= this.options.maxWaitingClients
    ) {
      return Promise.reject(new Error('max waitingClients count exceeded'))
    }
    const resourceRequest = new ResourceRequest(this.options.acquireTimeoutMillis)
    this._waitingClientsQueue.enqueue(resourceRequest, priority)
    this._dispense()
    return resourceRequest.promise
  }

  async use(fn, priority) {
    const resource = await this.acquire(priority)
    try {
      const result = await fn(resource)
      await this.release(resource)
      return result
    } catch (err) {
      await this.destroy(resource)
      throw err
    }
  }

  release(resource) {
    const loan = this._resourceLoans.get(resource)
    if (!loan) return Promise.reject(new Error('Resource not currently part of this pool'))
    this._resourceLoans.delete(resource)
    const pooledResource = loan.pooledResource
    pooledResource.updateState(ResourceState.IDLE)
    this._availableObjects.add(pooledResource)
    this._dispense()
    return Promise.resolve()
  }

  destroy(resource) {
    const loan = this._resourceLoans.get(resource)
    if (!loan) return Promise.reject(new Error('Resource not currently part of this pool'))
    this._resourceLoans.delete(resource)
    const pooledResource = loan.pooledResource
    pooledResource.updateState(ResourceState.INVALID)
    this._destroy(pooledResource)
    this._dispense()
    return Promise.resolve()
  }

  drain() {
    this._draining = true
    const allResourceRequestsSettled = this._waitingClientsQueue.length > 0 ? this._waitingClientsQueue.tail.promise : Promise.resolve()
    return allResourceRequestsSettled
      .then(() => Promise.all(Array.from(this._resourceLoans.values()).map(loan => loan.pooledResource.promise)))
      .then(() => clearTimeout(this._scheduledEviction))
  }

  async clear() {
    const reflectedCreatePromises = Array.from(this._factoryCreateOperations)
    await Promise.all(reflectedCreatePromises)
    for (const resource of this._availableObjects) {
      this._destroy(resource)
    }
    const reflectedDestroyPromises = Array.from(this._factoryDestroyOperations)
    return Promise.all(reflectedDestroyPromises)
  }

  ready() {
    return new Promise(resolve => {
      const isReady = () => {
        if (this.available >= this.options.min) resolve()
        else setTimeout(isReady, 100)
      }
      isReady()
    })
  }

  get size() {
    return this._allObjects.size + this._factoryCreateOperations.size
  }

  get spareResourceCapacity() {
    return this.options.max - this.size
  }

  get available() {
    return this._availableObjects.size
  }

  get borrowed() {
    return this._resourceLoans.size
  }

  get pending() {
    return this._waitingClientsQueue.length
  }
}

module.exports = {
  createPool: (factory, config) => new Pool(factory, config)
}
