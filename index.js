const { EventEmitter } = require('events')

const ResourceState = {
  ALLOCATED: 'ALLOCATED',
  IDLE: 'IDLE',
  INVALID: 'INVALID',
  RETURNING: 'RETURNING',
  VALIDATION: 'VALIDATION'
}

class PriorityQueue {
  constructor(size) {
    this._size = Math.max(Number(size) || 0, 1)
    this._slots = Array.from({ length: this._size }, () => [])
    this._nextNonEmptySlot = 0 // Track the next non-empty slot
  }

  get length() {
    return this._slots.reduce((total, slot) => total + slot.length, 0)
  }

  enqueue(obj, priority) {
    const normalizedPriority = Math.min(Math.max(Number(priority) || 0, 0), this._size - 1)
    this._slots[normalizedPriority].push(obj)
    if (normalizedPriority < this._nextNonEmptySlot) {
      this._nextNonEmptySlot = normalizedPriority
    }
  }

  dequeue() {
    while (this._nextNonEmptySlot < this._size && this._slots[this._nextNonEmptySlot].length === 0) {
      this._nextNonEmptySlot++
    }
    if (this._nextNonEmptySlot < this._size) {
      return this._slots[this._nextNonEmptySlot].shift()
    }
    return undefined
  }

  get head() {
    for (let i = 0; i < this._size; i++) {
      if (this._slots[i].length > 0) return this._slots[i][0]
    }
    return undefined
  }

  get tail() {
    for (let i = this._size - 1; i >= 0; i--) {
      if (this._slots[i].length > 0) return this._slots[i][this._slots[i].length - 1]
    }
    return undefined
  }
}

class DefaultEvictor {
  evict(config, pooledResource, availableObjectsCount) {
    const idleTime = Date.now() - pooledResource.lastIdleTime
    if (config.softIdleTimeoutMillis > 0 && config.softIdleTimeoutMillis < idleTime && config.min < availableObjectsCount) {
      return true
    }
    return config.idleTimeoutMillis < idleTime
  }
}

class PooledResource {
  constructor(resource) {
    this.creationTime = Date.now()
    this.lastReturnTime = null
    this.lastBorrowTime = null
    this.lastIdleTime = null
    this.obj = resource
    this.state = ResourceState.IDLE
  }

  allocate() {
    this.lastBorrowTime = Date.now()
    this.state = ResourceState.ALLOCATED
  }

  deallocate() {
    this.lastReturnTime = Date.now()
    this.state = ResourceState.IDLE
  }

  invalidate() {
    this.state = ResourceState.INVALID
  }

  test() {
    this.state = ResourceState.VALIDATION
  }

  idle() {
    this.lastIdleTime = Date.now()
    this.state = ResourceState.IDLE
  }

  returning() {
    this.state = ResourceState.RETURNING
  }
}

class Deferred {
  constructor() {
    this._state = Deferred.PENDING
    this._promise = new Promise((resolve, reject) => {
      this._resolve = resolve
      this._reject = reject
    })
  }

  get state() {
    return this._state
  }

  get promise() {
    return this._promise
  }

  reject(reason) {
    if (this._state !== Deferred.PENDING) return
    this._state = Deferred.REJECTED
    this._reject(reason)
  }

  resolve(value) {
    if (this._state !== Deferred.PENDING) return
    this._state = Deferred.FULFILLED
    this._resolve(value)
  }
}

Deferred.PENDING = 'PENDING'
Deferred.FULFILLED = 'FULFILLED'
Deferred.REJECTED = 'REJECTED'

class ResourceRequest extends Deferred {
  constructor(ttl) {
    super()
    this._timeout = null
    this._ttl = ttl || null
    this._startTime = Date.now()
    if (ttl !== undefined) this.setTimeout(ttl)
  }

  setTimeout(delay) {
    if (this._state !== Deferred.PENDING) return
    const ttl = parseInt(delay, 10)
    if (isNaN(ttl) || ttl <= 0) throw new Error('delay must be a positive int')
    this.removeTimeout()
    this._ttl = ttl
    this._timeout = setTimeout(() => this._fireTimeout(), ttl)
  }

  removeTimeout() {
    if (this._timeout) {
      clearTimeout(this._timeout)
      this._timeout = null
    }
  }

  _fireTimeout() {
    this.reject(new TimeoutError('ResourceRequest timed out'))
  }

  reject(reason) {
    this.removeTimeout()
    super.reject(reason)
  }

  resolve(value) {
    this.removeTimeout()
    super.resolve(value)
  }

  get remainingTime() {
    return this._timeout ? Math.max(0, this._ttl - (Date.now() - this._startTime)) : 0
  }
}

class TimeoutError extends Error {
  constructor(message) {
    super(message)
    this.name = this.constructor.name
    if (typeof Error.captureStackTrace === 'function') {
      Error.captureStackTrace(this, this.constructor)
    } else {
      this.stack = new Error(message).stack
    }
  }
}

class Pool extends EventEmitter {
  constructor(factory, options = {}) {
    super()
    this.factory = factory
    this.options = {
      fifo: true,
      priorityRange: 1,
      testOnBorrow: false,
      testOnReturn: false,
      autostart: true,
      evictionRunIntervalMillis: 0,
      numTestsPerEvictionRun: 3,
      softIdleTimeoutMillis: -1,
      idleTimeoutMillis: 30000,
      acquireTimeoutMillis: null,
      destroyTimeoutMillis: null,
      maxWaitingClients: null,
      min: 0,
      max: 10,
      ...options
    }
    this._draining = false
    this._started = false
    this._waitingClientsQueue = new PriorityQueue(this.options.priorityRange)
    this._availableObjects = new Set()
    this._resourceLoans = new Map()
    this._allObjects = new Set()
    this._evictor = new DefaultEvictor()
    this._factoryCreateOperations = new Set()
    this._factoryDestroyOperations = new Set()
    this._testOnBorrowResources = new Set()
    this._testOnReturnResources = new Set()
    if (this.options.autostart) this.start()
  }

  _destroy(pooledResource) {
    pooledResource.invalidate()
    this._allObjects.delete(pooledResource)
    const destroyPromise = this.factory.destroy(pooledResource.obj)
    const wrappedDestroyPromise = this.options.destroyTimeoutMillis
      ? this._applyDestroyTimeout(destroyPromise)
      : destroyPromise
    wrappedDestroyPromise.catch(reason => this.emit('factoryDestroyError', reason))
    this._ensureMinimum()
  }

  _applyDestroyTimeout(promise) {
    return Promise.race([
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error('destroy timed out')), this.options.destroyTimeoutMillis).unref()
      ),
      promise
    ])
  }

  _createResource() {
    const factoryPromise = this.factory.create()
    this._factoryCreateOperations.add(factoryPromise)
    factoryPromise
      .then(resource => {
        this._factoryCreateOperations.delete(factoryPromise)
        const pooledResource = new PooledResource(resource)
        this._allObjects.add(pooledResource)
        this._addPooledResourceToAvailableObjects(pooledResource)
        this._dispense()
      })
      .catch(reason => {
        this._factoryCreateOperations.delete(factoryPromise)
        this.emit('factoryCreateError', reason)
        this._dispense()
      })
  }

  _addPooledResourceToAvailableObjects(pooledResource) {
    pooledResource.idle()
    this._availableObjects.add(pooledResource)
  }

  _dispatchPooledResourceToNextWaitingClient(pooledResource) {
    const clientResourceRequest = this._waitingClientsQueue.dequeue()
    if (!clientResourceRequest) {
      this._addPooledResourceToAvailableObjects(pooledResource)
      return false
    }
    const loan = new ResourceLoan(pooledResource)
    this._resourceLoans.set(pooledResource.obj, loan)
    pooledResource.allocate()
    clientResourceRequest.resolve(pooledResource.obj)
    return true
  }

  _ensureMinimum() {
    if (this._draining) return
    const minShortfall = this.options.min - this._count
    for (let i = 0; i < minShortfall; i++) {
      this._createResource()
    }
  }

  _dispense() {
    const numWaitingClients = this._waitingClientsQueue.length
    if (numWaitingClients < 1) return
    const resourceShortfall = numWaitingClients - this._potentiallyAllocableResourceCount
    const actualNumberOfResourcesToCreate = Math.min(this.spareResourceCapacity, resourceShortfall)
    for (let i = 0; i < actualNumberOfResourcesToCreate; i++) {
      this._createResource()
    }
    if (this.options.testOnBorrow) {
      this._testAndDispatchResources(numWaitingClients)
    } else {
      this._dispatchAvailableResources(numWaitingClients)
    }
  }

  _testAndDispatchResources(numWaitingClients) {
    const resourcesToTest = Math.min(this._availableObjects.size, numWaitingClients - this._testOnBorrowResources.size)
    for (let i = 0; i < resourcesToTest; i++) {
      this._testOnBorrow()
    }
  }

  _dispatchAvailableResources(numWaitingClients) {
    const resourcesToDispatch = Math.min(this._availableObjects.size, numWaitingClients)
    for (let i = 0; i < resourcesToDispatch; i++) {
      this._dispatchResource()
    }
  }

  _testOnBorrow() {
    if (this._availableObjects.size < 1) return false
    const pooledResource = this._availableObjects.values().next().value
    this._availableObjects.delete(pooledResource)
    pooledResource.test()
    this._testOnBorrowResources.add(pooledResource)
    const validationPromise = this.factory.validate(pooledResource.obj)
    validationPromise
      .then(isValid => {
        this._testOnBorrowResources.delete(pooledResource)
        if (!isValid) {
          pooledResource.invalidate()
          this._destroy(pooledResource)
          this._dispense()
          return
        }
        this._dispatchPooledResourceToNextWaitingClient(pooledResource)
      })
    return true
  }

  _dispatchResource() {
    if (this._availableObjects.size < 1) return false
    const pooledResource = this._availableObjects.values().next().value
    this._availableObjects.delete(pooledResource)
    this._dispatchPooledResourceToNextWaitingClient(pooledResource)
    return true
  }

  _evict() {
    const evictionConfig = {
      softIdleTimeoutMillis: this.options.softIdleTimeoutMillis,
      idleTimeoutMillis: this.options.idleTimeoutMillis,
      min: this.options.min,
    }
    const resourcesToEvict = []
    for (const resource of this._availableObjects) {
      if (resourcesToEvict.length >= this.options.numTestsPerEvictionRun) break
      if (this._evictor.evict(evictionConfig, resource, this._availableObjects.size)) {
        resourcesToEvict.push(resource)
      }
    }
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

  _descheduleEvictorRun() {
    if (this._scheduledEviction) clearTimeout(this._scheduledEviction)
    this._scheduledEviction = null
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
      this._waitingClientsQueue.length >= this.options.maxWaitingClients
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
    loan.resolve()
    const pooledResource = loan.pooledResource
    pooledResource.deallocate()
    this._addPooledResourceToAvailableObjects(pooledResource)
    this._dispense()
    return Promise.resolve()
  }

  destroy(resource) {
    const loan = this._resourceLoans.get(resource)
    if (!loan) return Promise.reject(new Error('Resource not currently part of this pool'))
    this._resourceLoans.delete(resource)
    loan.resolve()
    const pooledResource = loan.pooledResource
    pooledResource.deallocate()
    this._destroy(pooledResource)
    this._dispense()
    return Promise.resolve()
  }

  drain() {
    this._draining = true
    return this.__allResourceRequestsSettled()
      .then(() => this.__allResourcesReturned())
      .then(() => this._descheduleEvictorRun())
  }

  __allResourceRequestsSettled() {
    if (this._waitingClientsQueue.length > 0) {
      return this._waitingClientsQueue.tail.promise
    }
    return Promise.resolve()
  }

  __allResourcesReturned() {
    const ps = Array.from(this._resourceLoans.values()).map(loan => loan.promise)
    return Promise.all(ps)
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
        if (this.available >= this.min) resolve()
        else setTimeout(isReady, 100)
      }
      isReady()
    })
  }

  get _potentiallyAllocableResourceCount() {
    return this._availableObjects.size + this._testOnBorrowResources.size + this._testOnReturnResources.size + this._factoryCreateOperations.size
  }

  get _count() {
    return this._allObjects.size + this._factoryCreateOperations.size
  }

  get spareResourceCapacity() {
    return this.options.max - (this._allObjects.size + this._factoryCreateOperations.size)
  }

  get size() {
    return this._count
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

  get max() {
    return this.options.max
  }

  get min() {
    return this.options.min
  }
}

module.exports = {
  createPool: (factory, config) => new Pool(factory, config)
}
