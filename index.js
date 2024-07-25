const ResourceState = {
  ALLOCATED: 'ALLOCATED',
  IDLE: 'IDLE',
  INVALID: 'INVALID',
  RETURNING: 'RETURNING',
  VALIDATION: 'VALIDATION'
}

class Deque {
  constructor() {
    this._list = []
  }

  shift() {
    return this._list.shift()
  }

  unshift(element) {
    this._list.unshift(element)
  }

  push(element) {
    this._list.push(element)
  }

  pop() {
    return this._list.pop()
  }

  [Symbol.iterator]() {
    return this._list[Symbol.iterator]()
  }

  get head() {
    return this._list[0]
  }

  get tail() {
    return this._list[this._list.length - 1]
  }

  get length() {
    return this._list.length
  }
}

class Queue extends Deque {
  push(resourceRequest) {
    const node = { data: resourceRequest }
    resourceRequest.promise.catch(reason => {
      if (reason.name === 'TimeoutError') {
        this._list.remove(node)
      }
    })
    this._list.push(node)
  }
}

class PriorityQueue {
  constructor(size) {
    this._size = Math.max(+size | 0, 1)
    this._slots = Array.from({ length: this._size }, () => new Queue())
  }

  get length() {
    return this._slots.reduce((total, slot) => total + slot.length, 0)
  }

  enqueue(obj, priority) {
    priority = (priority && +priority | 0) || 0
    priority = Math.min(Math.max(priority, 0), this._size - 1)
    this._slots[priority].push(obj)
  }

  dequeue() {
    for (let slot of this._slots) {
      if (slot.length) return slot.shift()
    }
    return undefined
  }

  get head() {
    for (let slot of this._slots) {
      if (slot.length > 0) return slot.head
    }
    return undefined
  }

  get tail() {
    for (let i = this._slots.length - 1; i >= 0; i--) {
      if (this._slots[i].length > 0) return this._slots[i].tail
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
    this._resolve = undefined
    this._reject = undefined

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

class ResourceLoan extends Deferred {
  constructor(pooledResource) {
    super()
    this._creationTimestamp = Date.now()
    this.pooledResource = pooledResource
  }

  reject() { /** Loans can only be resolved at the moment */ }
}

class ResourceRequest extends Deferred {
  constructor(ttl) {
    super()
    this._creationTimestamp = Date.now()
    this._timeout = null
    if (ttl !== undefined) this.setTimeout(ttl)
  }

  setTimeout(delay) {
    if (this._state !== ResourceRequest.PENDING) return
    const ttl = parseInt(delay, 10)
    if (isNaN(ttl) || ttl <= 0) throw new Error('delay must be a positive int')
    const age = Date.now() - this._creationTimestamp
    if (this._timeout) this.removeTimeout()
    this._timeout = setTimeout(() => this._fireTimeout(), Math.max(ttl - age, 0))
  }

  removeTimeout() {
    if (this._timeout) clearTimeout(this._timeout)
    this._timeout = null
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

class Pool extends require('events').EventEmitter {
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
    this._availableObjects = new Deque()
    this._resourceLoans = new Map()
    this._allObjects = new Set()
    this._evictor = new DefaultEvictor()
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
    factoryPromise
      .then(resource => {
        const pooledResource = new PooledResource(resource)
        this._allObjects.add(pooledResource)
        this._addPooledResourceToAvailableObjects(pooledResource)
        this._dispense()
      })
      .catch(reason => {
        this.emit('factoryCreateError', reason)
        this._dispense()
      })
  }

  _addPooledResourceToAvailableObjects(pooledResource) {
    pooledResource.idle()
    if (this.options.fifo) {
      this._availableObjects.push(pooledResource)
    } else {
      this._availableObjects.unshift(pooledResource)
    }
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
    for (let i = 0; actualNumberOfResourcesToCreate > i; i++) {
      this._createResource()
    }
    if (this.options.testOnBorrow) {
      const desiredNumberOfResourcesToMoveIntoTest = numWaitingClients - this._testOnBorrowResources.size
      const actualNumberOfResourcesToMoveIntoTest = Math.min(this._availableObjects.length, desiredNumberOfResourcesToMoveIntoTest)
      for (let i = 0; actualNumberOfResourcesToMoveIntoTest > i; i++) {
        this._testOnBorrow()
      }
    } else {
      const actualNumberOfResourcesToDispatch = Math.min(this._availableObjects.length, numWaitingClients)
      for (let i = 0; actualNumberOfResourcesToDispatch > i; i++) {
        this._dispatchResource()
      }
    }
  }

  _testOnBorrow() {
    if (this._availableObjects.length < 1) return false
    const pooledResource = this._availableObjects.shift()
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
    if (this._availableObjects.length < 1) return false
    const pooledResource = this._availableObjects.shift()
    this._dispatchPooledResourceToNextWaitingClient(pooledResource)
    return false
  }

  _evict() {
    const testsToRun = Math.min(this.options.numTestsPerEvictionRun, this._availableObjects.length)
    const evictionConfig = {
      softIdleTimeoutMillis: this.options.softIdleTimeoutMillis,
      idleTimeoutMillis: this.options.idleTimeoutMillis,
      min: this.options.min,
    }
    for (let testsHaveRun = 0; testsHaveRun < testsToRun;) {
      const resource = this._availableObjects.shift()
      const shouldEvict = this._evictor.evict(evictionConfig, resource, this._availableObjects.length)
      testsHaveRun++
      if (shouldEvict) {
        this._destroy(resource)
      } else {
        this._availableObjects.push(resource)
      }
    }
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
      this._availableObjects.length < 1 &&
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
    return this._availableObjects.length + this._testOnBorrowResources.size + this._testOnReturnResources.size + this._factoryCreateOperations.size
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
    return this._availableObjects.length
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
