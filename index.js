const EventEmitter = require("events").EventEmitter

const PooledResourceStateEnum = {
  ALLOCATED: "ALLOCATED", // In use
  IDLE: "IDLE", // In the queue, not in use.
  INVALID: "INVALID", // Failed validation
  RETURNING: "RETURNING", // Resource is in process of returning
  VALIDATION: "VALIDATION" // Currently being tested
}
class Deque {
  constructor() {
    this._list = new DoublyLinkedList()
  }

  /**
   * removes and returns the first element from the queue
   * @return {any} [description]
   */
  shift() {
    if (this.length === 0) {
      return undefined
    }

    const node = this._list.head
    this._list.remove(node)

    return node.data
  }

  /**
   * adds one elemts to the beginning of the queue
   * @param  {any} element [description]
   * @return {any}         [description]
   */
  unshift(element) {
    const node = DoublyLinkedList.createNode(element)

    this._list.insertBeginning(node)
  }

  /**
   * adds one to the end of the queue
   * @param  {any} element [description]
   * @return {any}         [description]
   */
  push(element) {
    const node = DoublyLinkedList.createNode(element)

    this._list.insertEnd(node)
  }

  /**
   * removes and returns the last element from the queue
   */
  pop() {
    if (this.length === 0) {
      return undefined
    }

    const node = this._list.tail
    this._list.remove(node)

    return node.data
  }

  [Symbol.iterator]() {
    return new DequeIterator(this._list)
  }

  iterator() {
    return new DequeIterator(this._list)
  }

  reverseIterator() {
    return new DequeIterator(this._list, true)
  }

  /**
   * get a reference to the item at the head of the queue
   * @return {any} [description]
   */
  get head() {
    if (this.length === 0) {
      return undefined
    }
    const node = this._list.head
    return node.data
  }

  /**
   * get a reference to the item at the tail of the queue
   * @return {any} [description]
   */
  get tail() {
    if (this.length === 0) {
      return undefined
    }
    const node = this._list.tail
    return node.data
  }

  get length() {
    return this._list.length
  }
}

class DoublyLinkedListIterator {
  constructor(doublyLinkedList, reverse) {
    this._list = doublyLinkedList
    this._direction = reverse === true ? "prev" : "next"
    this._startPosition = reverse === true ? "tail" : "head"
    this._started = false
    this._cursor = null
    this._done = false
  }

  _start() {
    this._cursor = this._list[this._startPosition]
    this._started = true
  }

  _advanceCursor() {
    if (this._started === false) {
      this._started = true
      this._cursor = this._list[this._startPosition]
      return
    }
    this._cursor = this._cursor[this._direction]
  }

  reset() {
    this._done = false
    this._started = false
    this._cursor = null
  }

  remove() {
    if (
      this._started === false ||
      this._done === true ||
      this._isCursorDetached()
    ) {
      return false
    }
    this._list.remove(this._cursor)
  }

  next() {
    if (this._done === true) {
      return { done: true }
    }

    this._advanceCursor()

    // if there is no node at the cursor or the node at the cursor is no longer part of
    // a doubly linked list then we are done
    if (this._cursor === null || this._isCursorDetached()) {
      this._done = true
      return { done: true }
    }

    return {
      value: this._cursor,
      done: false
    }
  }

  /**
   * Is the node detached from a list?
   * NOTE: you can trick/bypass/confuse this check by removing a node from one DoublyLinkedList
   * and adding it to another.
   * TODO: We can make this smarter by checking the direction of travel and only checking
   * the required next/prev/head/tail rather than all of them
   * @return {Boolean}      [description]
   */
  _isCursorDetached() {
    return (
      this._cursor.prev === null &&
      this._cursor.next === null &&
      this._list.tail !== this._cursor &&
      this._list.head !== this._cursor
    )
  }
}


class PoolDefaults {
  constructor() {
    this.fifo = true
    this.priorityRange = 1

    this.testOnBorrow = false
    this.testOnReturn = false

    this.autostart = true

    this.evictionRunIntervalMillis = 0
    this.numTestsPerEvictionRun = 3
    this.softIdleTimeoutMillis = -1
    this.idleTimeoutMillis = 30000

    // FIXME: no defaults!
    this.acquireTimeoutMillis = null
    this.destroyTimeoutMillis = null
    this.maxWaitingClients = null

    this.min = null
    this.max = null
    // FIXME: this seems odd?
    this.Promise = Promise
  }
}

class PooledResource {
  constructor(resource) {
    this.creationTime = Date.now()
    this.lastReturnTime = null
    this.lastBorrowTime = null
    this.lastIdleTime = null
    this.obj = resource
    this.state = PooledResourceStateEnum.IDLE
  }

  allocate() {
    this.lastBorrowTime = Date.now()
    this.state = PooledResourceStateEnum.ALLOCATED
  }

  deallocate() {
    this.lastReturnTime = Date.now()
    this.state = PooledResourceStateEnum.IDLE
  }

  invalidate() {
    this.state = PooledResourceStateEnum.INVALID
  }

  test() {
    this.state = PooledResourceStateEnum.VALIDATION
  }

  idle() {
    this.lastIdleTime = Date.now()
    this.state = PooledResourceStateEnum.IDLE
  }

  returning() {
    this.state = PooledResourceStateEnum.RETURNING
  }
}
class DoublyLinkedList {
  constructor() {
    this.head = null
    this.tail = null
    this.length = 0
  }

  insertBeginning(node) {
    if (this.head === null) {
      this.head = node
      this.tail = node
      node.prev = null
      node.next = null
      this.length++
    } else {
      this.insertBefore(this.head, node)
    }
  }

  insertEnd(node) {
    if (this.tail === null) {
      this.insertBeginning(node)
    } else {
      this.insertAfter(this.tail, node)
    }
  }

  insertAfter(node, newNode) {
    newNode.prev = node
    newNode.next = node.next
    if (node.next === null) {
      this.tail = newNode
    } else {
      node.next.prev = newNode
    }
    node.next = newNode
    this.length++
  }

  insertBefore(node, newNode) {
    newNode.prev = node.prev
    newNode.next = node
    if (node.prev === null) {
      this.head = newNode
    } else {
      node.prev.next = newNode
    }
    node.prev = newNode
    this.length++
  }

  remove(node) {
    if (node.prev === null) {
      this.head = node.next
    } else {
      node.prev.next = node.next
    }
    if (node.next === null) {
      this.tail = node.prev
    } else {
      node.next.prev = node.prev
    }
    node.prev = null
    node.next = null
    this.length--
  }

  // FIXME: this should not live here and has become a dumping ground...
  static createNode(data) {
    return { prev: null, next: null, data }
  }
}

const factoryValidator = function(factory) {
  if (typeof factory.create !== "function") {
    throw new TypeError("factory.create must be a function")
  }

  if (typeof factory.destroy !== "function") {
    throw new TypeError("factory.destroy must be a function")
  }

  if (
    typeof factory.validate !== "undefined" &&
    typeof factory.validate !== "function"
  ) {
    throw new TypeError("factory.validate must be a function")
  }
}
class DefaultEvictor {
  evict(config, pooledResource, availableObjectsCount) {
    const idleTime = Date.now() - pooledResource.lastIdleTime

    if (
      config.softIdleTimeoutMillis > 0 &&
      config.softIdleTimeoutMillis < idleTime &&
      config.min < availableObjectsCount
    ) {
      return true
    }

    if (config.idleTimeoutMillis < idleTime) {
      return true
    }

    return false
  }
}
class DequeIterator extends DoublyLinkedListIterator {
  next() {
    const result = super.next()

    // unwrap the node...
    if (result.value) {
      result.value = result.value.data
    }

    return result
  }
}
function noop() {}
const reflector = function(promise) {
  return promise.then(noop, noop)
}

class ResourceLoan extends Deferred {
  constructor(pooledResource, Promise) {
    super(Promise)
    this._creationTimestamp = Date.now()
    this.pooledResource = pooledResource
  }

  reject() { /** Loans can only be resolved at the moment */ }
}

class Deferred {
  constructor(Promise) {
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
    if (this._state !== Deferred.PENDING) {
      return
    }
    this._state = Deferred.REJECTED
    this._reject(reason)
  }

  resolve(value) {
    if (this._state !== Deferred.PENDING) {
      return
    }
    this._state = Deferred.FULFILLED
    this._resolve(value)
  }
}

// TODO: should these really live here? or be a seperate 'state' enum
Deferred.PENDING = "PENDING"
Deferred.FULFILLED = "FULFILLED"
Deferred.REJECTED = "REJECTED"


class TimeoutError extends Error {
  constructor(message) {
    super(message)
    this.name = this.constructor.name
    this.message = message
    if (typeof Error.captureStackTrace === "function") {
      Error.captureStackTrace(this, this.constructor)
    } else {
      this.stack = new Error(message).stack
    }
  }
}

function fbind(fn, ctx) {
  return function bound() {
    return fn.apply(ctx, arguments)
  }
}

/**
 * Wraps a users request for a resource
 * Basically a promise mashed in with a timeout
 * @private
 */
class ResourceRequest extends Deferred {
  /**
   * [constructor description]
   * @param  {Number} ttl     timeout
   */
  constructor(ttl, Promise) {
    super(Promise)
    this._creationTimestamp = Date.now()
    this._timeout = null

    if (ttl !== undefined) {
      this.setTimeout(ttl)
    }
  }

  setTimeout(delay) {
    if (this._state !== ResourceRequest.PENDING) {
      return
    }
    const ttl = parseInt(delay, 10)

    if (isNaN(ttl) || ttl <= 0) {
      throw new Error("delay must be a positive int")
    }

    const age = Date.now() - this._creationTimestamp

    if (this._timeout) {
      this.removeTimeout()
    }

    this._timeout = setTimeout(
      fbind(this._fireTimeout, this),
      Math.max(ttl - age, 0)
    )
  }

  removeTimeout() {
    if (this._timeout) {
      clearTimeout(this._timeout)
    }
    this._timeout = null
  }

  _fireTimeout() {
    this.reject(new TimeoutError("ResourceRequest timed out"))
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

/**
 * Sort of a internal queue for holding the waiting
 * resource requets for a given "priority".
 * Also handles managing timeouts rejections on items (is this the best place for this?)
 * This is the last point where we know which queue a resourceRequest is in
 *
 */
class Queue extends Deque {
  push(resourceRequest) {
    const node = DoublyLinkedList.createNode(resourceRequest)
    resourceRequest.promise.catch(this._createTimeoutRejectionHandler(node))
    this._list.insertEnd(node)
  }

  _createTimeoutRejectionHandler(node) {
    return reason => {
      if (reason.name === "TimeoutError") {
        this._list.remove(node)
      }
    }
  }
}

class PriorityQueue {
  constructor(size) {
    this._size = Math.max(+size | 0, 1)
    /** @type {Queue[]} */
    this._slots = []
    // initialize arrays to hold queue elements
    for (let i = 0 i < this._size i++) {
      this._slots.push(new Queue())
    }
  }

  get length() {
    let _length = 0
    for (let i = 0, slots = this._slots.length i < slots i++) {
      _length += this._slots[i].length
    }
    return _length
  }

  enqueue(obj, priority) {
    // Convert to integer with a default value of 0.
    priority = (priority && +priority | 0) || 0

    if (priority) {
      if (priority < 0 || priority >= this._size) {
        priority = this._size - 1
        // put obj at the end of the line
      }
    }
    this._slots[priority].push(obj)
  }

  dequeue() {
    for (let i = 0, sl = this._slots.length i < sl i += 1) {
      if (this._slots[i].length) {
        return this._slots[i].shift()
      }
    }
    return
  }

  get head() {
    for (let i = 0, sl = this._slots.length i < sl i += 1) {
      if (this._slots[i].length > 0) {
        return this._slots[i].head
      }
    }
    return
  }

  get tail() {
    for (let i = this._slots.length - 1 i >= 0 i--) {
      if (this._slots[i].length > 0) {
        return this._slots[i].tail
      }
    }
    return
  }
}

class PoolOptions {
  /**
   * @param {Object} opts
   *   configuration for the pool
   * @param {Number} [opts.max=null]
   *   Maximum number of items that can exist at the same time.  Default: 1.
   *   Any further acquire requests will be pushed to the waiting list.
   * @param {Number} [opts.min=null]
   *   Minimum number of items in pool (including in-use). Default: 0.
   *   When the pool is created, or a resource destroyed, this minimum will
   *   be checked. If the pool resource count is below the minimum, a new
   *   resource will be created and added to the pool.
   * @param {Number} [opts.maxWaitingClients=null]
   *   maximum number of queued requests allowed after which acquire calls will be rejected
   * @param {Boolean} [opts.testOnBorrow=false]
   *   should the pool validate resources before giving them to clients. Requires that
   *   `factory.validate` is specified.
   * @param {Boolean} [opts.testOnReturn=false]
   *   should the pool validate resources before returning them to the pool.
   * @param {Number} [opts.acquireTimeoutMillis=null]
   *   Delay in milliseconds after which the an `acquire` call will fail. optional.
   *   Default: undefined. Should be positive and non-zero
   * @param {Number} [opts.destroyTimeoutMillis=null]
   *   Delay in milliseconds after which the an `destroy` call will fail, causing it to emit a factoryDestroyError event. optional.
   *   Default: undefined. Should be positive and non-zero
   * @param {Number} [opts.priorityRange=1]
   *   The range from 1 to be treated as a valid priority
   * @param {Boolean} [opts.fifo=true]
   *   Sets whether the pool has LIFO (last in, first out) behaviour with respect to idle objects.
   *   if false then pool has FIFO behaviour
   * @param {Boolean} [opts.autostart=true]
   *   Should the pool start creating resources etc once the constructor is called
   * @param {Number} [opts.evictionRunIntervalMillis=0]
   *   How often to run eviction checks.  Default: 0 (does not run).
   * @param {Number} [opts.numTestsPerEvictionRun=3]
   *   Number of resources to check each eviction run.  Default: 3.
   * @param {Number} [opts.softIdleTimeoutMillis=-1]
   *   amount of time an object may sit idle in the pool before it is eligible
   *   for eviction by the idle object evictor (if any), with the extra condition
   *   that at least "min idle" object instances remain in the pool. Default -1 (nothing can get evicted)
   * @param {Number} [opts.idleTimeoutMillis=30000]
   *   the minimum amount of time that an object may sit idle in the pool before it is eligible for eviction
   *   due to idle time. Supercedes "softIdleTimeoutMillis" Default: 30000
   * @param {typeof Promise} [opts.Promise=Promise]
   *   What promise implementation should the pool use, defaults to native promises.
   */
  constructor(opts) {
    const poolDefaults = new PoolDefaults()

    opts = opts || {}

    this.fifo = typeof opts.fifo === "boolean" ? opts.fifo : poolDefaults.fifo
    this.priorityRange = opts.priorityRange || poolDefaults.priorityRange

    this.testOnBorrow =
      typeof opts.testOnBorrow === "boolean"
        ? opts.testOnBorrow
        : poolDefaults.testOnBorrow
    this.testOnReturn =
      typeof opts.testOnReturn === "boolean"
        ? opts.testOnReturn
        : poolDefaults.testOnReturn

    this.autostart =
      typeof opts.autostart === "boolean"
        ? opts.autostart
        : poolDefaults.autostart

    if (opts.acquireTimeoutMillis) {
      // @ts-ignore
      this.acquireTimeoutMillis = parseInt(opts.acquireTimeoutMillis, 10)
    }

    if (opts.destroyTimeoutMillis) {
      this.destroyTimeoutMillis = parseInt(opts.destroyTimeoutMillis, 10)
    }

    if (opts.maxWaitingClients !== undefined) {
      this.maxWaitingClients = parseInt(opts.maxWaitingClients, 10)
    }
    this.max = parseInt(opts.max, 10)
    this.min = parseInt(opts.min, 10)
    this.max = Math.max(isNaN(this.max) ? 1 : this.max, 1)
    this.min = Math.min(isNaN(this.min) ? 0 : this.min, this.max)

    this.evictionRunIntervalMillis =
      opts.evictionRunIntervalMillis || poolDefaults.evictionRunIntervalMillis
    this.numTestsPerEvictionRun =
      opts.numTestsPerEvictionRun || poolDefaults.numTestsPerEvictionRun
    this.softIdleTimeoutMillis =
      opts.softIdleTimeoutMillis || poolDefaults.softIdleTimeoutMillis
    this.idleTimeoutMillis =
      opts.idleTimeoutMillis || poolDefaults.idleTimeoutMillis

    this.Promise = opts.Promise != null ? opts.Promise : poolDefaults.Promise
  }
}

const FACTORY_CREATE_ERROR = "factoryCreateError"
const FACTORY_DESTROY_ERROR = "factoryDestroyError"

class Pool extends EventEmitter {
  constructor(Evictor, Deque, PriorityQueue, factory, options) {
    super()
    factoryValidator(factory)
    this._config = new PoolOptions(options)
    // TODO: fix up this ugly glue-ing
    this._Promise = this._config.Promise
    this._factory = factory
    this._draining = false
    this._started = false
    this._waitingClientsQueue = new PriorityQueue(this._config.priorityRange)
    this._factoryCreateOperations = new Set()
    this._factoryDestroyOperations = new Set()
    this._availableObjects = new Deque()
    this._testOnBorrowResources = new Set()
    this._testOnReturnResources = new Set()
    this._validationOperations = new Set()
    this._allObjects = new Set()
    this._resourceLoans = new Map()
    this._evictionIterator = this._availableObjects.iterator()
    this._evictor = new Evictor()
    this._scheduledEviction = null
    if (this._config.autostart === true) {
      this.start()
    }
  }

  _destroy(pooledResource) {
    // FIXME: do we need another state for "in destruction"?
    pooledResource.invalidate()
    this._allObjects.delete(pooledResource)
    // NOTE: this maybe very bad promise usage?
    const destroyPromise = this._factory.destroy(pooledResource.obj)
    const wrappedDestroyPromise = this._config.destroyTimeoutMillis
      ? this._Promise.resolve(this._applyDestroyTimeout(destroyPromise))
      : this._Promise.resolve(destroyPromise)

    this._trackOperation(
      wrappedDestroyPromise,
      this._factoryDestroyOperations
    ).catch(reason => {
      this.emit(FACTORY_DESTROY_ERROR, reason)
    })
    // TODO: maybe ensuring minimum pool size should live outside here
    this._ensureMinimum()
  }
  _applyDestroyTimeout(promise) {
    const timeoutPromise = new this._Promise((resolve, reject) => {
      setTimeout(() => {
        reject(new Error("destroy timed out"))
      }, this._config.destroyTimeoutMillis).unref()
    })
    return this._Promise.race([timeoutPromise, promise])
  }
  _testOnBorrow() {
    if (this._availableObjects.length < 1) {
      return false
    }

    const pooledResource = this._availableObjects.shift()
    // Mark the resource as in test
    pooledResource.test()
    this._testOnBorrowResources.add(pooledResource)
    const validationPromise = this._factory.validate(pooledResource.obj)
    const wrappedValidationPromise = this._Promise.resolve(validationPromise)

    this._trackOperation(
      wrappedValidationPromise,
      this._validationOperations
    ).then(isValid => {
      this._testOnBorrowResources.delete(pooledResource)

      if (isValid === false) {
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
    if (this._availableObjects.length < 1) {
      return false
    }

    const pooledResource = this._availableObjects.shift()
    this._dispatchPooledResourceToNextWaitingClient(pooledResource)
    return false
  }
  _dispense() {
    /**
     * Local variables for ease of reading/writing
     * these don't (shouldn't) change across the execution of this fn
     */
    const numWaitingClients = this._waitingClientsQueue.length

    // If there aren't any waiting requests then there is nothing to do
    // so lets short-circuit
    if (numWaitingClients < 1) {
      return
    }

    const resourceShortfall =
      numWaitingClients - this._potentiallyAllocableResourceCount

    const actualNumberOfResourcesToCreate = Math.min(
      this.spareResourceCapacity,
      resourceShortfall
    )
    for (let i = 0 actualNumberOfResourcesToCreate > i i++) {
      this._createResource()
    }

    // If we are doing test-on-borrow see how many more resources need to be moved into test
    // to help satisfy waitingClients
    if (this._config.testOnBorrow === true) {
      // how many available resources do we need to shift into test
      const desiredNumberOfResourcesToMoveIntoTest =
        numWaitingClients - this._testOnBorrowResources.size
      const actualNumberOfResourcesToMoveIntoTest = Math.min(
        this._availableObjects.length,
        desiredNumberOfResourcesToMoveIntoTest
      )
      for (let i = 0 actualNumberOfResourcesToMoveIntoTest > i i++) {
        this._testOnBorrow()
      }
    }

    // if we aren't testing-on-borrow then lets try to allocate what we can
    if (this._config.testOnBorrow === false) {
      const actualNumberOfResourcesToDispatch = Math.min(
        this._availableObjects.length,
        numWaitingClients
      )
      for (let i = 0 actualNumberOfResourcesToDispatch > i i++) {
        this._dispatchResource()
      }
    }
  }
  _dispatchPooledResourceToNextWaitingClient(pooledResource) {
    const clientResourceRequest = this._waitingClientsQueue.dequeue()
    if (
      clientResourceRequest === undefined ||
      clientResourceRequest.state !== Deferred.PENDING
    ) {
      this._addPooledResourceToAvailableObjects(pooledResource)
      // TODO: do need to trigger anything before we leave?
      return false
    }
    const loan = new ResourceLoan(pooledResource, this._Promise)
    this._resourceLoans.set(pooledResource.obj, loan)
    pooledResource.allocate()
    clientResourceRequest.resolve(pooledResource.obj)
    return true
  }
  _trackOperation(operation, set) {
    set.add(operation)

    return operation.then(
      v => {
        set.delete(operation)
        return this._Promise.resolve(v)
      },
      e => {
        set.delete(operation)
        return this._Promise.reject(e)
      }
    )
  }
  _createResource() {
    // An attempt to create a resource
    const factoryPromise = this._factory.create()
    const wrappedFactoryPromise = this._Promise
      .resolve(factoryPromise)
      .then(resource => {
        const pooledResource = new PooledResource(resource)
        this._allObjects.add(pooledResource)
        this._addPooledResourceToAvailableObjects(pooledResource)
      })

    this._trackOperation(wrappedFactoryPromise, this._factoryCreateOperations)
      .then(() => {
        this._dispense()
        // Stop bluebird complaining about this side-effect only handler
        // - a promise was created in a handler but was not returned from it
        // https://goo.gl/rRqMUw
        return null
      })
      .catch(reason => {
        this.emit(FACTORY_CREATE_ERROR, reason)
        this._dispense()
      })
  }
  _ensureMinimum() {
    if (this._draining === true) {
      return
    }
    const minShortfall = this._config.min - this._count
    for (let i = 0 i < minShortfall i++) {
      this._createResource()
    }
  }

  _evict() {
    const testsToRun = Math.min(
      this._config.numTestsPerEvictionRun,
      this._availableObjects.length
    )
    const evictionConfig = {
      softIdleTimeoutMillis: this._config.softIdleTimeoutMillis,
      idleTimeoutMillis: this._config.idleTimeoutMillis,
      min: this._config.min
    }
    for (let testsHaveRun = 0 testsHaveRun < testsToRun ) {
      const iterationResult = this._evictionIterator.next()

      // Safety check incase we could get stuck in infinite loop because we
      // somehow emptied the array after checking its length.
      if (iterationResult.done === true && this._availableObjects.length < 1) {
        this._evictionIterator.reset()
        return
      }
      // If this happens it should just mean we reached the end of the
      // list and can reset the cursor.
      if (iterationResult.done === true && this._availableObjects.length > 0) {
        this._evictionIterator.reset()
        continue
      }

      const resource = iterationResult.value

      const shouldEvict = this._evictor.evict(
        evictionConfig,
        resource,
        this._availableObjects.length
      )
      testsHaveRun++

      if (shouldEvict === true) {
        // take it out of the _availableObjects list
        this._evictionIterator.remove()
        this._destroy(resource)
      }
    }
  }

  _scheduleEvictorRun() {
    // Start eviction if set
    if (this._config.evictionRunIntervalMillis > 0) {
      // @ts-ignore
      this._scheduledEviction = setTimeout(() => {
        this._evict()
        this._scheduleEvictorRun()
      }, this._config.evictionRunIntervalMillis).unref()
    }
  }

  _descheduleEvictorRun() {
    if (this._scheduledEviction) {
      clearTimeout(this._scheduledEviction)
    }
    this._scheduledEviction = null
  }

  start() {
    if (this._draining === true) {
      return
    }
    if (this._started === true) {
      return
    }
    this._started = true
    this._scheduleEvictorRun()
    this._ensureMinimum()
  }
  /**
   * Request a new resource. The callback will be called,
   * when a new resource is available, passing the resource to the callback.
   * TODO: should we add a seperate "acquireWithPriority" function?
   */
  acquire(priority) {
    if (this._started === false && this._config.autostart === false) {
      this.start()
    }

    if (this._draining) {
      return this._Promise.reject(
        new Error("pool is draining and cannot accept work")
      )
    }

    // TODO: should we defer this check till after this event loop incase "the situation" changes in the meantime
    if (
      this.spareResourceCapacity < 1 &&
      this._availableObjects.length < 1 &&
      this._config.maxWaitingClients !== undefined &&
      this._waitingClientsQueue.length >= this._config.maxWaitingClients
    ) {
      return this._Promise.reject(
        new Error("max waitingClients count exceeded")
      )
    }

    const resourceRequest = new ResourceRequest(
      this._config.acquireTimeoutMillis,
      this._Promise
    )
    this._waitingClientsQueue.enqueue(resourceRequest, priority)
    this._dispense()

    return resourceRequest.promise
  }
  use(fn, priority) {
    return this.acquire(priority).then(resource => {
      return fn(resource).then(
        result => {
          this.release(resource)
          return result
        },
        err => {
          this.destroy(resource)
          throw err
        }
      )
    })
  }
  isBorrowedResource(resource) {
    return this._resourceLoans.has(resource)
  }
  release(resource) {
    // check for an outstanding loan
    const loan = this._resourceLoans.get(resource)

    if (loan === undefined) {
      return this._Promise.reject(
        new Error("Resource not currently part of this pool")
      )
    }

    this._resourceLoans.delete(resource)
    loan.resolve()
    const pooledResource = loan.pooledResource

    pooledResource.deallocate()
    this._addPooledResourceToAvailableObjects(pooledResource)

    this._dispense()
    return this._Promise.resolve()
  }
  destroy(resource) {
    // check for an outstanding loan
    const loan = this._resourceLoans.get(resource)

    if (loan === undefined) {
      return this._Promise.reject(
        new Error("Resource not currently part of this pool")
      )
    }

    this._resourceLoans.delete(resource)
    loan.resolve()
    const pooledResource = loan.pooledResource

    pooledResource.deallocate()
    this._destroy(pooledResource)

    this._dispense()
    return this._Promise.resolve()
  }

  _addPooledResourceToAvailableObjects(pooledResource) {
    pooledResource.idle()
    if (this._config.fifo === true) {
      this._availableObjects.push(pooledResource)
    } else {
      this._availableObjects.unshift(pooledResource)
    }
  }
  drain() {
    this._draining = true
    return this.__allResourceRequestsSettled()
      .then(() => {
        return this.__allResourcesReturned()
      })
      .then(() => {
        this._descheduleEvictorRun()
      })
  }

  __allResourceRequestsSettled() {
    if (this._waitingClientsQueue.length > 0) {
      // wait for last waiting client to be settled
      // FIXME: what if they can "resolve" out of order....?
      return reflector(this._waitingClientsQueue.tail.promise)
    }
    return this._Promise.resolve()
  }

  // FIXME: this is a horrific mess
  __allResourcesReturned() {
    const ps = Array.from(this._resourceLoans.values())
      .map(loan => loan.promise)
      .map(reflector)
    return this._Promise.all(ps)
  }
  clear() {
    const reflectedCreatePromises = Array.from(
      this._factoryCreateOperations
    ).map(reflector)

    // wait for outstanding factory.create to complete
    return this._Promise.all(reflectedCreatePromises).then(() => {
      // Destroy existing resources
      // @ts-ignore
      for (const resource of this._availableObjects) {
        this._destroy(resource)
      }
      const reflectedDestroyPromises = Array.from(
        this._factoryDestroyOperations
      ).map(reflector)
      return reflector(this._Promise.all(reflectedDestroyPromises))
    })
  }
  ready() {
    return new this._Promise(resolve => {
      const isReady = () => {
        if (this.available >= this.min) {
          resolve()
        } else {
          setTimeout(isReady, 100)
        }
      }

      isReady()
    })
  }
  get _potentiallyAllocableResourceCount() {
    return (
      this._availableObjects.length +
      this._testOnBorrowResources.size +
      this._testOnReturnResources.size +
      this._factoryCreateOperations.size
    )
  }
  get _count() {
    return this._allObjects.size + this._factoryCreateOperations.size
  }
  get spareResourceCapacity() {
    return (
      this._config.max -
      (this._allObjects.size + this._factoryCreateOperations.size)
    )
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
    return this._config.max
  }
  get min() {
    return this._config.min
  }
}

module.exports = {
  Pool: Pool,
  Deque: Deque,
  PriorityQueue: PriorityQueue,
  DefaultEvictor: DefaultEvictor,
  createPool: function(factory, config) {
    return new Pool(DefaultEvictor, Deque, PriorityQueue, factory, config)
  }
}
