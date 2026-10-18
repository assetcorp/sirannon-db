const offsetMs = Number(process.env.SIRANNON_FAILOVER_CLOCK_OFFSET_MS ?? '0')

if (Number.isFinite(offsetMs) && offsetMs !== 0) {
  const SystemDate = globalThis.Date
  const systemNow = SystemDate.now.bind(SystemDate)
  const shiftedNow = (): number => systemNow() + offsetMs

  globalThis.Date = new Proxy(SystemDate, {
    construct(target, args, newTarget) {
      return Reflect.construct(target, args.length === 0 ? [shiftedNow()] : args, newTarget)
    },
    apply() {
      return new SystemDate(shiftedNow()).toString()
    },
    get(target, property, receiver) {
      if (property === 'now') return shiftedNow
      return Reflect.get(target, property, receiver)
    },
  })
}
