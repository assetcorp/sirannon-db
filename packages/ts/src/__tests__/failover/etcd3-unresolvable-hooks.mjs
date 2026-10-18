export async function resolve(specifier, context, nextResolve) {
  if (specifier === 'etcd3' || specifier.startsWith('etcd3/')) {
    const error = new Error(
      `Cannot find package '${specifier}' imported from ${context.parentURL ?? 'the entry point'}`,
    )
    error.code = 'ERR_MODULE_NOT_FOUND'
    throw error
  }
  return nextResolve(specifier, context)
}
