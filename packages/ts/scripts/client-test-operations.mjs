const ITEM_COLUMNS = ['id', 'label', 'amount', 'payload']

export const sharedOperations = {
  reads: {
    itemById: {
      args: ['id'],
      columns: ITEM_COLUMNS,
      statement: args => ({ sql: 'SELECT id, label, amount, payload FROM items WHERE id = ?', params: [args.id] }),
    },
    itemsInOrder: {
      columns: ITEM_COLUMNS,
      statement: () => ({ sql: 'SELECT id, label, amount, payload FROM items ORDER BY id' }),
    },
  },
  writes: {
    putItem: {
      args: ['id', 'label', 'amount', 'payload'],
      statements: args => ({
        sql: 'INSERT INTO items (id, label, amount, payload) VALUES (?, ?, ?, ?)',
        params: [args.id, args.label, args.amount, args.payload],
      }),
    },
    removeItem: {
      args: ['id'],
      statements: args => [{ sql: 'DELETE FROM items WHERE id = ?', params: [args.id] }],
    },
  },
}
