import { z } from 'zod'

export const productSchema = z.object({
  id: z.number().int().nonnegative(),
  name: z.string().min(1).max(80),
  price: z.number().finite().positive(),
  stock: z.number().int().nonnegative(),
})

export const activityRecordSchema = z.object({
  id: z.number().int().nonnegative(),
  product_name: z.string().min(1).max(80),
  action: z.enum(['allocated', 'received', 'created']),
  quantity: z.number().int().nonnegative(),
  created_at: z.string().min(1),
})

export const addProductInputSchema = z.object({
  name: z.string().trim().min(1).max(80),
  price: z.number().finite().positive().max(100_000),
  stock: z.number().int().min(0).max(100_000),
})

export const productIdInputSchema = z.object({
  id: z.number().int().positive(),
  name: z.string().min(1).max(80),
})

export const receiveInventoryInputSchema = productIdInputSchema.extend({
  quantity: z.number().int().min(1).max(1_000),
})

export const inventorySnapshotSchema = z.object({
  products: z.array(productSchema),
  activity: z.array(activityRecordSchema),
})

export type AddProductInput = z.infer<typeof addProductInputSchema>
export type ProductActionInput = z.infer<typeof productIdInputSchema>
export type ReceiveInventoryInput = z.infer<typeof receiveInventoryInputSchema>
export type Product = z.infer<typeof productSchema>
export type ActivityRecord = z.infer<typeof activityRecordSchema>
export type InventorySnapshot = z.infer<typeof inventorySnapshotSchema>
