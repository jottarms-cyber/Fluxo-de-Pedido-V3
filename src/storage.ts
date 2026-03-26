import { 
  collection, 
  getDocs, 
  setDoc, 
  doc, 
  deleteDoc, 
  onSnapshot, 
  query, 
  orderBy,
  limit,
  writeBatch,
  getDocFromServer
} from 'firebase/firestore';
import { db, auth } from './firebase';
import { Client, Product, Order, Operator } from './types';

enum OperationType {
  CREATE = 'create',
  UPDATE = 'update',
  DELETE = 'delete',
  LIST = 'list',
  GET = 'get',
  WRITE = 'write',
}

interface FirestoreErrorInfo {
  error: string;
  operationType: OperationType;
  path: string | null;
  authInfo: {
    userId: string | undefined;
    email: string | null | undefined;
    emailVerified: boolean | undefined;
    isAnonymous: boolean | undefined;
    tenantId: string | null | undefined;
    providerInfo: {
      providerId: string;
      displayName: string | null;
      email: string | null;
      photoUrl: string | null;
    }[];
  }
}

function handleFirestoreError(error: unknown, operationType: OperationType, path: string | null) {
  const errInfo: FirestoreErrorInfo = {
    error: error instanceof Error ? error.message : String(error),
    authInfo: {
      userId: auth.currentUser?.uid,
      email: auth.currentUser?.email,
      emailVerified: auth.currentUser?.emailVerified,
      isAnonymous: auth.currentUser?.isAnonymous,
      tenantId: auth.currentUser?.tenantId,
      providerInfo: auth.currentUser?.providerData.map(provider => ({
        providerId: provider.providerId,
        displayName: provider.displayName,
        email: provider.email,
        photoUrl: provider.photoURL
      })) || []
    },
    operationType,
    path
  }
  console.error('Firestore Error: ', JSON.stringify(errInfo));
  throw new Error(JSON.stringify(errInfo));
}

export const storage = {
  // Clients
  async getClients(): Promise<Client[]> {
    const path = 'clients';
    try {
      const q = query(collection(db, path), limit(200));
      const querySnapshot = await getDocs(q);
      return querySnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() } as Client));
    } catch (error) {
      handleFirestoreError(error, OperationType.LIST, path);
      return [];
    }
  },
  async saveClient(client: Client) {
    const path = `clients/${client.id}`;
    try {
      return await setDoc(doc(db, 'clients', client.id), client);
    } catch (error) {
      handleFirestoreError(error, OperationType.WRITE, path);
    }
  },
  async deleteClient(id: string) {
    const path = `clients/${id}`;
    try {
      return await deleteDoc(doc(db, 'clients', id));
    } catch (error) {
      handleFirestoreError(error, OperationType.DELETE, path);
    }
  },
  async saveClients(clients: Client[]) {
    const path = 'clients (batch)';
    try {
      // Split into chunks of 500 for Firestore batch limits
      const chunks = [];
      for (let i = 0; i < clients.length; i += 500) {
        chunks.push(clients.slice(i, i + 500));
      }

      for (const chunk of chunks) {
        const batch = writeBatch(db);
        chunk.forEach(client => {
          const docRef = doc(db, 'clients', client.id);
          batch.set(docRef, client);
        });
        await batch.commit();
      }
      return true;
    } catch (error) {
      handleFirestoreError(error, OperationType.WRITE, path);
    }
  },

  // Products
  async getProducts(): Promise<Product[]> {
    const path = 'products';
    try {
      const q = query(collection(db, path), limit(200));
      const querySnapshot = await getDocs(q);
      return querySnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() } as Product));
    } catch (error) {
      handleFirestoreError(error, OperationType.LIST, path);
      return [];
    }
  },
  async saveProduct(product: Product) {
    console.log('storage.saveProduct called with:', product);
    const path = `products/${product.id}`;
    try {
      return await setDoc(doc(db, 'products', product.id), product);
    } catch (error) {
      handleFirestoreError(error, OperationType.WRITE, path);
    }
  },
  async saveProducts(products: Product[]) {
    const path = 'products (batch)';
    console.log(`storage.saveProducts: Attempting to save ${products.length} products`);
    try {
      // Split into chunks of 500 for Firestore batch limits
      const chunks = [];
      for (let i = 0; i < products.length; i += 500) {
        chunks.push(products.slice(i, i + 500));
      }

      for (const chunk of chunks) {
        const batch = writeBatch(db);
        chunk.forEach(product => {
          const docRef = doc(db, 'products', product.id);
          batch.set(docRef, product);
        });
        await batch.commit();
        console.log(`storage.saveProducts: Committed batch of ${chunk.length} products`);
      }
      return true;
    } catch (error) {
      console.error('storage.saveProducts error:', error);
      handleFirestoreError(error, OperationType.WRITE, path);
    }
  },
  async clearProducts() {
    const path = 'products (clear)';
    try {
      const querySnapshot = await getDocs(collection(db, 'products'));
      const docs = querySnapshot.docs;
      
      // Split into chunks of 500 for Firestore batch limits
      const chunks = [];
      for (let i = 0; i < docs.length; i += 500) {
        chunks.push(docs.slice(i, i + 500));
      }

      for (const chunk of chunks) {
        const batch = writeBatch(db);
        chunk.forEach(doc => {
          batch.delete(doc.ref);
        });
        await batch.commit();
      }
      return true;
    } catch (error) {
      handleFirestoreError(error, OperationType.DELETE, path);
    }
  },

  // Orders
  async getOrders(): Promise<Order[]> {
    const path = 'orders';
    try {
      const q = query(collection(db, path), orderBy('createdAt', 'desc'), limit(100));
      const querySnapshot = await getDocs(q);
      return querySnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() } as Order));
    } catch (error) {
      handleFirestoreError(error, OperationType.LIST, path);
      return [];
    }
  },
  async saveOrder(order: Order) {
    const path = `orders/${order.id}`;
    try {
      return await setDoc(doc(db, 'orders', order.id), order);
    } catch (error) {
      handleFirestoreError(error, OperationType.WRITE, path);
    }
  },
  async deleteOrder(id: string) {
    const path = `orders/${id}`;
    try {
      return await deleteDoc(doc(db, 'orders', id));
    } catch (error) {
      handleFirestoreError(error, OperationType.DELETE, path);
    }
  },
  async deleteProduct(id: string) {
    const path = `products/${id}`;
    try {
      return await deleteDoc(doc(db, 'products', id));
    } catch (error) {
      handleFirestoreError(error, OperationType.DELETE, path);
    }
  },
  async clearClients() {
    const path = 'clients (clear)';
    try {
      const querySnapshot = await getDocs(collection(db, 'clients'));
      const docs = querySnapshot.docs;

      // Split into chunks of 500 for Firestore batch limits
      const chunks = [];
      for (let i = 0; i < docs.length; i += 500) {
        chunks.push(docs.slice(i, i + 500));
      }

      for (const chunk of chunks) {
        const batch = writeBatch(db);
        chunk.forEach(doc => {
          batch.delete(doc.ref);
        });
        await batch.commit();
      }
      return true;
    } catch (error) {
      handleFirestoreError(error, OperationType.DELETE, path);
    }
  },

  async clearOrders() {
    const path = 'orders (clear)';
    try {
      const querySnapshot = await getDocs(collection(db, 'orders'));
      const docs = querySnapshot.docs;

      // Split into chunks of 500 for Firestore batch limits
      const chunks = [];
      for (let i = 0; i < docs.length; i += 500) {
        chunks.push(docs.slice(i, i + 500));
      }

      for (const chunk of chunks) {
        const batch = writeBatch(db);
        chunk.forEach(doc => {
          batch.delete(doc.ref);
        });
        await batch.commit();
      }
      return true;
    } catch (error) {
      handleFirestoreError(error, OperationType.DELETE, path);
    }
  },
  // Real-time Listeners
  subscribeClients(callback: (clients: Client[]) => void, onError?: (error: any) => void) {
    const path = 'clients';
    const q = query(collection(db, path), limit(200));
    return onSnapshot(q, (snapshot) => {
      callback(snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() } as Client)));
    }, (error) => {
      if (onError) onError(error);
      else handleFirestoreError(error, OperationType.GET, path);
    });
  },
  subscribeProducts(callback: (products: Product[]) => void, onError?: (error: any) => void) {
    const path = 'products';
    const q = query(collection(db, path), limit(200));
    return onSnapshot(q, (snapshot) => {
      callback(snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() } as Product)));
    }, (error) => {
      if (onError) onError(error);
      else handleFirestoreError(error, OperationType.GET, path);
    });
  },
  subscribeOrders(callback: (orders: Order[]) => void, onError?: (error: any) => void) {
    const path = 'orders';
    const q = query(query(collection(db, path), orderBy('createdAt', 'desc')), limit(100));
    return onSnapshot(q, (snapshot) => {
      callback(snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() } as Order)));
    }, (error) => {
      if (onError) onError(error);
      else handleFirestoreError(error, OperationType.GET, path);
    });
  },

  subscribeOperators(callback: (operators: Operator[]) => void, onError?: (error: any) => void) {
    const path = 'operators';
    return onSnapshot(collection(db, path), (snapshot) => {
      callback(snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() } as Operator)));
    }, (error) => {
      if (onError) onError(error);
      else handleFirestoreError(error, OperationType.GET, path);
    });
  },

  // Connection Test
  async testConnection() {
    try {
      console.log('Testing connection to Firestore (Long Polling forced)...');
      // Use a simple collection fetch instead of getDocFromServer for better robustness
      const q = query(collection(db, 'test'), limit(1));
      await getDocs(q);
      console.log('Connection test successful.');
    } catch (error) {
      console.error("Firestore connection error details:", error);
      throw error;
    }
  },

  // Operators
  async getOperators(): Promise<Operator[]> {
    const path = 'operators';
    try {
      const querySnapshot = await getDocs(collection(db, path));
      return querySnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() } as Operator));
    } catch (error) {
      handleFirestoreError(error, OperationType.LIST, path);
      return [];
    }
  },
  async saveOperator(operator: Operator) {
    console.log('storage.saveOperator called with:', operator);
    const path = `operators/${operator.id}`;
    try {
      const result = await setDoc(doc(db, 'operators', operator.id), operator);
      console.log('storage.saveOperator success');
      return result;
    } catch (error) {
      console.error('storage.saveOperator error:', error);
      handleFirestoreError(error, OperationType.WRITE, path);
    }
  },
  async deleteOperator(id: string) {
    const path = `operators/${id}`;
    try {
      return await deleteDoc(doc(db, 'operators', id));
    } catch (error) {
      handleFirestoreError(error, OperationType.DELETE, path);
    }
  },

  // Cleanup Utilities
  async deduplicateClients() {
    console.log('Starting client deduplication...');
    try {
      const querySnapshot = await getDocs(collection(db, 'clients'));
      const clients = querySnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() } as Client));
      
      const cnpjMap = new Map<string, string[]>();
      clients.forEach(c => {
        if (!c.cnpj) return;
        if (!cnpjMap.has(c.cnpj)) cnpjMap.set(c.cnpj, []);
        cnpjMap.get(c.cnpj)!.push(c.id);
      });

      let deletedCount = 0;
      for (const [cnpj, ids] of cnpjMap.entries()) {
        if (ids.length > 1) {
          // Keep the first one, delete the rest
          const toDelete = ids.slice(1);
          for (const id of toDelete) {
            await deleteDoc(doc(db, 'clients', id));
            deletedCount++;
          }
        }
      }
      console.log(`Client deduplication finished. Deleted ${deletedCount} duplicates.`);
      return deletedCount;
    } catch (error) {
      console.error('Error deduplicating clients:', error);
      throw error;
    }
  },

  async deduplicateProducts() {
    console.log('Starting product deduplication...');
    try {
      const querySnapshot = await getDocs(collection(db, 'products'));
      const products = querySnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() } as Product));
      
      const codeMap = new Map<string, string[]>();
      products.forEach(p => {
        if (!p.codigoInterno) return;
        if (!codeMap.has(p.codigoInterno)) codeMap.set(p.codigoInterno, []);
        codeMap.get(p.codigoInterno)!.push(p.id);
      });

      let deletedCount = 0;
      for (const [code, ids] of codeMap.entries()) {
        if (ids.length > 1) {
          // Keep the first one, delete the rest
          const toDelete = ids.slice(1);
          for (const id of toDelete) {
            await deleteDoc(doc(db, 'products', id));
            deletedCount++;
          }
        }
      }
      console.log(`Product deduplication finished. Deleted ${deletedCount} duplicates.`);
      return deletedCount;
    } catch (error) {
      console.error('Error deduplicating products:', error);
      throw error;
    }
  }
};
