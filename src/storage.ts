import { 
  collection, 
  getDocs, 
  setDoc, 
  doc, 
  deleteDoc, 
  onSnapshot, 
  query, 
  orderBy,
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
      const querySnapshot = await getDocs(collection(db, path));
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
      const querySnapshot = await getDocs(collection(db, path));
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
      const q = query(collection(db, path), orderBy('createdAt', 'desc'));
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
  subscribeClients(callback: (clients: Client[]) => void) {
    const path = 'clients';
    return onSnapshot(collection(db, path), (snapshot) => {
      callback(snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() } as Client)));
    }, (error) => {
      handleFirestoreError(error, OperationType.GET, path);
    });
  },
  subscribeProducts(callback: (products: Product[]) => void) {
    const path = 'products';
    return onSnapshot(collection(db, path), (snapshot) => {
      callback(snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() } as Product)));
    }, (error) => {
      handleFirestoreError(error, OperationType.GET, path);
    });
  },
  subscribeOrders(callback: (orders: Order[]) => void) {
    const path = 'orders';
    const q = query(collection(db, path), orderBy('createdAt', 'desc'));
    return onSnapshot(q, (snapshot) => {
      callback(snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() } as Order)));
    }, (error) => {
      handleFirestoreError(error, OperationType.GET, path);
    });
  },

  subscribeOperators(callback: (operators: Operator[]) => void) {
    const path = 'operators';
    return onSnapshot(collection(db, path), (snapshot) => {
      callback(snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() } as Operator)));
    }, (error) => {
      handleFirestoreError(error, OperationType.GET, path);
    });
  },

  // Connection Test
  async testConnection() {
    try {
      await getDocFromServer(doc(db, 'test', 'connection'));
    } catch (error) {
      if(error instanceof Error && error.message.includes('the client is offline')) {
        console.error("Please check your Firebase configuration. ");
      }
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
  }
};
