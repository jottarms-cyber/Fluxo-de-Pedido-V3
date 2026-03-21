import * as React from 'react';
import { useState, useEffect, useRef, useMemo, Component, ErrorInfo, ReactNode } from 'react';
import { 
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Cell,
  PieChart, Pie, Legend
} from 'recharts';
import * as d3 from 'd3';
import { 
  LayoutDashboard, 
  PlusCircle, 
  Kanban, 
  Search, 
  Barcode, 
  Trash2, 
  ChevronRight, 
  Printer,
  CheckCircle2,
  Package,
  PackageSearch,
  Users,
  TrendingUp,
  X,
  FileUp,
  FileDown,
  UserPlus,
  Edit2,
  Save,
  Download,
  FileText,
  Plus,
  Upload,
  FileSpreadsheet,
  LogOut,
  LogIn,
  Scan,
  AlertCircle,
  Link as LinkIcon,
  Loader2,
  ArrowLeft,
  Archive,
  RotateCcw,
  UserCheck,
  Lock
} from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';
import * as XLSX from 'xlsx';
import { jsPDF } from 'jspdf';
import autoTable from 'jspdf-autotable';
import JsBarcode from 'jsbarcode';
import { Html5QrcodeScanner } from 'html5-qrcode';
import { onAuthStateChanged, User } from 'firebase/auth';
import { auth, signInWithGoogle, logout, loginWithEmail, registerWithEmail } from './firebase';
import { Client, Product, Order, OrderItem, OrderStatus, BarcodeType, Operator } from './types';
import { INITIAL_CLIENTS, INITIAL_PRODUCTS, ORDER_STATUSES } from './constants';
import { storage } from './storage';

const BarcodeGenerator = ({ value, className = "" }: { value: string, className?: string }) => {
  const canvasRef = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    if (canvasRef.current && value) {
      try {
        // Ensure value is a string and not empty
        const barcodeValue = String(value).trim();
        if (barcodeValue) {
          JsBarcode(canvasRef.current, barcodeValue, {
            format: "CODE128",
            width: 2,
            height: 40,
            displayValue: true,
            fontSize: 14,
            margin: 2,
            background: "#ffffff"
          });
        }
      } catch (e) {
        console.error("Barcode generation error:", e);
      }
    }
  }, [value]);

  return <canvas ref={canvasRef} className={`block max-w-full ${className}`} />;
};

const AtacadaoLogo = ({ className = "w-12 h-12" }: { className?: string }) => (
  <svg viewBox="0 0 100 100" className={className} xmlns="http://www.w3.org/2000/svg">
    <circle cx="50" cy="50" r="48" fill="#f9d423" />
    <g transform="translate(12, 12) scale(0.76)">
      <path d="M50 0 L0 100 L28 100 L38 75 L62 75 L72 100 L100 100 Z" fill="white" />
      <path d="M50 8 L12 92 L32 92 L40 72 L60 72 L68 92 L88 92 Z" fill="#f37021" />
      <rect x="0" y="36" width="100" height="4" rx="2" fill="#00a651" />
      <rect x="0" y="44" width="100" height="4" rx="2" fill="#00a651" />
      <rect x="0" y="52" width="100" height="4" rx="2" fill="#00a651" />
      <rect x="0" y="60" width="100" height="4" rx="2" fill="#00a651" />
    </g>
    <text 
      x="50" 
      y="90" 
      textAnchor="middle" 
      fill="#00a651" 
      fontSize="16" 
      fontWeight="900" 
      fontStyle="italic"
      fontFamily="Inter, sans-serif"
      letterSpacing="-1"
    >
      ATACADÃO
    </text>
  </svg>
);

// Error Boundary Component
interface ErrorBoundaryProps {
  children: ReactNode;
}

interface ErrorBoundaryState {
  hasError: boolean;
  error: Error | null;
}

export class ErrorBoundary extends (React.Component as any) {
  constructor(props: any) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error: Error) {
    return { hasError: true, error };
  }

  componentDidCatch(error: Error, errorInfo: ErrorInfo) {
    console.error("Uncaught error:", error, errorInfo);
  }

  render() {
    if (this.state.hasError) {
      let errorMessage = "Ocorreu um erro inesperado.";
      try {
        const parsedError = JSON.parse(this.state.error?.message || "");
        if (parsedError.error) {
          errorMessage = `Erro no Firestore: ${parsedError.error} (${parsedError.operationType} em ${parsedError.path})`;
        }
      } catch (e) {
        errorMessage = this.state.error?.message || errorMessage;
      }

      return (
        <div className="min-h-screen flex items-center justify-center bg-slate-50 p-4">
          <div className="bg-white p-8 rounded-[2.5rem] shadow-2xl border border-red-100 max-w-md w-full text-center space-y-6">
            <div className="w-16 h-16 bg-red-100 rounded-2xl flex items-center justify-center text-red-600 mx-auto">
              <AlertCircle size={32} />
            </div>
            <h2 className="text-2xl font-black text-slate-900 uppercase italic">Ops! Algo deu errado</h2>
            <p className="text-slate-600 font-bold">{errorMessage}</p>
            <button 
              onClick={() => window.location.reload()}
              className="w-full bg-brand-orange text-white py-4 rounded-2xl font-black uppercase tracking-widest text-xs hover:bg-brand-orange/90 transition-all shadow-lg shadow-brand-orange/20"
            >
              Recarregar Aplicativo
            </button>
          </div>
        </div>
      );
    }

    return this.props.children;
  }
}

export default function App() {
  const [user, setUser] = useState<User | null>(null);
  const [authReady, setAuthReady] = useState(false);
  const [isRegistering, setIsRegistering] = useState(false);
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [authError, setAuthError] = useState('');
  const [activeTab, setActiveTab] = useState<'dashboard' | 'new-order' | 'kanban' | 'clients' | 'products' | 'import-export' | 'registrations'>('dashboard');
  const [orders, setOrders] = useState<Order[]>([]);
  const [clients, setClients] = useState<Client[]>([]);
  const [products, setProducts] = useState<Product[]>([]);
  const [operators, setOperators] = useState<Operator[]>([]);

  // Barcode Linking State
  const [linkingProduct, setLinkingProduct] = useState<Product | null>(null);
  const [newBarcode, setNewBarcode] = useState('');
  const [linkSearch, setLinkSearch] = useState('');
  const [linkingBarcodeType, setLinkingBarcodeType] = useState<BarcodeType>('unidade');
  const [isSavingLink, setIsSavingLink] = useState(false);
  const [linkError, setLinkError] = useState<string | null>(null);
  const [linkSuccess, setLinkSuccess] = useState(false);

  // New Order State
  const [selectedClient, setSelectedClient] = useState<Client | null>(null);
  const [cnpjInput, setCnpjInput] = useState('');
  const [barcodeInput, setBarcodeInput] = useState('');
  const [isScanning, setIsScanning] = useState(false);
  const [scannedNotFound, setScannedNotFound] = useState<string | null>(null);
  const [isLinkingFromOrder, setIsLinkingFromOrder] = useState(false);
  const [currentProduct, setCurrentProduct] = useState<Product | null>(null);
  const [orderItems, setOrderItems] = useState<OrderItem[]>([]);
  const [itemQty, setItemQty] = useState(1);
  const [itemUnit, setItemUnit] = useState<BarcodeType>('unidade');
  const [nomeQuemFezPedido, setNomeQuemFezPedido] = useState('');
  const [itemPrice, setItemPrice] = useState<number>(0);
  const [itemMultiplier, setItemMultiplier] = useState<number>(1);

  // Client Form State
  const [isClientModalOpen, setIsClientModalOpen] = useState(false);
  const [editingClient, setEditingClient] = useState<Client | null>(null);
  const [clientForm, setClientForm] = useState<Partial<Client>>({});
  const [clientSearch, setClientSearch] = useState('');

  // Product Form State
  const [isProductModalOpen, setIsProductModalOpen] = useState(false);
  const [editingProduct, setEditingProduct] = useState<Product | null>(null);
  const [productForm, setProductForm] = useState<Partial<Product>>({
    multiplicador: 1,
    precoUnitario: 0,
    barcodes: { unidade: '' }
  });
  const [productSearch, setProductSearch] = useState('');

  // Operator Form State
  const [isOperatorModalOpen, setIsOperatorModalOpen] = useState(false);
  const [editingOperator, setEditingOperator] = useState<Operator | null>(null);
  const [operatorForm, setOperatorForm] = useState<Partial<Operator>>({});
  const [operatorSearch, setOperatorSearch] = useState('');

  // Invoice Modal
  const [viewingInvoice, setViewingInvoice] = useState<Order | null>(null);

  // Separator Modal
  const [orderToSetSeparator, setOrderToSetSeparator] = useState<Order | null>(null);
  const [separatorName, setSeparatorName] = useState('');

  // Separation Modal
  const [separatingOrder, setSeparatingOrder] = useState<Order | null>(null);
  const [isSeparationModalOpen, setIsSeparationModalOpen] = useState(false);

  const [cnpjError, setCnpjError] = useState('');
  const [showSuccessOrder, setShowSuccessOrder] = useState(false);

  // Dashboard Drill-down State
  const [drillDownType, setDrillDownType] = useState<'client' | 'vendedor' | 'product' | null>(null);
  const [drillDownValue, setDrillDownValue] = useState<string | null>(null);

  const todayOrders = useMemo(() => {
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    return orders.filter(order => {
      const orderDate = new Date(order.createdAt);
      orderDate.setHours(0, 0, 0, 0);
      return orderDate.getTime() === today.getTime();
    });
  }, [orders]);

  const stats = useMemo(() => {
    const topClients = d3.rollups(
      todayOrders,
      v => d3.sum(v, (d: Order) => d.total || 0),
      (d: Order) => d.clientName
    )
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5)
    .map(([name, value]) => ({ name, value }));

    const topSellers = d3.rollups(
      todayOrders,
      v => d3.sum(v, (d: Order) => d.total || 0),
      (d: Order) => d.nomeQuemFezPedido || 'Não Informado'
    )
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5)
    .map(([name, value]) => ({ name, value }));

    const topProducts = d3.rollups(
      todayOrders.flatMap(o => o.items),
      v => d3.sum(v, (d: OrderItem) => d.quantidade),
      (d: OrderItem) => d.descricao
    )
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5)
    .map(([name, value]) => ({ name, value }));

    return { topClients, topSellers, topProducts };
  }, [todayOrders]);

  const drillDownData = useMemo(() => {
    if (!drillDownType || !drillDownValue) return [];

    if (drillDownType === 'client') {
      return todayOrders.filter(o => o.clientName === drillDownValue);
    }
    if (drillDownType === 'vendedor') {
      return todayOrders.filter(o => (o.nomeQuemFezPedido || 'Não Informado') === drillDownValue);
    }
    if (drillDownType === 'product') {
      return todayOrders.filter(o => o.items.some(item => item.descricao === drillDownValue));
    }
    return [];
  }, [drillDownType, drillDownValue, todayOrders]);

  // Initialize DB and load data
  useEffect(() => {
    const unsubscribeAuth = onAuthStateChanged(auth, (u) => {
      setUser(u);
      setAuthReady(true);
    });
    return () => unsubscribeAuth();
  }, []);

  useEffect(() => {
    if (!user) return;

    // Test connection
    storage.testConnection();

    const unsubProducts = storage.subscribeProducts(async (storedProducts) => {
      console.log('Products subscription fired with:', storedProducts.length, 'products');
      if (storedProducts.length === 0) {
        console.log('Database empty, populating with initial products...');
        await storage.saveProducts(INITIAL_PRODUCTS);
      } else {
        setProducts(storedProducts);
      }
    });

    const unsubClients = storage.subscribeClients(async (storedClients) => {
      if (storedClients.length === 0) {
        console.log('Database empty, populating with initial clients...');
        const initialClients = INITIAL_CLIENTS.map(c => ({
          id: crypto.randomUUID(),
          razaoSocial: c.name,
          nomeFantasia: c.name,
          cnpj: c.cnpj,
          inscricaoEstadual: '',
          telefone: '',
          email: '',
          contato: '',
          cep: '',
          endereco: '',
          numero: '',
          bairro: '',
          cidade: '',
          estado: '',
          complemento: '',
          observacoes: '',
          name: c.name
        }));
        await storage.saveClients(initialClients);
      } else {
        setClients(storedClients);
      }
    });

    const unsubOrders = storage.subscribeOrders((storedOrders) => {
      setOrders(storedOrders);
    });

    const unsubOperators = storage.subscribeOperators((storedOperators) => {
      console.log('Operators subscription fired with:', storedOperators.length, 'operators');
      setOperators(storedOperators);
    });

    return () => {
      unsubClients();
      unsubProducts();
      unsubOrders();
      unsubOperators();
    };
  }, [user]);

  useEffect(() => {
    if (orders.length > 0) {
      handleClearArchivedOrders();
    }
  }, [orders]);

  // CNPJ Search
  const handleCnpjSearch = () => {
    const client = clients.find(c => c.cnpj === cnpjInput);
    if (client) {
      setSelectedClient(client);
      setCnpjError('');
    } else {
      setSelectedClient(null);
      setCnpjError('Cliente não cadastrado');
    }
  };

  // Barcode Search
  const handleBarcodeSearch = (code: string) => {
    const product = products.find(p => 
      (p.codigoInterno || p.codigo || p.internalCode) === code || 
      (p.barcode === code) ||
      Object.values(p.barcodes || {}).includes(code)
    );
    if (product) {
      setCurrentProduct(product);
      setScannedNotFound(null);
      setItemPrice(product.precoUnitario);
      setItemMultiplier(product.multiplicador || 1);
      
      // Automatically set the sale type based on the barcode scanned
      const type = Object.keys(product.barcodes || {}).find(key => product.barcodes[key as BarcodeType] === code) as BarcodeType;
      if (type) {
        setItemUnit(type);
      } else {
        setItemUnit('unidade');
      }
    } else {
      setCurrentProduct(null);
      if (code.length > 0) { // Show link option for any code not found
        setScannedNotFound(code);
      } else {
        setScannedNotFound(null);
      }
    }
  };

  // Camera Scanner Setup
  useEffect(() => {
    let scanner: Html5QrcodeScanner | null = null;
    if (isScanning) {
      scanner = new Html5QrcodeScanner(
        "reader",
        { 
          fps: 10, 
          qrbox: { width: 250, height: 150 },
          videoConstraints: {
            facingMode: "environment"
          }
        },
        /* verbose= */ false
      );
      scanner.render((decodedText) => {
        setBarcodeInput(decodedText);
        handleBarcodeSearch(decodedText);
        setIsScanning(false);
        if (scanner) scanner.clear();
      }, (error) => {
        // console.warn(error);
      });
    }
    return () => {
      if (scanner) scanner.clear();
    };
  }, [isScanning]);

  const extractMultiplier = (embalagem: string): number => {
    if (!embalagem) return 1;
    const upper = embalagem.toUpperCase();
    
    // Tenta encontrar padrões como "1 X 20", "CXA 20", "FD 12", "1/24"
    const parts = upper.split('X');
    if (parts.length >= 2) {
      const secondPart = parts[1].trim().split(' ')[0];
      const num = parseInt(secondPart);
      if (!isNaN(num)) return num;
    }
    
    // Tenta encontrar um número após CXA, FD, CX, etc.
    const match = upper.match(/(?:CXA|FD|CX|BOX|CAIXA|FARDO|FDO)\s*(\d+)/i);
    if (match && match[1]) {
      return parseInt(match[1]);
    }

    // Tenta encontrar padrão 1/24
    const slashMatch = upper.match(/1\/(\d+)/);
    if (slashMatch && slashMatch[1]) {
      return parseInt(slashMatch[1]);
    }

    // Tenta encontrar apenas um número isolado que pareça um multiplicador
    const singleNumMatch = upper.match(/\b(\d+)\b/);
    if (singleNumMatch && singleNumMatch[1]) {
      const n = parseInt(singleNumMatch[1]);
      if (n > 1 && n < 1000) return n;
    }

    return 1;
  };

  const addItem = () => {
    if (!currentProduct) return;
    
    const multiplicador = itemUnit === 'unidade' ? 1 : itemMultiplier;
    const valorTotal = itemQty * multiplicador * itemPrice;
    
    const newItem: OrderItem = {
      id: crypto.randomUUID(),
      codigoInterno: currentProduct.codigoInterno,
      descricao: currentProduct.descricao,
      embalagem: currentProduct.embalagem,
      barcode: currentProduct.barcodes?.[itemUnit] || 
               currentProduct.barcodes?.unidade || 
               (currentProduct.barcode !== currentProduct.codigoInterno ? currentProduct.barcode : '') || 
               '',
      quantidade: itemQty,
      tipoVenda: itemUnit,
      precoUnitario: itemPrice,
      multiplicador: multiplicador,
      valorTotal: valorTotal,
      completo: currentProduct.completo,
    };
    
    setOrderItems([...orderItems, newItem]);
    setCurrentProduct(null);
    setBarcodeInput('');
    setItemQty(1);
    setItemUnit('unidade');
    setItemPrice(0);
    setItemMultiplier(1);
  };

  const removeItem = (id: string) => {
    setOrderItems(orderItems.filter(item => item.id !== id));
  };

  const finalizeOrder = async () => {
    if (!selectedClient || orderItems.length === 0 || !nomeQuemFezPedido.trim()) return;
    
    const totalOrder = orderItems.reduce((acc, item) => acc + item.valorTotal, 0);
    
    const newOrder: Order = {
      id: `PED-${Math.floor(1000 + Math.random() * 9000)}`,
      numero: Math.floor(1000 + Math.random() * 9000).toString(),
      data: new Date().toISOString(),
      clientCnpj: selectedClient.cnpj,
      clientName: selectedClient.razaoSocial || selectedClient.nomeFantasia || '',
      items: orderItems,
      status: 'Separação',
      createdAt: new Date().toISOString(),
      nomeQuemFezPedido: nomeQuemFezPedido.trim(),
      total: totalOrder,
    };
    
    // Optimistic: update local state immediately
    setOrders(prev => [newOrder, ...prev]);
    
    // Save in background
    storage.saveOrder(newOrder).catch(err => console.error('Error saving order:', err));
    
    // Reset form immediately
    setSelectedClient(null);
    setCnpjInput('');
    setOrderItems([]);
    setNomeQuemFezPedido('');
    
    // Show success and invoice
    setShowSuccessOrder(true);
    setViewingInvoice(newOrder);
    setActiveTab('kanban');
    
    // Auto-hide success after 5 seconds
    setTimeout(() => setShowSuccessOrder(false), 5000);
  };

  const advanceStatus = async (orderId: string) => {
    console.log('Advancing status for order:', orderId);
    const order = orders.find(o => o.id === orderId);
    if (!order) {
      console.error('Order not found:', orderId);
      return;
    }

    const currentIndex = ORDER_STATUSES.indexOf(order.status);
    console.log('Current status:', order.status, 'Index:', currentIndex);
    
    if (currentIndex < ORDER_STATUSES.length - 1) {
      const nextStatus = ORDER_STATUSES[currentIndex + 1] as OrderStatus;
      console.log('Next status:', nextStatus);
      
      // If advancing from 'Pedido Separado' to 'Pedido Faturado', prompt for separator
      if (order.status === 'Pedido Separado') {
        setOrderToSetSeparator(order);
        setSeparatorName('');
        return;
      }

      const updatedOrder = { ...order, status: nextStatus };
      // Optimistic update
      setOrders(prev => prev.map(o => o.id === orderId ? updatedOrder : o));
      storage.saveOrder(updatedOrder).catch(err => console.error('Error advancing status:', err));
    } else {
      console.warn('Order already at final status');
    }
  };

  const handleArchiveOrder = async (orderId: string) => {
    const order = orders.find(o => o.id === orderId);
    if (!order) return;

    const updatedOrder: Order = { 
      ...order, 
      archivedAt: new Date().toISOString() 
    };

    // Optimistic update
    setOrders(prev => prev.map(o => o.id === orderId ? updatedOrder : o));
    storage.saveOrder(updatedOrder).catch(err => console.error('Error archiving order:', err));
  };

  const handleClearArchivedOrders = async () => {
    const now = new Date();
    const sevenDaysAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
    
    const ordersToDelete = orders.filter(o => {
      if (!o.archivedAt) return false;
      return new Date(o.archivedAt) < sevenDaysAgo;
    });

    if (ordersToDelete.length === 0) return;

    // Optimistic update
    setOrders(prev => prev.filter(o => !ordersToDelete.find(td => td.id === o.id)));
    
    for (const order of ordersToDelete) {
      storage.deleteOrder(order.id).catch(err => console.error('Error deleting archived order:', err));
    }
  };

  const handleSetSeparator = async () => {
    if (!orderToSetSeparator || !separatorName.trim()) return;
    
    const currentIndex = ORDER_STATUSES.indexOf(orderToSetSeparator.status);
    const nextStatus = ORDER_STATUSES[currentIndex + 1] as OrderStatus;
    
    const updatedOrder = { 
      ...orderToSetSeparator, 
      status: nextStatus,
      separador: separatorName.trim()
    };
    
    // Optimistic update
    setOrders(prev => prev.map(o => o.id === updatedOrder.id ? updatedOrder : o));
    storage.saveOrder(updatedOrder).catch(err => console.error('Error setting separator:', err));
    
    setOrderToSetSeparator(null);
    setSeparatorName('');
  };

  // Client Management
  const handleSaveClient = async (e: React.FormEvent) => {
    e.preventDefault();
    const newClient: Client = {
      id: editingClient?.id || crypto.randomUUID(),
      razaoSocial: clientForm.razaoSocial || '',
      nomeFantasia: clientForm.nomeFantasia || '',
      cnpj: clientForm.cnpj || '',
      inscricaoEstadual: clientForm.inscricaoEstadual || '',
      telefone: clientForm.telefone || '',
      email: clientForm.email || '',
      contato: clientForm.contato || '',
      cep: clientForm.cep || '',
      endereco: clientForm.endereco || '',
      numero: clientForm.numero || '',
      bairro: clientForm.bairro || '',
      cidade: clientForm.cidade || '',
      estado: clientForm.estado || '',
      complemento: clientForm.complemento || '',
      observacoes: clientForm.observacoes || '',
    };

    // Optimistic update
    if (editingClient) {
      setClients(prev => prev.map(c => c.id === newClient.id ? newClient : c));
    } else {
      setClients(prev => [newClient, ...prev]);
    }
    
    storage.saveClient(newClient).catch(err => console.error('Error saving client:', err));
    
    setIsClientModalOpen(false);
    setEditingClient(null);
    setClientForm({});
  };

  const handleDeleteClient = async (id: string) => {
    if (confirm('Deseja realmente excluir este cliente?')) {
      // Optimistic update
      setClients(prev => prev.filter(c => c.id !== id));
      storage.deleteClient(id).catch(err => console.error('Error deleting client:', err));
    }
  };

  useEffect(() => {
    if (isProductModalOpen && productForm.embalagem) {
      const extracted = extractMultiplier(productForm.embalagem);
      if (extracted > 1 && (productForm.multiplicador === 1 || !productForm.multiplicador)) {
        setProductForm(prev => ({ ...prev, multiplicador: extracted }));
      }
    }
  }, [productForm.embalagem, isProductModalOpen]);

  const handleSaveProduct = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!productForm.codigoInterno || !productForm.descricao) {
      alert('Código Interno e Descrição são obrigatórios.');
      return;
    }

    const productData: Product = {
      id: editingProduct?.id || crypto.randomUUID(),
      codigoInterno: productForm.codigoInterno,
      descricao: productForm.descricao,
      embalagem: productForm.embalagem || '',
      barcodes: productForm.barcodes || { unidade: '' },
      precoUnitario: Number(productForm.precoUnitario) || 0,
      multiplicador: Number(productForm.multiplicador) || 1,
      completo: productForm.completo || `${productForm.descricao} ${productForm.embalagem || ''}`.trim(),
      stock: Number(productForm.stock) || 0,
      dailySales: Number(productForm.dailySales) || 0,
      barcode: productForm.barcodes?.unidade || productForm.barcodes?.caixa || '',
      codigo: productForm.codigoInterno,
      internalCode: productForm.codigoInterno,
      description: productForm.descricao
    };

    // Optimistic update
    if (editingProduct) {
      setProducts(prev => prev.map(p => p.id === productData.id ? productData : p));
    } else {
      const existing = products.find(p => p.codigoInterno === productData.codigoInterno);
      if (existing) {
        if (confirm(`Já existe um produto com o código ${productData.codigoInterno}. Deseja atualizar o cadastro existente?`)) {
          setProducts(prev => prev.map(p => p.id === existing.id ? { ...productData, id: existing.id } : p));
          storage.saveProduct({ ...productData, id: existing.id }).catch(err => console.error('Error updating product:', err));
        } else {
          return;
        }
      } else {
        setProducts(prev => [productData, ...prev]);
        storage.saveProduct(productData).catch(err => console.error('Error saving product:', err));
      }
    }

    if (editingProduct) {
      storage.saveProduct(productData).catch(err => console.error('Error updating product:', err));
    }

    setIsProductModalOpen(false);
    setEditingProduct(null);
    setProductForm({
      multiplicador: 1,
      precoUnitario: 0,
      barcodes: { unidade: '' }
    });
  };

  const handleDeleteProduct = async (id: string) => {
    if (confirm('Deseja realmente excluir este produto?')) {
      // Optimistic update
      setProducts(prev => prev.filter(p => p.id !== id));
      storage.deleteProduct(id).catch(err => console.error('Error deleting product:', err));
    }
  };

  const [isPasswordModalOpen, setIsPasswordModalOpen] = useState(false);
  const [passwordInput, setPasswordInput] = useState('');
  const [passwordCallback, setPasswordCallback] = useState<(() => void) | null>(null);
  const ADMIN_PASSWORD = 'atacadao@172cascavel';

  const checkPassword = (callback: () => void) => {
    setPasswordCallback(() => callback);
    setIsPasswordModalOpen(true);
    setPasswordInput('');
  };

  const handlePasswordSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (passwordInput === ADMIN_PASSWORD) {
      setIsPasswordModalOpen(false);
      if (passwordCallback) {
        passwordCallback();
        setPasswordCallback(null);
      }
    } else {
      alert('Senha incorreta!');
    }
  };

  // Import / Export
  const handleRestoreDefaultProducts = async () => {
    if (confirm('Isso restaurará os produtos iniciais do sistema. Deseja continuar?')) {
      await storage.saveProducts(INITIAL_PRODUCTS);
      setProducts(INITIAL_PRODUCTS);
      alert('Produtos restaurados com sucesso!');
    }
  };

  const handleRestoreDefaultClients = async () => {
    if (confirm('Isso restaurará os clientes iniciais do sistema. Deseja continuar?')) {
      const initialClients = INITIAL_CLIENTS.map(c => ({
        id: crypto.randomUUID(),
        razaoSocial: c.name,
        nomeFantasia: c.name,
        cnpj: c.cnpj,
        inscricaoEstadual: '',
        telefone: '',
        email: '',
        contato: '',
        cep: '',
        endereco: '',
        numero: '',
        bairro: '',
        cidade: '',
        estado: '',
        complemento: '',
        observacoes: ''
      }));
      await storage.saveClients(initialClients);
      setClients(initialClients);
      alert('Clientes restaurados com sucesso!');
    }
  };

  const handleImportProducts = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;

    const reader = new FileReader();
    reader.onload = async (evt) => {
      const bstr = evt.target?.result;
      const wb = XLSX.read(bstr, { type: 'binary' });
      const wsname = wb.SheetNames[0];
      const ws = wb.Sheets[wsname];
      const data = XLSX.utils.sheet_to_json(ws) as any[];

      if (data.length === 0) {
        alert('A planilha parece estar vazia.');
        return;
      }

      // Diagnostic: show columns found in the first row
      const firstRowKeys = Object.keys(data[0]);
      
      // Use a Map to prevent duplicates during import
      const productMap = new Map<string, Product>();
      products.forEach(p => {
        const key = (p.codigoInterno || p.codigo || p.internalCode || '').trim().toUpperCase();
        if (key) productMap.set(key, p);
      });

      let processedCount = 0;

      data.forEach(row => {
        const normalize = (s: string) => s.normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase().trim();
        
        const getVal = (keys: string[]) => {
          const normalizedKeys = keys.map(normalize);
          const foundKey = Object.keys(row).find(k => 
            normalizedKeys.includes(normalize(k))
          );
          return foundKey ? String(row[foundKey]).trim() : '';
        };

        const codigoInterno = getVal(['Código Interno', 'codigoInterno', 'codigo', 'cod', 'internalCode', 'id', 'sku', 'referencia', 'ref']).toUpperCase();
        const descricao = getVal(['Descrição', 'descricao', 'description', 'nome', 'produto', 'item', 'nome do produto']);
        const embalagem = getVal(['Embalagem', 'embalagem', 'completo', 'complemento', 'unidade', 'un', 'emb']);
        const precoUnitario = parseFloat(getVal(['Preço Unitário', 'precoUnitario', 'preco', 'price', 'valor', 'vlr unit', 'preço']).replace(',', '.') || '0');
        let multiplicador = parseInt(getVal(['Multiplicador', 'multiplicador', 'mult', 'fator']) || '0');
        
        if (multiplicador <= 1) {
          multiplicador = extractMultiplier(embalagem);
        }
        
        const barcodeUnidade = getVal(['Barcode Unidade', 'barcode_unidade', 'ean_unidade', 'ean', 'codigo de barras']);
        const barcodeCaixa = getVal(['Barcode Caixa', 'barcode_caixa', 'ean_caixa', 'ean_cx']);
        const barcodeFardo = getVal(['Barcode Fardo', 'barcode_fardo', 'ean_fardo']);
        const barcodeKg = getVal(['Barcode KG', 'barcode_kg', 'ean_kg']);

        if (!codigoInterno || !descricao) return;

        const existingProduct = productMap.get(codigoInterno);
        const productData: Product = {
          id: existingProduct?.id || crypto.randomUUID(),
          codigoInterno,
          descricao,
          embalagem,
          barcodes: {
            unidade: barcodeUnidade || existingProduct?.barcodes?.unidade || '',
            caixa: barcodeCaixa || existingProduct?.barcodes?.caixa || '',
            fardo: barcodeFardo || existingProduct?.barcodes?.fardo || '',
            kg: barcodeKg || existingProduct?.barcodes?.kg || '',
          },
          precoUnitario,
          multiplicador,
          completo: embalagem,
          codigo: codigoInterno,
          internalCode: codigoInterno,
          description: descricao,
          barcode: barcodeUnidade || barcodeCaixa || existingProduct?.barcodes?.unidade || existingProduct?.barcodes?.caixa || '',
          stock: 0,
          dailySales: 0
        };

        productMap.set(codigoInterno, productData);
        processedCount++;
      });

      if (processedCount === 0) {
        alert(`Não foi possível processar os produtos.\n\nVerifique se as colunas de Código e Descrição estão presentes.\n\nColunas encontradas no arquivo:\n${firstRowKeys.join(' | ')}`);
      } else {
        const updatedProducts = Array.from(productMap.values());
        setProducts(updatedProducts);
        await storage.saveProducts(updatedProducts);
        alert(`${processedCount} produtos processados com sucesso!`);
      }
    };
    reader.readAsBinaryString(file);
    e.target.value = ''; // Reset input
  };

  const handleClearProducts = async () => {
    if (confirm('ATENÇÃO: Isso excluirá TODOS os produtos permanentemente. Deseja continuar?')) {
      await storage.clearProducts();
      setProducts([]);
      alert('Todos os produtos foram excluídos.');
    }
  };

  const handleImportBarcodes = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;

    const reader = new FileReader();
    reader.onload = async (evt) => {
      const bstr = evt.target?.result;
      const wb = XLSX.read(bstr, { type: 'binary' });
      const wsname = wb.SheetNames[0];
      const ws = wb.Sheets[wsname];
      const data = XLSX.utils.sheet_to_json(ws) as any[];

      const updatedProducts = [...products];
      let linkedCount = 0;
      let createdCount = 0;

      data.forEach(row => {
        const barcode = String(row['Código de Barras'] || row['barcode'] || row['EAN'] || '').trim();
        const internalCode = String(row['Código Interno'] || row['codigo'] || row['internalCode'] || '').trim();

        if (!barcode || !internalCode) return;

        const index = updatedProducts.findIndex(p => p.codigoInterno === internalCode || p.codigo === internalCode || p.internalCode === internalCode);
        if (index !== -1) {
          const existing = updatedProducts[index];
          updatedProducts[index] = { 
            ...existing, 
            barcode: barcode,
            barcodes: {
              ...(existing.barcodes || {}),
              unidade: barcode
            }
          };
          linkedCount++;
        } else {
          updatedProducts.push({
            id: crypto.randomUUID(),
            codigoInterno: internalCode,
            descricao: 'PRODUTO SEM DESCRIÇÃO (VINCULADO VIA EAN)',
            embalagem: 'N/I',
            barcodes: {
              unidade: barcode
            },
            precoUnitario: 0,
            multiplicador: 1,
            barcode: barcode,
            codigo: internalCode,
            internalCode: internalCode,
            description: 'PRODUTO SEM DESCRIÇÃO (VINCULADO VIA EAN)',
            stock: 0,
            dailySales: 0
          });
          createdCount++;
        }
      });

      setProducts(updatedProducts);
      await storage.saveProducts(updatedProducts);
      alert(`${linkedCount} códigos vinculados e ${createdCount} novos registros criados!`);
    };
    reader.readAsBinaryString(file);
  };

  const handleExportProducts = () => {
    const exportData = products.map(p => ({
      'Código': p.codigo || p.internalCode,
      'Descrição': p.descricao || p.description,
      'Embalagem': p.completo || p.embalagem || ''
    }));
    const ws = XLSX.utils.json_to_sheet(exportData);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, "Produtos");
    XLSX.writeFile(wb, "produtos_atacadao.xlsx");
  };

  const handleImportClients = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;

    const reader = new FileReader();
    reader.onload = async (evt) => {
      const bstr = evt.target?.result;
      const wb = XLSX.read(bstr, { type: 'binary' });
      const wsname = wb.SheetNames[0];
      const ws = wb.Sheets[wsname];
      const data = XLSX.utils.sheet_to_json(ws) as any[];

      // Use a Map to prevent duplicates by CNPJ
      const clientMap = new Map<string, Client>();
      clients.forEach(c => {
        const key = c.cnpj.replace(/\D/g, '').trim();
        if (key) clientMap.set(key, c);
      });

      let processedCount = 0;

      data.forEach(row => {
        const razaoSocial = String(row['Razão Social'] || row['razao_social'] || row['Nome'] || '').trim();
        const cnpjRaw = String(row['CNPJ'] || row['cnpj'] || '').trim();
        const cnpj = cnpjRaw.replace(/\D/g, '').trim();

        if (!razaoSocial || !cnpj) return;

        const clientData: Partial<Client> = {
          razaoSocial,
          nomeFantasia: String(row['Nome Fantasia'] || row['nome_fantasia'] || razaoSocial).trim(),
          cnpj: cnpjRaw,
          inscricaoEstadual: String(row['IE'] || row['inscricao_estadual'] || '').trim(),
          telefone: String(row['Telefone'] || row['tel'] || '').trim(),
          email: String(row['Email'] || row['e-mail'] || '').trim(),
          contato: String(row['Contato'] || '').trim(),
          cep: String(row['CEP'] || '').trim(),
          endereco: String(row['Endereço'] || row['logradouro'] || '').trim(),
          numero: String(row['Número'] || row['n'] || '').trim(),
          bairro: String(row['Bairro'] || '').trim(),
          cidade: String(row['Cidade'] || '').trim(),
          estado: String(row['Estado'] || row['UF'] || '').trim(),
          complemento: String(row['Complemento'] || '').trim(),
          observacoes: String(row['Observações'] || '').trim()
        };

        if (clientMap.has(cnpj)) {
          const existing = clientMap.get(cnpj)!;
          clientMap.set(cnpj, { ...existing, ...clientData });
        } else {
          clientMap.set(cnpj, {
            id: crypto.randomUUID(),
            ...clientData as Client
          });
        }
        processedCount++;
      });

      const updatedClients = Array.from(clientMap.values());
      setClients(updatedClients);
      await storage.saveClients(updatedClients);
      alert(`${processedCount} clientes processados com sucesso!`);
    };
    reader.readAsBinaryString(file);
    e.target.value = ''; // Reset input
  };

  const handleClearClients = async () => {
    if (confirm('ATENÇÃO: Isso excluirá TODOS os clientes permanentemente. Deseja continuar?')) {
      await storage.clearClients();
      setClients([]);
      alert('Todos os clientes foram excluídos.');
    }
  };

  const handleClearOrders = async () => {
    if (confirm('ATENÇÃO: Isso excluirá TODOS os pedidos permanentemente. Deseja continuar?')) {
      await storage.clearOrders();
      setOrders([]);
      alert('Todos os pedidos foram excluídos.');
    }
  };

  const [isSavingOperator, setIsSavingOperator] = useState(false);

  const handleSaveOperator = async () => {
    console.log('handleSaveOperator called with form:', operatorForm);
    if (!operatorForm.nome || !operatorForm.matricula) {
      alert('Por favor, preencha o nome e a matrícula do operador.');
      return;
    }

    // Check for duplicate matricula if creating new
    if (!editingOperator) {
      const isDuplicate = operators.some(op => op.matricula === operatorForm.matricula);
      if (isDuplicate) {
        alert('Já existe um operador cadastrado com esta matrícula.');
        return;
      }
    }

    setIsSavingOperator(true);

    const operatorData: Operator = {
      id: editingOperator?.id || `op-${Date.now()}`,
      nome: operatorForm.nome,
      matricula: operatorForm.matricula,
      createdAt: editingOperator?.createdAt || new Date().toISOString()
    };

    console.log('Saving operator data:', operatorData);

    try {
      console.log('Attempting to save operator to storage...');
      await storage.saveOperator(operatorData);
      console.log('Operator saved successfully to storage');
      setIsOperatorModalOpen(false);
      setEditingOperator(null);
      setOperatorForm({});
      alert(editingOperator ? 'Operador atualizado com sucesso!' : 'Operador cadastrado com sucesso!');
    } catch (error) {
      console.error('Erro ao salvar operador:', error);
      alert('Erro ao salvar operador. Tente novamente.');
    } finally {
      setIsSavingOperator(false);
    }
  };

  const handleDeleteOperator = async (id: string) => {
    if (confirm('Deseja realmente excluir este operador?')) {
      try {
        await storage.deleteOperator(id);
        alert('Operador excluído com sucesso!');
      } catch (error) {
        console.error('Erro ao excluir operador:', error);
        alert('Erro ao excluir operador.');
      }
    }
  };

  const handleExportClients = () => {
    const ws = XLSX.utils.json_to_sheet(clients);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, "Clientes");
    XLSX.writeFile(wb, "clientes_atacadao.xlsx");
  };

  const generatePDF = (order: Order) => {
    console.log('Generating PDF for order:', order.id);
    try {
      const doc = new jsPDF({ orientation: 'landscape' });
    
      // ==========================================
      // PAGE 1: NOTA PARA FATURAMENTO
      // ==========================================
      doc.setFontSize(18);
      doc.setTextColor(243, 112, 33); // Brand Orange
      doc.setFont("helvetica", "bold");
      doc.text("ATACADÃO - NOTA PARA FATURAMENTO", 148.5, 15, { align: "center" });
      
      // Order Barcode for Page 1
      const orderCanvas = document.createElement('canvas');
      try {
        JsBarcode(orderCanvas, order.id, {
          format: "CODE128",
          width: 2,
          height: 40,
          displayValue: true,
          fontSize: 14
        });
        const orderBarcodeImg = orderCanvas.toDataURL('image/png');
        doc.addImage(orderBarcodeImg, 'PNG', 220, 8, 50, 18);
      } catch (e) {
        console.error('Order barcode generation error:', e);
      }

      doc.setFontSize(10);
      doc.setTextColor(0, 0, 0);
      doc.setFont("helvetica", "bold");
      doc.text(`RAZÃO SOCIAL: ${order.clientName.toUpperCase()}`, 20, 25);
      doc.text(`CNPJ: ${order.clientCnpj}`, 200, 25);
      
      doc.setFont("helvetica", "normal");
      doc.text(`PEDIDO: ${order.id} | DATA: ${new Date(order.createdAt).toLocaleString()}`, 20, 32);
      doc.text(`OPERADOR: ${order.nomeQuemFezPedido || 'N/I'}`, 200, 32);

      // Table Data
      const tableData = order.items.map(item => {
        const currentProd = products.find(p => p.codigoInterno === item.codigoInterno || p.id === item.id);
        const ean = currentProd?.barcodes?.[item.tipoVenda] || 
                    currentProd?.barcodes?.unidade || 
                    (currentProd?.barcode !== item.codigoInterno ? currentProd?.barcode : '') || 
                    (item.barcode !== item.codigoInterno ? item.barcode : '');

        return [
          ean, // Column 0: Barcode Value (will be replaced by image)
          item.codigoInterno,
          item.descricao.toUpperCase(),
          item.embalagem || '',
          item.quantidade,
          `R$ ${item.precoUnitario.toFixed(2)}`,
          `R$ ${(item.precoUnitario * item.multiplicador).toFixed(2)}`,
          `R$ ${item.valorTotal.toFixed(2)}`
        ];
      });

      autoTable(doc, {
        startY: 40,
        head: [['CÓDIGO DE BARRAS', 'CÓD. INTERNO', 'DESCRIÇÃO DA MERCADORIA', 'EMBALAGEM', 'QUANTIDADE', 'PREÇO UND', 'PREÇO CAIXA', 'PREÇO TOTAL']],
        body: tableData,
        headStyles: { fillColor: [243, 112, 33], textColor: [255, 255, 255], fontStyle: 'bold' },
        styles: { fontSize: 8, cellPadding: 2, minCellHeight: 15 },
        columnStyles: {
          0: { cellWidth: 45 }, // Barcode
          1: { cellWidth: 25 }, // Cód
          2: { cellWidth: 'auto' }, // Desc
          3: { cellWidth: 40 }, // Emb
          4: { cellWidth: 20 }, // Qtd
          5: { cellWidth: 25 }, // Preço Und
          6: { cellWidth: 25 }, // Preço Caixa
          7: { cellWidth: 25 }, // Total
        },
        didDrawCell: (data) => {
          if (data.section === 'body' && data.column.index === 0) {
            const barcodeValue = data.cell.raw as string;
            if (barcodeValue && barcodeValue.length > 0) {
              const canvas = document.createElement('canvas');
              try {
                JsBarcode(canvas, barcodeValue, {
                  format: "CODE128",
                  width: 2,
                  height: 40,
                  displayValue: true,
                  fontSize: 14,
                  margin: 2
                });
                const imgData = canvas.toDataURL('image/png');
                doc.addImage(imgData, 'PNG', data.cell.x + 2, data.cell.y + 2, data.cell.width - 4, data.cell.height - 4);
              } catch (e) {
                console.error('Error drawing barcode in table:', e);
              }
            }
            // Clear the text so it doesn't overlap
            data.cell.text = [];
          }
        },
        alternateRowStyles: { fillColor: [250, 250, 250] }
      });

      const finalY = (doc as any).lastAutoTable.finalY + 15;
      doc.setFontSize(12);
      doc.setFont("helvetica", "bold");
      const totalValue = order.items.reduce((acc, item) => acc + item.valorTotal, 0);
      doc.text(`PREÇO TOTAL DO PEDIDO: R$ ${totalValue.toFixed(2)}`, 20, finalY);
      
      // Add a simple border/box for the footer
      doc.setDrawColor(243, 112, 33);
      doc.setLineWidth(0.5);
      doc.line(20, finalY + 2, 277, finalY + 2);

      // ==========================================
      // PAGE 2: VIA DO SEPARADOR (Landscape)
      // ==========================================
      doc.addPage();
      doc.setFontSize(18);
      doc.setTextColor(0, 166, 81); // Brand Green
      doc.text("ATACADÃO - VIA DO SEPARADOR", 148.5, 15, { align: "center" });
      
      doc.setFontSize(10);
      doc.setTextColor(0, 0, 0);
      doc.text(`CLIENTE: ${order.clientName.toUpperCase()}`, 20, 25);
      doc.text(`PEDIDO: ${order.id}`, 20, 30);
      
      const tableDataSeparator = order.items.map(item => {
        return [
          item.codigoInterno,
          item.descricao.toUpperCase(),
          item.embalagem || '',
          item.quantidade,
          item.tipoVenda === 'caixa' ? 'CAIXA' : 'UNIDADE'
        ];
      });
      
      autoTable(doc, {
        startY: 35,
        head: [['CÓD. INTERNO', 'DESCRIÇÃO', 'EMBALAGEM', 'QTD', 'TIPO']],
        body: tableDataSeparator,
        headStyles: { fillColor: [0, 166, 81] },
        styles: { fontSize: 10, cellPadding: 4 },
        columnStyles: {
          0: { cellWidth: 30 },
          1: { cellWidth: 'auto' },
          2: { cellWidth: 40 },
          3: { cellWidth: 20 },
          4: { cellWidth: 30 },
        }
      });

      // ==========================================
      // PAGE 3: CÓDIGOS PARA CHECKOUT (Landscape 2-column)
      // ==========================================
      doc.addPage();
      doc.setFontSize(18);
      doc.setTextColor(243, 112, 33);
      doc.text("ATACADÃO - CÓDIGOS PARA CHECKOUT", 148.5, 15, { align: "center" });
      
      let col1Y = 30;
      let col2Y = 30;
      const midPoint = 148.5;

      order.items.forEach((item, index) => {
        const isCol2 = index % 2 !== 0;
        let currentY = isCol2 ? col2Y : col1Y;
        let currentX = isCol2 ? midPoint + 10 : 20;

        if (currentY > 180) {
          if (!isCol2 || (isCol2 && col1Y > 180)) {
            doc.addPage();
            col1Y = 20;
            col2Y = 20;
            currentY = 20;
          }
        }

        const canvas = document.createElement('canvas');
        const currentProd = products.find(p => p.codigoInterno === item.codigoInterno || p.id === item.id);
        const barcodeToUse = currentProd?.barcodes?.[item.tipoVenda] || 
                             currentProd?.barcodes?.unidade || 
                             (currentProd?.barcode !== item.codigoInterno ? currentProd?.barcode : null) || 
                             (item.barcode !== item.codigoInterno ? item.barcode : null);
        
        if (barcodeToUse) {
          try {
            JsBarcode(canvas, barcodeToUse, {
              format: "CODE128",
              width: 2,
              height: 35,
              displayValue: true,
              fontSize: 12,
              margin: 2
            });
            const imgData = canvas.toDataURL('image/png');
            doc.setFontSize(9);
            doc.setFont("helvetica", "bold");
            doc.text(`${item.codigoInterno} - ${item.descricao}`, currentX, currentY);
            doc.setFont("helvetica", "normal");
            doc.text(`Qtd: ${item.quantidade} | Emb: ${item.embalagem || 'N/I'}`, currentX, currentY + 4);
            doc.addImage(imgData, 'PNG', currentX, currentY + 6, 60, 18);
            if (isCol2) col2Y += 35; else col1Y += 35;
          } catch (e) {
            if (isCol2) col2Y += 10; else col1Y += 10;
          }
        } else {
          doc.setFontSize(9);
          doc.setFont("helvetica", "bold");
          doc.text(`${item.codigoInterno} - ${item.descricao}`, currentX, currentY);
          doc.setTextColor(255, 0, 0);
          doc.text(`[PRODUTO SEM EAN VINCULADO]`, currentX, currentY + 4);
          doc.setTextColor(0, 0, 0);
          if (isCol2) col2Y += 15; else col1Y += 15;
        }
      });

      doc.save(`Nota_Faturamento_${order.id}.pdf`);
      
      // Also trigger print dialog
      console.log('Triggering print dialog for order:', order.id);
      doc.autoPrint();
      const blob = doc.output('bloburl');
      const printWindow = window.open(blob, '_blank');
      if (!printWindow) {
        console.warn('Pop-up blocked, could not open print window');
        // Fallback: tell user to open the downloaded file
      }
    } catch (error) {
      console.error('Error generating PDF:', error);
    }
  };

  const generateSeparationPDF = (order: Order) => {
    console.log('Generating Separation PDF for order:', order.id);
    try {
      const doc = new jsPDF({
        orientation: "landscape",
        unit: "mm",
        format: "a4"
      });
      
      // ==========================================
      // PAGE: VIA DO SEPARADOR
      // ==========================================
      doc.setFontSize(18);
      doc.setTextColor(0, 166, 81); // Brand Green
      doc.text("ATACADÃO - VIA DO SEPARADOR", 148.5, 15, { align: "center" });
      
      doc.setFontSize(10);
      doc.setTextColor(0, 0, 0);
      doc.text(`Pedido: ${order.id}`, 20, 25);

      doc.text(`Data: ${new Date(order.createdAt).toLocaleString()}`, 20, 30);
      doc.text(`Cliente: ${order.clientName}`, 20, 35);
      
      // Items Table (Separator)
      const tableDataSeparator = order.items.map(item => {
        return [
          item.codigoInterno,
          item.descricao.toUpperCase(),
          item.embalagem || '',
          item.quantidade,
          item.tipoVenda === 'caixa' ? 'CAIXA' : 'UNIDADE'
        ];
      });
      
      autoTable(doc, {
        startY: 45,
        head: [['CÓD. INTERNO', 'DESCRIÇÃO', 'EMBALAGEM', 'QTD', 'TIPO']],
        body: tableDataSeparator,
        headStyles: { fillColor: [0, 166, 81] },
        styles: { fontSize: 10, cellPadding: 4 },
        columnStyles: {
          0: { cellWidth: 30 },
          1: { cellWidth: 'auto' },
          2: { cellWidth: 40 },
          3: { cellWidth: 20 },
          4: { cellWidth: 30 },
        }
      });

      const totalItems = order.items.reduce((acc, item) => acc + item.quantidade, 0);
      const finalYSep = (doc as any).lastAutoTable.finalY + 10;
      doc.setFontSize(12);
      doc.text(`TOTAL DE ITENS PARA SEPARAÇÃO: ${totalItems}`, 20, finalYSep);

      doc.save(`Separacao_${order.id}.pdf`);
      
      // Also trigger print dialog
      doc.autoPrint();
      window.open(doc.output('bloburl'), '_blank');
    } catch (error) {
      console.error('Error generating separation PDF:', error);
    }
  };

  const handleLinkBarcode = async () => {
    if (!linkingProduct || !newBarcode) return;

    const updatedProduct: Product = { 
      ...linkingProduct,
      codigoInterno: linkingProduct.codigoInterno || linkingProduct.codigo || linkingProduct.internalCode || '',
      descricao: linkingProduct.descricao || linkingProduct.description || 'PRODUTO SEM DESCRIÇÃO',
      embalagem: linkingProduct.embalagem || 'N/I',
      precoUnitario: linkingProduct.precoUnitario || 0,
      multiplicador: linkingProduct.multiplicador || 1,
      barcodes: {
        ...(linkingProduct.barcodes || {}),
        [linkingBarcodeType]: newBarcode
      },
      // Update legacy field for compatibility, prioritizing the new barcode
      barcode: newBarcode,
      codigo: linkingProduct.codigoInterno || linkingProduct.codigo || linkingProduct.internalCode || '',
      internalCode: linkingProduct.codigoInterno || linkingProduct.codigo || linkingProduct.internalCode || '',
      description: linkingProduct.descricao || linkingProduct.description || 'PRODUTO SEM DESCRIÇÃO'
    };

    try {
      // Optimistic update
      setProducts(prev => prev.map(p => p.id === updatedProduct.id ? updatedProduct : p));
      
      // Persist to storage
      await storage.saveProduct(updatedProduct);
      
      alert('Código de barras vinculado com sucesso!');
      setLinkingProduct(null);
      setNewBarcode('');
      setLinkSearch('');
      setLinkingBarcodeType('unidade');
    } catch (err) {
      console.error('Error linking barcode:', err);
      alert('Erro ao salvar o vínculo. Verifique sua conexão.');
    }
  };

  const filteredLinkProducts = useMemo(() => {
    if (!linkSearch) return [];
    return products.filter(p => 
      (p.descricao || p.description || '').toLowerCase().includes(linkSearch.toLowerCase()) ||
      (p.codigo || p.internalCode || '').toLowerCase().includes(linkSearch.toLowerCase())
    ).slice(0, 5);
  }, [products, linkSearch]);

  const filteredClients = useMemo(() => {
    return clients.filter(c => 
      c.razaoSocial.toLowerCase().includes(clientSearch.toLowerCase()) ||
      c.nomeFantasia.toLowerCase().includes(clientSearch.toLowerCase()) ||
      c.cnpj.includes(clientSearch)
    );
  }, [clients, clientSearch]);

  const filteredProducts = useMemo(() => {
    return products.filter(p => 
      (p.descricao || p.description || '').toLowerCase().includes(productSearch.toLowerCase()) ||
      (p.codigo || p.internalCode || '').toLowerCase().includes(productSearch.toLowerCase())
    );
  }, [products, productSearch]);

  const topClient = useMemo(() => {
    if (orders.length === 0) return null;
    const clientTotals: Record<string, number> = {};
    orders.forEach(o => {
      const orderItemsCount = o.items.reduce((sum, item) => sum + item.quantidade, 0);
      clientTotals[o.clientName] = (clientTotals[o.clientName] || 0) + orderItemsCount;
    });
    const sorted = Object.entries(clientTotals).sort((a, b) => b[1] - a[1]);
    return sorted[0];
  }, [orders]);

  const handleEmailAuth = async (e: React.FormEvent) => {
    e.preventDefault();
    setAuthError('');
    try {
      if (isRegistering) {
        await registerWithEmail(email, password);
      } else {
        await loginWithEmail(email, password);
      }
    } catch (error: any) {
      setAuthError(error.message);
    }
  };

  if (!authReady) {
    return (
      <div className="min-h-screen bg-brand-bg flex items-center justify-center">
        <div className="animate-spin rounded-full h-12 w-12 border-t-2 border-b-2 border-brand-orange"></div>
      </div>
    );
  }

  if (!user) {
    return (
      <div className="min-h-screen bg-brand-bg flex items-center justify-center p-4">
        <motion.div 
          initial={{ opacity: 0, scale: 0.9 }}
          animate={{ opacity: 1, scale: 1 }}
          className="bg-white p-8 md:p-12 rounded-[3rem] shadow-2xl max-w-md w-full text-center space-y-6 border border-slate-100"
        >
          <div className="flex justify-center">
            <div className="bg-white p-1 rounded-full shadow-2xl">
              <AtacadaoLogo className="w-24 h-24" />
            </div>
          </div>
          <div>
            <h1 className="text-4xl font-black tracking-tighter uppercase italic text-brand-green">ATACADÃO</h1>
            <p className="text-[10px] font-black text-brand-orange uppercase tracking-[0.3em] mt-1">Gestão de Pedidos</p>
          </div>

          <form onSubmit={handleEmailAuth} className="space-y-4 text-left">
            <div>
              <label className="text-[10px] font-black uppercase tracking-widest text-slate-400 ml-1">E-mail</label>
              <input 
                type="email" 
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                required
                className="w-full bg-slate-50 border-2 border-slate-100 rounded-2xl px-4 py-3 text-sm font-bold focus:border-brand-orange outline-none transition-all"
                placeholder="exemplo@empresa.com"
              />
            </div>
            <div>
              <label className="text-[10px] font-black uppercase tracking-widest text-slate-400 ml-1">Senha</label>
              <input 
                type="password" 
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                required
                className="w-full bg-slate-50 border-2 border-slate-100 rounded-2xl px-4 py-3 text-sm font-bold focus:border-brand-orange outline-none transition-all"
                placeholder="••••••••"
              />
            </div>
            {authError && (
              <p className="text-red-500 text-[10px] font-bold uppercase text-center">{authError}</p>
            )}
            <button 
              type="submit"
              className="w-full bg-brand-orange text-white py-4 rounded-2xl font-black uppercase tracking-widest text-xs hover:bg-brand-orange/90 transition-all shadow-xl shadow-brand-orange/20"
            >
              {isRegistering ? 'Criar Conta' : 'Entrar'}
            </button>
          </form>

          <div className="relative">
            <div className="absolute inset-0 flex items-center"><div className="w-full border-t border-slate-100"></div></div>
            <div className="relative flex justify-center text-[10px] uppercase font-black tracking-widest"><span className="bg-white px-2 text-slate-300">Ou</span></div>
          </div>

          <button 
            onClick={signInWithGoogle}
            className="w-full bg-white border-2 border-slate-100 text-slate-600 py-4 rounded-2xl font-black uppercase tracking-widest text-xs hover:bg-slate-50 transition-all flex items-center justify-center gap-3"
          >
            <LogIn size={18} className="text-brand-orange" />
            Entrar com Google
          </button>

          <button 
            onClick={() => setIsRegistering(!isRegistering)}
            className="text-[10px] font-black uppercase tracking-widest text-brand-orange hover:underline"
          >
            {isRegistering ? 'Já tenho uma conta' : 'Criar nova conta'}
          </button>
        </motion.div>
      </div>
    );
  }

  return (
    <div className="min-h-screen flex flex-col bg-brand-bg">
      {/* Header / Top Bar */}
      <header className="bg-brand-orange text-white shadow-lg z-20">
        <div className="max-w-7xl mx-auto px-4 h-16 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <AtacadaoLogo className="w-10 h-10" />
            <h1 className="text-2xl font-black tracking-tighter uppercase italic text-white">ATACADÃO</h1>
            <div className="h-6 w-px bg-white/30 mx-2 hidden md:block" />
            <span className="text-sm font-bold tracking-widest uppercase opacity-90 hidden md:block">Fluxo de Pedido</span>
          </div>
          
          <div className="flex items-center gap-1 md:gap-4">
            <div className="hidden md:flex flex-col items-end mr-2">
              <span className="text-xs font-bold text-brand-green bg-white px-2 py-0.5 rounded">{user.email}</span>
            </div>
            <button 
              onClick={() => setActiveTab('dashboard')}
              className={`p-2 rounded-lg transition-all ${activeTab === 'dashboard' ? 'bg-white text-brand-orange shadow-inner' : 'hover:bg-white/10'}`}
              title="Dashboard"
            >
              <LayoutDashboard size={22} />
            </button>
            <button 
              onClick={() => setActiveTab('new-order')}
              className={`p-2 rounded-lg transition-all ${activeTab === 'new-order' ? 'bg-white text-brand-orange shadow-inner' : 'hover:bg-white/10'}`}
              title="Novo Pedido"
            >
              <PlusCircle size={22} />
            </button>
            <button 
              onClick={() => setActiveTab('kanban')}
              className={`p-2 rounded-lg transition-all ${activeTab === 'kanban' ? 'bg-white text-brand-orange shadow-inner' : 'hover:bg-white/10'}`}
              title="Pedidos"
            >
              <Kanban size={22} />
            </button>
            <button 
              onClick={() => setActiveTab('clients')}
              className={`p-2 rounded-lg transition-all ${activeTab === 'clients' ? 'bg-white text-brand-orange shadow-inner' : 'hover:bg-white/10'}`}
              title="Clientes"
            >
              <Users size={22} />
            </button>
            <button 
              onClick={() => setActiveTab('products')}
              className={`p-2 rounded-lg transition-all ${activeTab === 'products' ? 'bg-white text-brand-orange shadow-inner' : 'hover:bg-white/10'}`}
              title="Produtos"
            >
              <Package size={22} />
            </button>
            <button 
              onClick={() => setActiveTab('import-export')}
              className={`p-2 rounded-lg transition-all ${activeTab === 'import-export' ? 'bg-white text-brand-orange shadow-inner' : 'hover:bg-white/10'}`}
              title="Importar/Exportar"
            >
              <FileUp size={22} />
            </button>
            <button 
              onClick={() => setActiveTab('registrations')}
              className={`p-2 rounded-lg transition-all ${activeTab === 'registrations' ? 'bg-white text-brand-orange shadow-inner' : 'hover:bg-white/10'}`}
              title="Cadastros"
            >
              <Plus size={22} />
            </button>
            <button 
              onClick={logout}
              className="p-2 rounded-lg transition-all hover:bg-white/10 text-white/70 hover:text-white"
              title="Sair"
            >
              <LogOut size={22} />
            </button>
          </div>
        </div>
      </header>

      {/* Main Content Area */}
      <main className="flex-1 p-4 md:p-8 overflow-y-auto">
        <AnimatePresence mode="wait">
          {activeTab === 'dashboard' && (
            <motion.div 
              key="dashboard"
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -10 }}
              className="max-w-7xl mx-auto space-y-8"
            >
              <div className="flex items-center gap-2">
                <div className="w-1 h-6 bg-brand-green rounded-full" />
                <h2 className="text-xs font-black text-brand-green uppercase tracking-[0.2em]">Visão Geral</h2>
              </div>

              {drillDownType ? (
                <div className="bg-white p-8 rounded-[2.5rem] shadow-sm border border-slate-200 space-y-6">
                  <div className="flex items-center justify-between">
                    <button 
                      onClick={() => {
                        setDrillDownType(null);
                        setDrillDownValue(null);
                      }}
                      className="flex items-center gap-2 text-slate-500 hover:text-brand-orange transition-colors font-black text-xs uppercase tracking-widest"
                    >
                      <ArrowLeft size={18} />
                      Voltar ao Dashboard
                    </button>
                    <h3 className="text-lg font-black text-brand-text uppercase tracking-tight">
                      Detalhes: {drillDownValue}
                    </h3>
                  </div>

                  <div className="overflow-x-auto">
                    <table className="w-full text-left">
                      <thead className="bg-slate-50 border-b border-slate-100">
                        <tr className="text-[10px] font-black text-slate-400 uppercase tracking-widest">
                          <th className="px-6 py-3">Pedido</th>
                          <th className="px-6 py-3">Data</th>
                          <th className="px-6 py-3">Cliente</th>
                          <th className="px-6 py-3">Quem fez o pedido</th>
                          <th className="px-6 py-3">Status</th>
                          <th className="px-6 py-3 text-right">Total</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-slate-50">
                        {drillDownData.map(order => (
                          <tr key={order.id} className="text-sm font-bold text-brand-text hover:bg-slate-50 transition-colors">
                            <td className="px-6 py-4">{order.id}</td>
                            <td className="px-6 py-4">{new Date(order.createdAt).toLocaleDateString()}</td>
                            <td className="px-6 py-4">{order.clientName}</td>
                            <td className="px-6 py-4">{order.nomeQuemFezPedido || 'N/I'}</td>
                            <td className="px-6 py-4">
                              <span className="px-2 py-1 bg-slate-100 rounded-lg text-[10px] uppercase tracking-tighter">
                                {order.status}
                              </span>
                            </td>
                            <td className="px-6 py-4 text-right">R$ {(order.total || 0).toFixed(2)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              ) : (
                <>
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                    <div className="bg-white p-6 rounded-2xl shadow-sm border-b-4 border-brand-orange">
                      <div className="flex items-center justify-between mb-4">
                        <span className="text-slate-500 text-xs font-bold uppercase tracking-wider">Total de Pedidos (Hoje)</span>
                        <div className="p-2 bg-brand-orange/10 text-brand-orange rounded-lg">
                          <TrendingUp size={20} />
                        </div>
                      </div>
                      <p className="text-3xl font-black text-brand-text">
                        {todayOrders.length}
                      </p>
                    </div>

                    <div className="bg-white p-6 rounded-2xl shadow-sm border-b-4 border-brand-green">
                      <div className="flex items-center justify-between mb-4">
                        <span className="text-slate-500 text-xs font-bold uppercase tracking-wider">Faturamento Total (Hoje)</span>
                        <div className="p-2 bg-brand-green/10 text-brand-green rounded-lg">
                          <Package size={20} />
                        </div>
                      </div>
                      <p className="text-3xl font-black text-brand-text">
                        R$ {todayOrders.reduce((acc, o) => acc + (o.total || 0), 0).toLocaleString('pt-BR', { minimumFractionDigits: 2 })}
                      </p>
                    </div>
                  </div>

                  <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
                    {/* Top Clients Chart */}
                    <div className="bg-white p-8 rounded-[2.5rem] shadow-sm border border-slate-200">
                      <h3 className="text-sm font-black text-brand-orange uppercase tracking-wider mb-6 flex items-center gap-2">
                        <Users size={18} />
                        Top 5 Clientes (Faturamento)
                      </h3>
                      <div className="h-[300px] w-full">
                        <ResponsiveContainer width="100%" height="100%">
                          <BarChart 
                            data={stats.topClients} 
                            layout="vertical"
                            onClick={(data) => {
                              if (data && data.activeLabel) {
                                setDrillDownType('client');
                                setDrillDownValue(data.activeLabel);
                              }
                            }}
                          >
                            <CartesianGrid strokeDasharray="3 3" horizontal={false} />
                            <XAxis type="number" hide />
                            <YAxis 
                              dataKey="name" 
                              type="category" 
                              width={150} 
                              tick={{ fontSize: 10, fontWeight: 'bold' }}
                            />
                            <Tooltip 
                              formatter={(value: number) => `R$ ${value.toFixed(2)}`}
                              contentStyle={{ borderRadius: '12px', border: 'none', boxShadow: '0 4px 12px rgba(0,0,0,0.1)' }}
                            />
                            <Bar dataKey="value" fill="#f37021" radius={[0, 4, 4, 0]}>
                              {stats.topClients.map((entry, index) => (
                                <Cell key={`cell-${index}`} fill={index === 0 ? '#f37021' : '#f9d423'} />
                              ))}
                            </Bar>
                          </BarChart>
                        </ResponsiveContainer>
                      </div>
                    </div>

                    {/* Top Sellers Chart */}
                    <div className="bg-white p-8 rounded-[2.5rem] shadow-sm border border-slate-200">
                      <h3 className="text-sm font-black text-brand-orange uppercase tracking-wider mb-6 flex items-center gap-2">
                        <TrendingUp size={18} />
                        Top 5 Vendedores (Vendas)
                      </h3>
                      <div className="h-[300px] w-full">
                        <ResponsiveContainer width="100%" height="100%">
                          <PieChart>
                            <Pie
                              data={stats.topSellers}
                              cx="50%"
                              cy="50%"
                              innerRadius={60}
                              outerRadius={80}
                              paddingAngle={5}
                              dataKey="value"
                              onClick={(data) => {
                                if (data && data.name) {
                                  setDrillDownType('vendedor');
                                  setDrillDownValue(data.name);
                                }
                              }}
                            >
                              {stats.topSellers.map((entry, index) => (
                                <Cell key={`cell-${index}`} fill={['#f37021', '#f9d423', '#00a651', '#334155', '#94a3b8'][index % 5]} />
                              ))}
                            </Pie>
                            <Tooltip formatter={(value: number) => `R$ ${value.toFixed(2)}`} />
                            <Legend />
                          </PieChart>
                        </ResponsiveContainer>
                      </div>
                    </div>

                    {/* Top Products Chart */}
                    <div className="bg-white p-8 rounded-[2.5rem] shadow-sm border border-slate-200 lg:col-span-2">
                      <h3 className="text-sm font-black text-brand-orange uppercase tracking-wider mb-6 flex items-center gap-2">
                        <Package size={18} />
                        Top 5 Produtos Mais Vendidos (Qtd)
                      </h3>
                      <div className="h-[300px] w-full">
                        <ResponsiveContainer width="100%" height="100%">
                          <BarChart 
                            data={stats.topProducts}
                            onClick={(data) => {
                              if (data && data.activeLabel) {
                                setDrillDownType('product');
                                setDrillDownValue(data.activeLabel);
                              }
                            }}
                          >
                            <CartesianGrid strokeDasharray="3 3" vertical={false} />
                            <XAxis dataKey="name" tick={{ fontSize: 10, fontWeight: 'bold' }} />
                            <YAxis tick={{ fontSize: 10, fontWeight: 'bold' }} />
                            <Tooltip 
                              contentStyle={{ borderRadius: '12px', border: 'none', boxShadow: '0 4px 12px rgba(0,0,0,0.1)' }}
                            />
                            <Bar dataKey="value" fill="#00a651" radius={[4, 4, 0, 0]} />
                          </BarChart>
                        </ResponsiveContainer>
                      </div>
                    </div>
                  </div>

                  <div className="bg-white p-8 rounded-[2.5rem] shadow-sm border border-slate-200">
                    <h3 className="text-sm font-black text-brand-orange uppercase tracking-wider mb-6 flex items-center gap-2">
                      <Kanban size={18} />
                      Status dos Pedidos
                    </h3>
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                      {ORDER_STATUSES.map(status => (
                        <div key={status} className="p-4 bg-slate-50 rounded-2xl border border-slate-100">
                          <p className="text-[10px] font-black text-slate-400 uppercase tracking-widest mb-1">{status}</p>
                          <p className="text-2xl font-black text-brand-text">
                            {orders.filter(o => o.status === status).length}
                          </p>
                        </div>
                      ))}
                    </div>
                  </div>
                </>
              )}
            </motion.div>
          )}

          {activeTab === 'new-order' && (
            <motion.div 
              key="new-order"
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -10 }}
              className="max-w-4xl mx-auto space-y-6"
            >
              <div className="flex items-center gap-2">
                <div className="w-1 h-6 bg-brand-green rounded-full" />
                <h2 className="text-xs font-black text-brand-green uppercase tracking-[0.2em]">Novo Pedido</h2>
              </div>

              {/* Client Section */}
              <section className="bg-white p-6 rounded-2xl shadow-sm border border-slate-200 space-y-4">
                <div className="flex items-center justify-between">
                  <h3 className="text-sm font-black text-brand-orange uppercase tracking-wider flex items-center gap-2">
                    <Users size={18} />
                    Dados do Cliente
                  </h3>
                  {!selectedClient && cnpjError && (
                    <div className="flex items-center gap-4">
                      <span className="text-xs font-bold text-red-500">{cnpjError}</span>
                      <button 
                        onClick={() => {
                          setEditingClient(null);
                          setClientForm({ cnpj: cnpjInput });
                          setIsClientModalOpen(true);
                        }}
                        className="text-xs font-bold text-brand-orange hover:underline flex items-center gap-1"
                      >
                        <UserPlus size={14} />
                        Cadastrar novo cliente
                      </button>
                    </div>
                  )}
                </div>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <div>
                    <label className="block text-[10px] font-black text-slate-400 uppercase tracking-widest mb-1">CNPJ</label>
                    <div className="flex gap-2">
                      <div className="relative flex-1">
                        <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" size={18} />
                        <input 
                          type="text" 
                          value={cnpjInput}
                          onChange={(e) => setCnpjInput(e.target.value)}
                          placeholder="00.000.000/0000-00"
                          className="w-full pl-10 pr-4 py-2.5 border border-slate-200 rounded-xl focus:ring-2 focus:ring-brand-orange focus:border-brand-orange outline-none transition-all"
                        />
                      </div>
                      <button 
                        onClick={handleCnpjSearch}
                        className="bg-brand-orange text-white px-4 rounded-xl font-bold hover:bg-brand-orange/90 transition-colors"
                      >
                        Buscar
                      </button>
                    </div>
                  </div>
                  <div>
                    <label className="block text-[10px] font-black text-slate-400 uppercase tracking-widest mb-1">Razão Social</label>
                    <input 
                      type="text" 
                      readOnly
                      value={selectedClient ? (selectedClient.razaoSocial || selectedClient.nomeFantasia) : cnpjInput ? 'Cliente não cadastrado' : ''}
                      className={`w-full px-4 py-2.5 border border-slate-200 rounded-xl bg-slate-50 font-bold outline-none ${!selectedClient && cnpjInput ? 'text-red-500' : 'text-brand-text'}`}
                    />
                  </div>
                  <div className="md:col-span-2">
                    <label className="block text-[10px] font-black text-slate-400 uppercase tracking-widest mb-1">Operador do Pedido</label>
                    {operators.length > 0 ? (
                      <select 
                        value={nomeQuemFezPedido}
                        onChange={(e) => setNomeQuemFezPedido(e.target.value)}
                        className="w-full px-4 py-2.5 border border-slate-200 rounded-xl focus:ring-2 focus:ring-brand-orange focus:border-brand-orange outline-none transition-all font-bold"
                      >
                        <option value="">Selecione um operador...</option>
                        {operators.map(op => (
                          <option key={op.id} value={op.nome}>{op.nome} (Matrícula: {op.matricula})</option>
                        ))}
                      </select>
                    ) : (
                      <input 
                        type="text" 
                        value={nomeQuemFezPedido}
                        onChange={(e) => setNomeQuemFezPedido(e.target.value)}
                        placeholder="Nome de quem fez o pedido (Cadastre operadores na aba Cadastros)"
                        className="w-full px-4 py-2.5 border border-slate-200 rounded-xl focus:ring-2 focus:ring-brand-orange focus:border-brand-orange outline-none transition-all font-bold"
                      />
                    )}
                  </div>
                </div>
              </section>

              {/* Product Section */}
              <section className="bg-white p-6 rounded-2xl shadow-sm border border-slate-200 space-y-4">
                <h3 className="text-sm font-black text-brand-orange uppercase tracking-wider flex items-center gap-2">
                  <Barcode size={18} />
                  Consulta de Mercadoria
                </h3>
                <div className="flex gap-2">
                  <div className="relative flex-1">
                    <Barcode className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" size={18} />
                    <input 
                      type="text" 
                      value={barcodeInput}
                      onChange={(e) => {
                        setBarcodeInput(e.target.value);
                        handleBarcodeSearch(e.target.value);
                      }}
                      onKeyDown={(e) => {
                        if (e.key === 'Enter') {
                          handleBarcodeSearch(barcodeInput);
                        }
                      }}
                      placeholder="Leia o código de barras ou digite o código interno"
                      className="w-full pl-10 pr-4 py-2.5 border border-slate-200 rounded-xl focus:ring-2 focus:ring-brand-orange focus:border-brand-orange outline-none transition-all"
                    />
                  </div>
                  <button 
                    onClick={() => setIsScanning(!isScanning)}
                    className={`p-2.5 rounded-xl border transition-all flex items-center justify-center ${isScanning ? 'bg-red-50 border-red-200 text-red-500' : 'bg-slate-50 border-slate-200 text-slate-500 hover:bg-slate-100'}`}
                    title="Usar Câmera"
                  >
                    <Scan size={20} />
                  </button>
                </div>

                {isScanning && (
                  <div className="relative bg-black rounded-2xl overflow-hidden aspect-video border-2 border-brand-orange">
                    <div id="reader" className="w-full h-full"></div>
                    <button 
                      onClick={() => setIsScanning(false)}
                      className="absolute top-2 right-2 p-2 bg-white/20 hover:bg-white/40 rounded-full text-white transition-all z-10"
                    >
                      <X size={20} />
                    </button>
                  </div>
                )}

                {scannedNotFound && !currentProduct && (
                  <motion.div 
                    initial={{ opacity: 0, y: -10 }}
                    animate={{ opacity: 1, y: 0 }}
                    className="p-4 bg-amber-50 rounded-2xl border border-amber-200 flex flex-col sm:flex-row items-center justify-between gap-4"
                  >
                    <div className="flex items-center gap-3">
                      <AlertCircle className="text-amber-500" size={24} />
                      <div>
                        <p className="text-sm font-black text-amber-900">Código não vinculado</p>
                        <p className="text-xs font-bold text-amber-700">O código <span className="font-black">{scannedNotFound}</span> não está cadastrado.</p>
                      </div>
                    </div>
                    <div className="flex flex-col sm:flex-row gap-2 w-full sm:w-auto">
                      <button 
                        onClick={() => {
                          setLinkingProduct(null);
                          setLinkSearch('');
                          setNewBarcode(scannedNotFound);
                          setIsLinkingFromOrder(true);
                        }}
                        className="px-4 py-2 bg-amber-500 text-white rounded-xl font-black text-xs uppercase tracking-widest hover:bg-amber-600 transition-all flex items-center justify-center gap-2"
                      >
                        <LinkIcon size={16} />
                        Vincular
                      </button>
                      <button 
                        onClick={() => {
                          setProductForm({
                            codigoInterno: '',
                            descricao: '',
                            embalagem: '',
                            multiplicador: 1,
                            precoUnitario: 0,
                            stock: 0,
                            dailySales: 0,
                            barcodes: { unidade: scannedNotFound }
                          });
                          setEditingProduct(null);
                          setIsProductModalOpen(true);
                        }}
                        className="px-4 py-2 bg-brand-green text-white rounded-xl font-black text-xs uppercase tracking-widest hover:bg-brand-green/90 transition-all flex items-center justify-center gap-2"
                      >
                        <Plus size={16} />
                        Cadastrar
                      </button>
                    </div>
                  </motion.div>
                )}

                {currentProduct && (
                  <motion.div 
                    initial={{ opacity: 0, height: 0 }}
                    animate={{ opacity: 1, height: 'auto' }}
                    className="p-4 bg-slate-50 rounded-2xl border border-slate-200 space-y-4"
                  >
                    <div className="flex justify-between items-start">
                      <div>
                        <p className="text-[10px] font-black text-slate-400 uppercase tracking-widest">Descrição</p>
                        <div className="flex items-center gap-2">
                          <p className="text-lg font-black text-brand-text">{currentProduct.descricao || currentProduct.description}</p>
                          {(currentProduct.embalagem || currentProduct.completo) && (
                            <span className="bg-brand-orange/10 text-brand-orange text-[10px] font-black px-2 py-0.5 rounded uppercase tracking-tighter">
                              {currentProduct.embalagem || currentProduct.completo}
                            </span>
                          )}
                        </div>
                        <p className="text-xs font-bold text-slate-500">Código: {currentProduct.codigo || currentProduct.internalCode}</p>
                      </div>
                    </div>

                    <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                      <div>
                        <label className="block text-[10px] font-black text-slate-400 uppercase tracking-widest mb-1">Quantidade</label>
                        <input 
                          type="text" 
                          value={itemQty === 0 ? '' : itemQty}
                          onChange={(e) => {
                            const val = e.target.value.replace(/\D/g, '');
                            const sanitizedVal = val.replace(/^0+/, '');
                            setItemQty(sanitizedVal === '' ? 0 : Number(sanitizedVal));
                          }}
                          className="w-full px-4 py-2 border border-slate-200 rounded-xl focus:ring-2 focus:ring-brand-orange outline-none font-bold"
                        />
                      </div>
                      <div>
                        <label className="block text-[10px] font-black text-slate-400 uppercase tracking-widest mb-1">Preço Unitário</label>
                        <input 
                          type="text" 
                          value={new Intl.NumberFormat('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(itemPrice)}
                          onChange={(e) => {
                            const rawValue = e.target.value.replace(/\D/g, '');
                            const numericValue = Number(rawValue) / 100;
                            setItemPrice(numericValue);
                          }}
                          className="w-full px-4 py-2 border border-slate-200 rounded-xl focus:ring-2 focus:ring-brand-orange outline-none font-bold"
                        />
                      </div>
                      <div>
                        <label className="block text-[10px] font-black text-slate-400 uppercase tracking-widest mb-1">Unidade</label>
                        <div className="flex bg-slate-100 p-1 rounded-xl">
                          <button 
                            onClick={() => setItemUnit('unidade')}
                            className={`flex-1 py-1.5 text-[10px] font-black uppercase tracking-widest rounded-lg transition-all ${itemUnit === 'unidade' ? 'bg-white text-brand-orange shadow-sm' : 'text-slate-400'}`}
                          >
                            UN
                          </button>
                          <button 
                            onClick={() => setItemUnit('caixa')}
                            className={`flex-1 py-1.5 text-[10px] font-black uppercase tracking-widest rounded-lg transition-all ${itemUnit === 'caixa' ? 'bg-white text-brand-orange shadow-sm' : 'text-slate-400'}`}
                          >
                            CX
                          </button>
                          <button 
                            onClick={() => setItemUnit('fardo')}
                            className={`flex-1 py-1.5 text-[10px] font-black uppercase tracking-widest rounded-lg transition-all ${itemUnit === 'fardo' ? 'bg-white text-brand-orange shadow-sm' : 'text-slate-400'}`}
                          >
                            FAR
                          </button>
                          <button 
                            onClick={() => setItemUnit('kg')}
                            className={`flex-1 py-1.5 text-[10px] font-black uppercase tracking-widest rounded-lg transition-all ${itemUnit === 'kg' ? 'bg-white text-brand-orange shadow-sm' : 'text-slate-400'}`}
                          >
                            KG
                          </button>
                        </div>
                      </div>
                      <div>
                        <label className="block text-[10px] font-black text-slate-400 uppercase tracking-widest mb-1">Mult. (Emb)</label>
                        <input 
                          type="number" 
                          value={itemMultiplier}
                          onChange={(e) => setItemMultiplier(Number(e.target.value))}
                          disabled={itemUnit === 'unidade'}
                          className={`w-full px-4 py-2 border border-slate-200 rounded-xl focus:ring-2 focus:ring-brand-orange outline-none font-bold ${itemUnit === 'unidade' ? 'bg-slate-100 text-slate-400' : 'bg-white text-brand-text'}`}
                        />
                      </div>

                      {itemUnit !== 'unidade' && (
                        <div className="col-span-2 md:col-span-4 p-3 bg-brand-orange/5 border border-brand-orange/10 rounded-xl flex justify-between items-center">
                          <span className="text-xs font-bold text-slate-500 uppercase">Preço da Caixa ({itemMultiplier} un):</span>
                          <span className="text-lg font-black text-brand-orange">
                            {new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(itemPrice * itemMultiplier)}
                          </span>
                        </div>
                      )}

                      <div className="col-span-2 md:col-span-4 flex items-end">
                        <button 
                          onClick={addItem}
                          className="w-full bg-brand-green text-white py-3 rounded-xl font-black uppercase tracking-widest text-xs hover:bg-brand-green/90 transition-all flex items-center justify-center gap-2 shadow-lg shadow-brand-green/20"
                        >
                          <Plus size={18} />
                          Adicionar ao Pedido
                        </button>
                      </div>
                    </div>
                  </motion.div>
                )}
              </section>

              {/* Items Table */}
              {orderItems.length > 0 && (
                <section className="bg-white rounded-2xl shadow-sm border border-slate-200 overflow-hidden">
                  <div className="overflow-x-auto">
                    <table className="w-full text-left">
                      <thead className="bg-slate-50 border-b border-slate-100">
                        <tr className="border-b border-slate-100 text-[10px] font-black text-slate-400 uppercase tracking-widest">
                          <th className="px-6 py-3">Cód</th>
                          <th className="px-6 py-3">Descrição</th>
                          <th className="px-6 py-3">Embalagem</th>
                          <th className="px-6 py-3">Qtd</th>
                          <th className="px-6 py-3">Unidade</th>
                          <th className="px-6 py-3">Preço</th>
                          <th className="px-6 py-3">Total</th>
                          <th className="px-6 py-3"></th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-slate-50">
                        {orderItems.map((item) => (
                          <tr key={item.id} className="text-sm font-bold text-brand-text hover:bg-slate-50 transition-colors">
                            <td className="px-6 py-4">{item.codigoInterno}</td>
                            <td className="px-6 py-4">{item.descricao}</td>
                            <td className="px-6 py-4 text-slate-400">{item.embalagem}</td>
                            <td className="px-6 py-4">{item.quantidade}</td>
                            <td className="px-6 py-4">{item.tipoVenda}</td>
                            <td className="px-6 py-4">R$ {item.precoUnitario.toFixed(2)}</td>
                            <td className="px-6 py-4">R$ {item.valorTotal.toFixed(2)}</td>
                            <td className="px-6 py-4">
                              <button 
                                onClick={() => removeItem(item.id)}
                                className="text-red-400 hover:text-red-600 p-1 transition-colors"
                              >
                                <Trash2 size={18} />
                              </button>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                  <div className="p-6 bg-slate-50 border-t border-slate-200 flex flex-col md:flex-row justify-between items-center gap-4">
                    <div className="text-center md:text-left">
                      <p className="text-[10px] font-black text-slate-400 uppercase tracking-widest">Valor Total do Pedido</p>
                      <p className="text-3xl font-black text-brand-orange">
                        R$ {orderItems.reduce((acc, item) => acc + item.valorTotal, 0).toFixed(2)}
                      </p>
                    </div>
                    <button 
                      onClick={finalizeOrder}
                      disabled={!selectedClient || !nomeQuemFezPedido.trim()}
                      className="w-full md:w-auto bg-brand-orange text-white px-12 py-4 rounded-2xl font-black uppercase tracking-widest hover:bg-brand-orange/90 transition-all shadow-lg shadow-brand-orange/20 disabled:opacity-50 disabled:shadow-none"
                    >
                      Finalizar Pedido
                    </button>
                  </div>
                </section>
              )}
            </motion.div>
          )}

          {activeTab === 'kanban' && (
            <motion.div 
              key="kanban"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              className="space-y-8"
            >
              <div className="flex items-center gap-2">
                <div className="w-1 h-6 bg-brand-green rounded-full" />
                <h2 className="text-xs font-black text-brand-green uppercase tracking-[0.2em]">Fluxo de Operação</h2>
              </div>

              <div className="flex gap-6 overflow-x-auto pb-8 kanban-container snap-x">
                {ORDER_STATUSES.map((status) => {
                  let bgColor = 'bg-slate-100';
                  let borderColor = 'border-slate-200';
                  let textColor = 'text-slate-500';
                  
                  if (status === 'Separação') {
                    bgColor = 'bg-[#FFF5F0]';
                    borderColor = 'border-brand-orange/20';
                    textColor = 'text-brand-orange/70';
                  } else if (status === 'Pedido Separado') {
                    bgColor = 'bg-brand-orange/5';
                    borderColor = 'border-brand-orange/40';
                    textColor = 'text-brand-orange';
                  } else if (status === 'Pedido Faturado') {
                    bgColor = 'bg-brand-orange/10';
                    borderColor = 'border-brand-orange/60';
                    textColor = 'text-brand-orange-dark';
                  } else if (status === 'Pedido Aguardando Retirada') {
                    bgColor = 'bg-brand-green/5';
                    borderColor = 'border-brand-green/30';
                    textColor = 'text-brand-green';
                  } else if (status === 'Pedido Retirado') {
                    bgColor = 'bg-brand-green/10';
                    borderColor = 'border-brand-green/50';
                    textColor = 'text-brand-green-dark';
                  }

                  return (
                    <div key={status} className={`kanban-column snap-center ${bgColor} ${borderColor}`}>
                      <div className="flex items-center justify-between mb-4">
                        <h3 className={`font-black uppercase text-[10px] tracking-[0.2em] ${textColor}`}>{status}</h3>
                        <span className="bg-white/80 text-brand-text px-2 py-0.5 rounded-full text-[10px] font-black shadow-sm">
                          {orders.filter(o => o.status === status).length}
                        </span>
                      </div>
                      
                      <div className="space-y-4">
                        {orders.filter(o => o.status === status && !o.archivedAt).map((order) => (
                          <motion.div 
                            layoutId={order.id}
                            key={order.id} 
                            className="order-card border-l-4 border-l-brand-orange"
                          >
                            <div className="flex justify-between items-start mb-2">
                              <div className="flex flex-col gap-1">
                                <span className="text-[10px] font-black text-brand-orange bg-brand-orange/5 px-2 py-1 rounded tracking-tighter w-fit">{order.id}</span>
                                <div className="scale-50 origin-left -my-3">
                                  <BarcodeGenerator value={order.id} />
                                </div>
                              </div>
                              <div className="flex flex-col items-end gap-1">
                                <span className="text-[9px] font-bold text-slate-400 uppercase">{new Date(order.createdAt).toLocaleTimeString([], {hour: '2-digit', minute:'2-digit'})}</span>
                                <span className="text-[8px] font-black text-brand-orange/60 uppercase tracking-tighter">{order.status}</span>
                              </div>
                            </div>
                            <h4 className="font-black text-brand-text text-sm mb-1 truncate uppercase">{order.clientName}</h4>
                            <p className="text-[11px] font-bold text-slate-400 mb-4 uppercase tracking-wider">
                              {order.items.reduce((sum, item) => sum + item.quantidade, 0)} itens totais
                            </p>
                            
                            <div className="flex flex-wrap gap-2 mt-auto">
                              {status === 'Separação' && (
                                <button 
                                  onClick={() => {
                                    setSeparatingOrder(order);
                                    setIsSeparationModalOpen(true);
                                  }}
                                  className="flex-1 min-w-[80px] bg-brand-orange text-white py-2.5 rounded-xl text-[10px] font-black flex items-center justify-center gap-1 hover:bg-brand-orange-dark transition-all shadow-md shadow-brand-orange/20"
                                >
                                  <PackageSearch size={12} />
                                  SEPARAÇÃO
                                </button>
                              )}
                              
                              {status === 'Pedido Retirado' ? (
                                <>
                                  <button 
                                    onClick={() => setViewingInvoice(order)}
                                    className="flex-1 min-w-[80px] bg-brand-green text-white py-2.5 rounded-xl text-[10px] font-black flex items-center justify-center gap-1 hover:opacity-90 transition-all shadow-md shadow-brand-green/20"
                                  >
                                    <Printer size={12} />
                                    NOTA
                                  </button>
                                  <button 
                                    onClick={() => handleArchiveOrder(order.id)}
                                    className="flex-1 min-w-[80px] bg-slate-400 text-white py-2.5 rounded-xl text-[10px] font-black flex items-center justify-center gap-1 hover:bg-slate-500 transition-all shadow-md shadow-slate-400/20"
                                  >
                                    <Archive size={12} />
                                    ARQUIVAR
                                  </button>
                                </>
                              ) : (
                                <>
                                  <button 
                                    onClick={() => setViewingInvoice(order)}
                                    className="flex-1 min-w-[80px] bg-brand-green text-white py-2.5 rounded-xl text-[10px] font-black flex items-center justify-center gap-1 hover:opacity-90 transition-all shadow-md shadow-brand-green/20"
                                  >
                                    <Printer size={12} />
                                    NOTA
                                  </button>
                                  <button 
                                    onClick={() => advanceStatus(order.id)}
                                    className="flex-1 min-w-[80px] min-h-[40px] bg-brand-orange text-white py-2.5 rounded-xl text-[10px] font-black flex items-center justify-center gap-1 hover:bg-brand-orange-dark transition-all shadow-md shadow-brand-orange/20 border-2 border-white/20"
                                  >
                                    {status === 'Pedido Aguardando Retirada' ? 'RETIRAR' : 'AVANÇAR'}
                                    <ChevronRight size={12} />
                                  </button>
                                </>
                              )}
                            </div>
                          </motion.div>
                        ))}
                      </div>
                    </div>
                  );
                })}
              </div>
            </motion.div>
          )}

          {activeTab === 'clients' && (
            <motion.div 
              key="clients"
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -10 }}
              className="max-w-6xl mx-auto space-y-6"
            >
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <div className="w-1 h-6 bg-brand-green rounded-full" />
                  <h2 className="text-xs font-black text-brand-green uppercase tracking-[0.2em]">Gestão de Clientes</h2>
                </div>
                <button 
                  onClick={() => {
                    setEditingClient(null);
                    setClientForm({
                      razaoSocial: '',
                      nomeFantasia: '',
                      cnpj: '',
                      inscricaoEstadual: '',
                      email: '',
                      telefone: '',
                      endereco: '',
                      cidade: '',
                      estado: '',
                      cep: ''
                    });
                    setIsClientModalOpen(true);
                  }}
                  className="bg-brand-orange text-white px-6 py-2.5 rounded-xl font-black uppercase text-xs tracking-widest hover:bg-brand-orange/90 transition-all flex items-center gap-2 shadow-lg shadow-brand-orange/20"
                >
                  <UserPlus size={18} />
                  Novo Cliente
                </button>
              </div>

              <div className="bg-white p-4 rounded-2xl shadow-sm border border-slate-200">
                <div className="relative">
                  <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" size={20} />
                  <input 
                    type="text" 
                    placeholder="Buscar por Razão Social, Nome Fantasia ou CNPJ..."
                    value={clientSearch}
                    onChange={(e) => setClientSearch(e.target.value)}
                    className="w-full pl-10 pr-4 py-3 border border-slate-200 rounded-xl focus:ring-2 focus:ring-brand-orange outline-none transition-all font-bold"
                  />
                </div>
              </div>

              <div className="bg-white rounded-2xl shadow-sm border border-slate-200 overflow-hidden">
                <div className="overflow-x-auto">
                  <table className="w-full text-left">
                    <thead>
                      <tr className="border-b border-slate-100 text-[10px] font-black text-slate-400 uppercase tracking-widest">
                        <th className="px-6 py-4">Razão Social / Fantasia</th>
                        <th className="px-6 py-4">CNPJ</th>
                        <th className="px-6 py-4">Contato</th>
                        <th className="px-6 py-4">Localização</th>
                        <th className="px-6 py-4 text-right">Ações</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-50">
                      {filteredClients.map((client) => (
                        <tr key={client.id} className="hover:bg-slate-50 transition-colors">
                          <td className="px-6 py-4">
                            <p className="font-black text-brand-text uppercase">{client.razaoSocial}</p>
                            <p className="text-xs font-bold text-slate-500 uppercase">{client.nomeFantasia}</p>
                          </td>
                          <td className="px-6 py-4 font-mono text-xs font-bold text-slate-600">{client.cnpj}</td>
                          <td className="px-6 py-4">
                            <p className="text-xs font-bold text-brand-text">{client.email}</p>
                            <p className="text-xs font-bold text-slate-500">{client.telefone}</p>
                          </td>
                          <td className="px-6 py-4">
                            <p className="text-xs font-bold text-brand-text uppercase">{client.cidade} - {client.estado}</p>
                          </td>
                          <td className="px-6 py-4 text-right">
                            <div className="flex justify-end gap-2">
                              <button 
                                onClick={() => {
                                  setEditingClient(client);
                                  setClientForm({ ...client });
                                  setIsClientModalOpen(true);
                                }}
                                className="p-2 text-slate-400 hover:text-brand-orange transition-colors"
                              >
                                <Edit2 size={18} />
                              </button>
                              <button 
                                onClick={() => handleDeleteClient(client.id)}
                                className="p-2 text-slate-400 hover:text-red-500 transition-colors"
                              >
                                <Trash2 size={18} />
                              </button>
                            </div>
                          </td>
                        </tr>
                      ))}
                      {filteredClients.length === 0 && (
                        <tr>
                          <td colSpan={5} className="px-6 py-12 text-center text-slate-400 font-bold">
                            Nenhum cliente encontrado.
                          </td>
                        </tr>
                      )}
                    </tbody>
                  </table>
                </div>
              </div>
            </motion.div>
          )}

          {activeTab === 'products' && (
            <motion.div 
              key="products"
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -10 }}
              className="max-w-6xl mx-auto space-y-6"
            >
              <div className="flex items-center justify-between gap-4">
                <div className="flex items-center gap-2">
                  <div className="w-1 h-6 bg-brand-green rounded-full" />
                  <h2 className="text-xs font-black text-brand-green uppercase tracking-[0.2em]">Catálogo de Produtos</h2>
                </div>
                <button 
                  onClick={() => {
                    setEditingProduct(null);
                    setProductForm({
                      multiplicador: 1,
                      precoUnitario: 0,
                      barcodes: { unidade: '' }
                    });
                    setIsProductModalOpen(true);
                  }}
                  className="bg-brand-green text-white px-4 py-2 rounded-xl font-black text-[10px] uppercase tracking-widest hover:bg-brand-green/90 transition-all flex items-center gap-2 shadow-lg shadow-brand-green/20"
                >
                  <Plus size={16} />
                  Novo Produto
                </button>
              </div>

              <div className="bg-white p-4 rounded-2xl shadow-sm border border-slate-200">
                <div className="relative">
                  <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" size={20} />
                  <input 
                    type="text" 
                    placeholder="Buscar por Código ou Descrição..."
                    value={productSearch}
                    onChange={(e) => setProductSearch(e.target.value)}
                    className="w-full pl-10 pr-4 py-3 border border-slate-200 rounded-xl focus:ring-2 focus:ring-brand-orange outline-none transition-all font-bold"
                  />
                </div>
              </div>

              <div className="bg-white rounded-2xl shadow-sm border border-slate-200 overflow-hidden">
                <div className="overflow-x-auto">
                  <table className="w-full text-left">
                    <thead>
                      <tr className="border-b border-slate-100 text-[10px] font-black text-slate-400 uppercase tracking-widest">
                        <th className="px-6 py-4">Código</th>
                        <th className="px-6 py-4">Descrição</th>
                        <th className="px-6 py-4">Embalagem</th>
                        <th className="px-6 py-4 text-right">Ações</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-50">
                      {filteredProducts.slice(0, 5).map((product) => (
                        <tr key={product.id} className="hover:bg-slate-50 transition-colors">
                          <td className="px-6 py-4 font-mono text-sm font-black text-brand-orange">
                            {product.codigoInterno || product.codigo || product.internalCode}
                          </td>
                          <td className="px-6 py-4 font-black text-brand-text uppercase">
                            {product.descricao || product.description}
                          </td>
                          <td className="px-6 py-4 font-bold text-slate-500 uppercase">
                            {product.embalagem || product.completo}
                          </td>
                          <td className="px-6 py-4 text-right">
                            <div className="flex justify-end gap-2">
                              <button 
                                onClick={() => {
                                  setEditingProduct(product);
                                  setProductForm({ ...product });
                                  setIsProductModalOpen(true);
                                }}
                                className="p-2 text-slate-400 hover:text-brand-orange transition-colors"
                              >
                                <Edit2 size={18} />
                              </button>
                              <button 
                                onClick={() => handleDeleteProduct(product.id)}
                                className="p-2 text-slate-400 hover:text-red-500 transition-colors"
                              >
                                <Trash2 size={18} />
                              </button>
                            </div>
                          </td>
                        </tr>
                      ))}
                      {filteredProducts.length === 0 && (
                        <tr>
                          <td colSpan={4} className="px-6 py-12 text-center text-slate-400 font-bold">
                            Nenhum produto encontrado.
                          </td>
                        </tr>
                      )}
                    </tbody>
                  </table>
                </div>
              </div>
            </motion.div>
          )}

          {activeTab === 'registrations' && (
            <motion.div 
              key="registrations"
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -10 }}
              className="max-w-4xl mx-auto space-y-8"
            >
              <div className="flex items-center gap-2">
                <div className="w-1 h-6 bg-brand-green rounded-full" />
                <h2 className="text-xs font-black text-brand-green uppercase tracking-[0.2em]">Cadastros e Vínculos</h2>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
                {/* Client Registration Card */}
                <div className="bg-white p-8 rounded-[2.5rem] shadow-sm border border-slate-200 space-y-6">
                  <div className="bg-brand-green/10 w-12 h-12 rounded-2xl flex items-center justify-center text-brand-green">
                    <UserPlus size={24} />
                  </div>
                  <div>
                    <h3 className="text-lg font-black text-brand-text uppercase italic">Novo Cliente</h3>
                    <p className="text-xs font-bold text-slate-500">Cadastre um novo cliente com CNPJ e Razão Social.</p>
                  </div>
                  <button 
                    onClick={() => {
                      setEditingClient(null);
                      setClientForm({});
                      setIsClientModalOpen(true);
                    }}
                    className="w-full bg-brand-green text-white py-4 rounded-2xl font-black uppercase tracking-widest text-xs hover:bg-brand-green/90 transition-all flex items-center justify-center gap-2 shadow-lg shadow-brand-green/20"
                  >
                    <Plus size={18} />
                    Abrir Cadastro
                  </button>
                </div>

                {/* Barcode Linking Card */}
                <div className="bg-white p-8 rounded-[2.5rem] shadow-sm border border-slate-200 space-y-6">
                  <div className="bg-brand-orange/10 w-12 h-12 rounded-2xl flex items-center justify-center text-brand-orange">
                    <Barcode size={24} />
                  </div>
                  <div>
                    <h3 className="text-lg font-black text-brand-text uppercase italic">Vincular EAN</h3>
                    <p className="text-xs font-bold text-slate-500">Vincule um código de barras a um produto existente.</p>
                  </div>
                  
                  <div className="space-y-4">
                    <div className="relative">
                      <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" size={18} />
                      <input 
                        type="text" 
                        placeholder="Buscar produto..."
                        value={linkSearch}
                        onChange={(e) => setLinkSearch(e.target.value)}
                        className="w-full pl-10 pr-4 py-3 border border-slate-200 rounded-xl focus:ring-2 focus:ring-brand-orange outline-none font-bold text-sm"
                      />
                      
                      {filteredLinkProducts.length > 0 && !linkingProduct && (
                        <div className="absolute top-full left-0 right-0 bg-white border border-slate-200 rounded-xl mt-2 shadow-xl z-10 overflow-hidden">
                          {filteredLinkProducts.map(p => (
                            <button
                              key={p.id}
                              onClick={() => {
                                setLinkingProduct(p);
                                setLinkSearch(p.descricao || p.description || '');
                              }}
                              className="w-full px-4 py-3 text-left hover:bg-slate-50 flex justify-between items-center border-b border-slate-50 last:border-0"
                            >
                              <span className="font-bold text-xs uppercase truncate">{p.descricao || p.description}</span>
                              <span className="text-[10px] font-black text-brand-orange">{p.codigo || p.internalCode}</span>
                            </button>
                          ))}
                        </div>
                      )}
                    </div>

                    {linkingProduct && (
                      <motion.div 
                        initial={{ opacity: 0, height: 0 }}
                        animate={{ opacity: 1, height: 'auto' }}
                        className="space-y-4 pt-2"
                      >
                        <div className="p-3 bg-slate-50 rounded-xl border border-slate-100 flex justify-between items-center">
                          <span className="text-[10px] font-black text-slate-400 uppercase">Selecionado: {linkingProduct.codigo}</span>
                          <button onClick={() => setLinkingProduct(null)} className="text-slate-400 hover:text-red-500">
                            <X size={14} />
                          </button>
                        </div>
                        
                        <div className="space-y-2">
                          <label className="text-[10px] font-black uppercase tracking-widest text-slate-400 ml-1">Tipo de Código</label>
                          <div className="flex bg-slate-100 p-1 rounded-xl">
                            <button 
                              onClick={() => setLinkingBarcodeType('unidade')}
                              className={`flex-1 py-1.5 text-[10px] font-black uppercase tracking-widest rounded-lg transition-all ${linkingBarcodeType === 'unidade' ? 'bg-white text-brand-orange shadow-sm' : 'text-slate-400'}`}
                            >
                              Unidade (UN)
                            </button>
                            <button 
                              onClick={() => setLinkingBarcodeType('caixa')}
                              className={`flex-1 py-1.5 text-[10px] font-black uppercase tracking-widest rounded-lg transition-all ${linkingBarcodeType === 'caixa' ? 'bg-white text-brand-orange shadow-sm' : 'text-slate-400'}`}
                            >
                              Caixa (CX)
                            </button>
                          </div>
                        </div>

                        <input 
                          type="text" 
                          placeholder="Novo Código de Barras (EAN)"
                          value={newBarcode}
                          onChange={(e) => setNewBarcode(e.target.value)}
                          className="w-full px-4 py-3 border border-slate-200 rounded-xl focus:ring-2 focus:ring-brand-orange outline-none font-bold text-sm"
                        />
                        <button 
                          onClick={handleLinkBarcode}
                          className="w-full bg-brand-orange text-white py-4 rounded-2xl font-black uppercase tracking-widest text-xs hover:bg-brand-orange/90 transition-all flex items-center justify-center gap-2 shadow-lg shadow-brand-orange/20"
                        >
                          <Save size={18} />
                          Confirmar Vínculo
                        </button>
                      </motion.div>
                    )}
                  </div>
                </div>
              </div>
            </motion.div>
          )}
          {activeTab === 'import-export' && (
            <motion.div 
              key="import-export"
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -10 }}
              className="max-w-4xl mx-auto space-y-8"
            >
              <div className="flex items-center gap-2">
                <div className="w-1 h-6 bg-brand-green rounded-full" />
                <h2 className="text-xs font-black text-brand-green uppercase tracking-[0.2em]">Importação e Exportação</h2>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
                {/* Products Import/Export */}
                <div className="bg-white p-8 rounded-[2.5rem] shadow-sm border border-slate-200 space-y-6">
                  <div className="bg-brand-orange/10 w-12 h-12 rounded-2xl flex items-center justify-center text-brand-orange">
                    <Package size={24} />
                  </div>
                  <div>
                    <h3 className="text-lg font-black text-brand-text uppercase italic">Produtos</h3>
                    <p className="text-xs font-bold text-slate-500">Gerencie seu catálogo via planilhas Excel ou CSV.</p>
                  </div>
                  <div className="space-y-3">
                    <label className="block w-full">
                      <input 
                        type="file" 
                        accept=".xlsx,.csv" 
                        onChange={(e) => {
                          const event = { ...e, target: { ...e.target, files: e.target.files } };
                          checkPassword(() => handleImportProducts(event as any));
                        }}
                        className="hidden"
                      />
                      <div className="w-full bg-brand-orange text-white py-4 rounded-2xl font-black uppercase tracking-widest text-xs hover:bg-brand-orange/90 transition-all flex items-center justify-center gap-2 cursor-pointer shadow-lg shadow-brand-orange/20">
                        <Upload size={18} />
                        Importar Produtos
                      </div>
                    </label>
                    <button 
                      onClick={() => checkPassword(handleExportProducts)}
                      className="w-full bg-white text-brand-orange border-2 border-brand-orange py-4 rounded-2xl font-black uppercase tracking-widest text-xs hover:bg-brand-orange/5 transition-all flex items-center justify-center gap-2"
                    >
                      <Download size={18} />
                      Exportar Produtos
                    </button>
                    <button 
                      onClick={() => {
                        const ws = XLSX.utils.json_to_sheet([
                          { 'Código': '1001', 'Descrição': 'ARROZ 5KG', 'Embalagem': 'FDO 1 X 6 X 5KG' },
                          { 'Código': '1002', 'Descrição': 'FEIJÃO 1KG', 'Embalagem': 'FDO 1 X 10 X 1KG' }
                        ]);
                        const wb = XLSX.utils.book_new();
                        XLSX.utils.book_append_sheet(wb, ws, "Modelo");
                        XLSX.writeFile(wb, "modelo_importacao_produtos.xlsx");
                      }}
                      className="w-full bg-slate-100 text-slate-600 py-3 rounded-2xl font-black uppercase tracking-widest text-[10px] hover:bg-slate-200 transition-all flex items-center justify-center gap-2"
                    >
                      <FileDown size={14} />
                      Baixar Modelo de Planilha
                    </button>
                    <button 
                      onClick={() => checkPassword(handleClearProducts)}
                      className="w-full bg-red-50 text-red-500 border-2 border-red-100 py-4 rounded-2xl font-black uppercase tracking-widest text-[10px] hover:bg-red-100 transition-all flex items-center justify-center gap-2"
                    >
                      <Trash2 size={14} />
                      Limpar Base de Produtos
                    </button>
                    <button 
                      onClick={() => checkPassword(handleRestoreDefaultProducts)}
                      className="w-full bg-slate-50 text-slate-500 border-2 border-slate-100 py-4 rounded-2xl font-black uppercase tracking-widest text-[10px] hover:bg-slate-100 transition-all flex items-center justify-center gap-2"
                    >
                      <RotateCcw size={14} />
                      Restaurar Produtos Padrão
                    </button>
                  </div>
                  <div className="p-4 bg-slate-50 rounded-2xl border border-slate-100">
                    <p className="text-[10px] font-black text-slate-400 uppercase tracking-widest mb-2">Formato Esperado</p>
                    <p className="text-[10px] font-bold text-slate-500">Colunas: <span className="text-brand-orange">Código | Descrição | Embalagem</span></p>
                  </div>
                </div>

                {/* Clients Import/Export */}
                <div className="bg-white p-8 rounded-[2.5rem] shadow-sm border border-slate-200 space-y-6">
                  <div className="bg-brand-green/10 w-12 h-12 rounded-2xl flex items-center justify-center text-brand-green">
                    <Users size={24} />
                  </div>
                  <div>
                    <h3 className="text-lg font-black text-brand-text uppercase italic">Clientes</h3>
                    <p className="text-xs font-bold text-slate-500">Importe ou exporte sua base de clientes cadastrados.</p>
                  </div>
                  <div className="space-y-3">
                    <label className="block w-full">
                      <input 
                        type="file" 
                        accept=".xlsx,.csv" 
                        onChange={(e) => {
                          const event = { ...e, target: { ...e.target, files: e.target.files } };
                          checkPassword(() => handleImportClients(event as any));
                        }}
                        className="hidden"
                      />
                      <div className="w-full bg-brand-green text-white py-4 rounded-2xl font-black uppercase tracking-widest text-xs hover:bg-brand-green/90 transition-all flex items-center justify-center gap-2 cursor-pointer shadow-lg shadow-brand-green/20">
                        <Upload size={18} />
                        Importar Clientes
                      </div>
                    </label>
                    <button 
                      onClick={() => checkPassword(handleExportClients)}
                      className="w-full bg-white text-brand-green border-2 border-brand-green py-4 rounded-2xl font-black uppercase tracking-widest text-xs hover:bg-brand-green/5 transition-all flex items-center justify-center gap-2"
                    >
                      <Download size={18} />
                      Exportar Clientes
                    </button>
                    <button 
                      onClick={() => {
                        const ws = XLSX.utils.json_to_sheet([
                          { 'Razão Social': 'CLIENTE EXEMPLO LTDA', 'CNPJ': '12.345.678/0001-99', 'Nome Fantasia': 'EXEMPLO', 'IE': '123456789', 'Telefone': '(45) 99999-9999', 'Email': 'contato@exemplo.com' }
                        ]);
                        const wb = XLSX.utils.book_new();
                        XLSX.utils.book_append_sheet(wb, ws, "Modelo");
                        XLSX.writeFile(wb, "modelo_importacao_clientes.xlsx");
                      }}
                      className="w-full bg-slate-100 text-slate-600 py-3 rounded-2xl font-black uppercase tracking-widest text-[10px] hover:bg-slate-200 transition-all flex items-center justify-center gap-2"
                    >
                      <FileDown size={14} />
                      Baixar Modelo de Planilha
                    </button>
                    <button 
                      onClick={() => checkPassword(handleClearClients)}
                      className="w-full bg-red-50 text-red-500 border-2 border-red-100 py-4 rounded-2xl font-black uppercase tracking-widest text-[10px] hover:bg-red-100 transition-all flex items-center justify-center gap-2"
                    >
                      <Trash2 size={14} />
                      Limpar Base de Clientes
                    </button>
                    <button 
                      onClick={() => checkPassword(handleRestoreDefaultClients)}
                      className="w-full bg-slate-50 text-slate-500 border-2 border-slate-100 py-4 rounded-2xl font-black uppercase tracking-widest text-[10px] hover:bg-slate-100 transition-all flex items-center justify-center gap-2"
                    >
                      <RotateCcw size={14} />
                      Restaurar Clientes Padrão
                    </button>
                  </div>
                  <div className="p-4 bg-slate-50 rounded-2xl border border-slate-100">
                    <p className="text-[10px] font-black text-slate-400 uppercase tracking-widest mb-2">Dica</p>
                    <p className="text-[10px] font-bold text-slate-500">A importação atualiza clientes existentes via <span className="text-brand-green">CNPJ</span>.</p>
                  </div>
                </div>

                {/* Barcode Mapping Import */}
                <div className="bg-white p-8 rounded-[2.5rem] shadow-sm border border-slate-200 space-y-6">
                  <div className="bg-slate-900/10 w-12 h-12 rounded-2xl flex items-center justify-center text-slate-900">
                    <Barcode size={24} />
                  </div>
                  <div>
                    <h3 className="text-lg font-black text-brand-text uppercase italic">Vínculo de EAN</h3>
                    <p className="text-xs font-bold text-slate-500">Vincule códigos de barras aos códigos internos.</p>
                  </div>
                  <div className="space-y-3">
                    <label className="block w-full">
                      <input 
                        type="file" 
                        accept=".xlsx,.csv" 
                        onChange={handleImportBarcodes}
                        className="hidden"
                      />
                      <div className="w-full bg-slate-900 text-white py-4 rounded-2xl font-black uppercase tracking-widest text-xs hover:bg-slate-800 transition-all flex items-center justify-center gap-2 cursor-pointer shadow-lg shadow-slate-900/20">
                        <Upload size={18} />
                        Importar Vínculos
                      </div>
                    </label>
                    <button 
                      onClick={() => checkPassword(handleClearOrders)}
                      className="w-full bg-red-50 text-red-500 border-2 border-red-100 py-4 rounded-2xl font-black uppercase tracking-widest text-[10px] hover:bg-red-100 transition-all flex items-center justify-center gap-2"
                    >
                      <Trash2 size={14} />
                      Limpar Base de Pedidos
                    </button>
                  </div>
                  <div className="p-4 bg-slate-50 rounded-2xl border border-slate-100">
                    <p className="text-[10px] font-black text-slate-400 uppercase tracking-widest mb-2">Formato Esperado</p>
                    <p className="text-[10px] font-bold text-slate-500">Colunas: <span className="text-brand-orange">Código de Barras | Código Interno</span></p>
                  </div>
                </div>

                {/* Operator Management */}
                <div className="bg-white p-8 rounded-[2.5rem] shadow-sm border border-slate-200 space-y-6">
                  <div className="bg-brand-orange/10 w-12 h-12 rounded-2xl flex items-center justify-center text-brand-orange">
                    <UserCheck size={24} />
                  </div>
                  <div>
                    <h3 className="text-lg font-black text-brand-text uppercase italic">Operadores</h3>
                    <p className="text-xs font-bold text-slate-500">Gerencie os operadores do sistema.</p>
                  </div>
                  <div className="space-y-3">
                    <button 
                      onClick={() => checkPassword(() => {
                        setEditingOperator(null);
                        setOperatorForm({});
                        setIsOperatorModalOpen(true);
                      })}
                      className="w-full bg-brand-orange text-white py-4 rounded-2xl font-black uppercase tracking-widest text-xs hover:bg-brand-orange/90 transition-all flex items-center justify-center gap-2 shadow-lg shadow-brand-orange/20"
                    >
                      <Plus size={18} />
                      Cadastrar Operador
                    </button>
                    
                    <div className="max-h-48 overflow-y-auto space-y-2 pr-2 custom-scrollbar">
                      {operators.length === 0 ? (
                        <p className="text-[10px] font-bold text-slate-400 text-center py-4 uppercase tracking-widest">Nenhum operador cadastrado</p>
                      ) : (
                        operators.map(op => (
                          <div key={op.id} className="flex items-center justify-between p-3 bg-slate-50 rounded-xl border border-slate-100 group">
                            <div>
                              <p className="text-xs font-black text-brand-text uppercase">{op.nome}</p>
                              <p className="text-[10px] font-bold text-slate-400 uppercase">Matrícula: {op.matricula}</p>
                            </div>
                            <div className="flex gap-1">
                              <button 
                                onClick={() => checkPassword(() => {
                                  setEditingOperator(op);
                                  setOperatorForm(op);
                                  setIsOperatorModalOpen(true);
                                })}
                                className="p-2 text-slate-400 hover:text-brand-orange transition-colors bg-white rounded-lg border border-slate-100 shadow-sm"
                              >
                                <Edit2 size={14} />
                              </button>
                              <button 
                                onClick={() => checkPassword(() => handleDeleteOperator(op.id))}
                                className="p-2 text-slate-400 hover:text-red-500 transition-colors bg-white rounded-lg border border-slate-100 shadow-sm"
                              >
                                <Trash2 size={14} />
                              </button>
                            </div>
                          </div>
                        ))
                      )}
                    </div>
                  </div>
                </div>
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </main>

      {/* Client Modal */}
      <AnimatePresence>
        {isClientModalOpen && (
          <div className="fixed inset-0 bg-brand-text/60 backdrop-blur-sm flex items-center justify-center p-4 z-50">
            <motion.div 
              initial={{ scale: 0.95, opacity: 0 }}
              animate={{ scale: 1, opacity: 1 }}
              exit={{ scale: 0.95, opacity: 0 }}
              className="bg-white w-full max-w-2xl rounded-[2.5rem] shadow-2xl overflow-hidden border border-slate-100"
            >
              <div className="p-8 space-y-6">
                <div className="flex justify-between items-center">
                  <div className="flex items-center gap-3">
                    <div className="bg-brand-orange p-2 rounded-xl">
                      <UserPlus className="text-white w-6 h-6" />
                    </div>
                    <div>
                      <h2 className="text-xl font-black text-brand-text tracking-tighter uppercase italic">
                        {editingClient ? 'Editar Cliente' : 'Novo Cliente'}
                      </h2>
                      <p className="text-[10px] font-black text-brand-orange uppercase tracking-[0.3em]">Cadastro de Parceiro</p>
                    </div>
                  </div>
                  <button onClick={() => setIsClientModalOpen(false)} className="p-2 hover:bg-slate-100 rounded-full transition-colors">
                    <X size={24} className="text-slate-400" />
                  </button>
                </div>

                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <div className="space-y-4">
                    <div>
                      <label className="block text-[10px] font-black text-slate-400 uppercase tracking-widest mb-1">Razão Social</label>
                      <input 
                        type="text" 
                        value={clientForm.razaoSocial}
                        onChange={(e) => setClientForm({ ...clientForm, razaoSocial: e.target.value })}
                        className="w-full px-4 py-2 border border-slate-200 rounded-xl focus:ring-2 focus:ring-brand-orange outline-none font-bold"
                        placeholder="Ex: Atacadão S.A."
                      />
                    </div>
                    <div>
                      <label className="block text-[10px] font-black text-slate-400 uppercase tracking-widest mb-1">Nome Fantasia</label>
                      <input 
                        type="text" 
                        value={clientForm.nomeFantasia}
                        onChange={(e) => setClientForm({ ...clientForm, nomeFantasia: e.target.value })}
                        className="w-full px-4 py-2 border border-slate-200 rounded-xl focus:ring-2 focus:ring-brand-orange outline-none font-bold"
                        placeholder="Ex: Atacadão"
                      />
                    </div>
                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <label className="block text-[10px] font-black text-slate-400 uppercase tracking-widest mb-1">CNPJ</label>
                        <input 
                          type="text" 
                          value={clientForm.cnpj}
                          onChange={(e) => setClientForm({ ...clientForm, cnpj: e.target.value })}
                          className="w-full px-4 py-2 border border-slate-200 rounded-xl focus:ring-2 focus:ring-brand-orange outline-none font-bold"
                          placeholder="00.000.000/0000-00"
                        />
                      </div>
                      <div>
                        <label className="block text-[10px] font-black text-slate-400 uppercase tracking-widest mb-1">Insc. Estadual</label>
                        <input 
                          type="text" 
                          value={clientForm.inscricaoEstadual}
                          onChange={(e) => setClientForm({ ...clientForm, inscricaoEstadual: e.target.value })}
                          className="w-full px-4 py-2 border border-slate-200 rounded-xl focus:ring-2 focus:ring-brand-orange outline-none font-bold"
                        />
                      </div>
                    </div>
                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <label className="block text-[10px] font-black text-slate-400 uppercase tracking-widest mb-1">E-mail</label>
                        <input 
                          type="email" 
                          value={clientForm.email}
                          onChange={(e) => setClientForm({ ...clientForm, email: e.target.value })}
                          className="w-full px-4 py-2 border border-slate-200 rounded-xl focus:ring-2 focus:ring-brand-orange outline-none font-bold"
                          placeholder="contato@cliente.com"
                        />
                      </div>
                      <div>
                        <label className="block text-[10px] font-black text-slate-400 uppercase tracking-widest mb-1">Contato</label>
                        <input 
                          type="text" 
                          value={clientForm.contato}
                          onChange={(e) => setClientForm({ ...clientForm, contato: e.target.value })}
                          className="w-full px-4 py-2 border border-slate-200 rounded-xl focus:ring-2 focus:ring-brand-orange outline-none font-bold"
                        />
                      </div>
                    </div>
                  </div>

                  <div className="space-y-4">
                    <div>
                      <label className="block text-[10px] font-black text-slate-400 uppercase tracking-widest mb-1">Telefone</label>
                      <input 
                        type="text" 
                        value={clientForm.telefone}
                        onChange={(e) => setClientForm({ ...clientForm, telefone: e.target.value })}
                        className="w-full px-4 py-2 border border-slate-200 rounded-xl focus:ring-2 focus:ring-brand-orange outline-none font-bold"
                        placeholder="(00) 00000-0000"
                      />
                    </div>
                    <div className="grid grid-cols-3 gap-4">
                      <div className="col-span-2">
                        <label className="block text-[10px] font-black text-slate-400 uppercase tracking-widest mb-1">Endereço</label>
                        <input 
                          type="text" 
                          value={clientForm.endereco}
                          onChange={(e) => setClientForm({ ...clientForm, endereco: e.target.value })}
                          className="w-full px-4 py-2 border border-slate-200 rounded-xl focus:ring-2 focus:ring-brand-orange outline-none font-bold"
                        />
                      </div>
                      <div>
                        <label className="block text-[10px] font-black text-slate-400 uppercase tracking-widest mb-1">Número</label>
                        <input 
                          type="text" 
                          value={clientForm.numero}
                          onChange={(e) => setClientForm({ ...clientForm, numero: e.target.value })}
                          className="w-full px-4 py-2 border border-slate-200 rounded-xl focus:ring-2 focus:ring-brand-orange outline-none font-bold"
                        />
                      </div>
                    </div>
                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <label className="block text-[10px] font-black text-slate-400 uppercase tracking-widest mb-1">Bairro</label>
                        <input 
                          type="text" 
                          value={clientForm.bairro}
                          onChange={(e) => setClientForm({ ...clientForm, bairro: e.target.value })}
                          className="w-full px-4 py-2 border border-slate-200 rounded-xl focus:ring-2 focus:ring-brand-orange outline-none font-bold"
                        />
                      </div>
                      <div>
                        <label className="block text-[10px] font-black text-slate-400 uppercase tracking-widest mb-1">Complemento</label>
                        <input 
                          type="text" 
                          value={clientForm.complemento}
                          onChange={(e) => setClientForm({ ...clientForm, complemento: e.target.value })}
                          className="w-full px-4 py-2 border border-slate-200 rounded-xl focus:ring-2 focus:ring-brand-orange outline-none font-bold"
                        />
                      </div>
                    </div>
                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <label className="block text-[10px] font-black text-slate-400 uppercase tracking-widest mb-1">Cidade</label>
                        <input 
                          type="text" 
                          value={clientForm.cidade}
                          onChange={(e) => setClientForm({ ...clientForm, cidade: e.target.value })}
                          className="w-full px-4 py-2 border border-slate-200 rounded-xl focus:ring-2 focus:ring-brand-orange outline-none font-bold"
                        />
                      </div>
                      <div>
                        <label className="block text-[10px] font-black text-slate-400 uppercase tracking-widest mb-1">Estado</label>
                        <input 
                          type="text" 
                          value={clientForm.estado}
                          onChange={(e) => setClientForm({ ...clientForm, estado: e.target.value })}
                          className="w-full px-4 py-2 border border-slate-200 rounded-xl focus:ring-2 focus:ring-brand-orange outline-none font-bold"
                          maxLength={2}
                        />
                      </div>
                    </div>
                    <div>
                      <label className="block text-[10px] font-black text-slate-400 uppercase tracking-widest mb-1">CEP</label>
                      <input 
                        type="text" 
                        value={clientForm.cep}
                        onChange={(e) => setClientForm({ ...clientForm, cep: e.target.value })}
                        className="w-full px-4 py-2 border border-slate-200 rounded-xl focus:ring-2 focus:ring-brand-orange outline-none font-bold"
                        placeholder="00000-000"
                      />
                    </div>
                  </div>
                </div>
                <div>
                  <label className="block text-[10px] font-black text-slate-400 uppercase tracking-widest mb-1">Observações</label>
                  <textarea 
                    value={clientForm.observacoes}
                    onChange={(e) => setClientForm({ ...clientForm, observacoes: e.target.value })}
                    className="w-full px-4 py-2 border border-slate-200 rounded-xl focus:ring-2 focus:ring-brand-orange outline-none font-bold h-20"
                  />
                </div>

                <div className="flex gap-4 pt-4">
                  <button 
                    onClick={handleSaveClient}
                    className="flex-1 py-4 bg-brand-orange text-white rounded-2xl font-black text-sm uppercase tracking-widest hover:bg-brand-orange/90 transition-all shadow-lg shadow-brand-orange/20 active:scale-95"
                  >
                    {editingClient ? 'Salvar Alterações' : 'Cadastrar Cliente'}
                  </button>
                  <button 
                    onClick={() => setIsClientModalOpen(false)}
                    className="px-8 py-4 border-2 border-slate-200 text-slate-400 rounded-2xl font-black text-sm uppercase tracking-widest hover:bg-slate-50 transition-all"
                  >
                    CANCELAR
                  </button>
                </div>
              </div>
            </motion.div>
          </div>
        )}
      </AnimatePresence>

      {/* Invoice Modal */}
      <AnimatePresence>
        {viewingInvoice && (
          <div className="fixed inset-0 bg-brand-text/60 backdrop-blur-sm flex items-center justify-center p-4 z-50">
            <motion.div 
              initial={{ scale: 0.95, opacity: 0 }}
              animate={{ scale: 1, opacity: 1 }}
              exit={{ scale: 0.95, opacity: 0 }}
              className="bg-white w-full max-w-5xl rounded-[2.5rem] shadow-2xl overflow-y-auto max-h-[90vh] border border-slate-100"
            >
              {showSuccessOrder && (
                <motion.div 
                  initial={{ height: 0, opacity: 0 }}
                  animate={{ height: 'auto', opacity: 1 }}
                  className="bg-brand-green/10 border-b-2 border-brand-green/20 p-4 flex items-center justify-center gap-3"
                >
                  <div className="bg-brand-green p-2 rounded-xl">
                    <CheckCircle2 className="text-white w-6 h-6" />
                  </div>
                  <div className="text-center">
                    <h3 className="text-lg font-black text-brand-green uppercase italic leading-none">PEDIDO FINALIZADO COM SUCESSO!</h3>
                    <p className="text-[10px] font-black text-brand-green/60 uppercase tracking-widest mt-1">O pedido foi enviado para a fila de separação.</p>
                  </div>
                </motion.div>
              )}
              <div className="bg-brand-orange p-6 flex justify-between items-center shrink-0">
                <div className="flex items-center gap-3">
                  <AtacadaoLogo className="w-10 h-10 brightness-0 invert" />
                  <div>
                    <h3 className="text-white font-black text-xl uppercase tracking-widest italic">Nota para Faturamento</h3>
                    <p className="text-white/80 text-[10px] font-bold uppercase tracking-widest">Pedido ID: {viewingInvoice.id}</p>
                  </div>
                </div>
                <button 
                  onClick={() => setViewingInvoice(null)}
                  className="p-2 hover:bg-white/10 rounded-full transition-colors text-white"
                >
                  <X size={24} />
                </button>
              </div>

              <div className="p-8 space-y-8">
                <div className="grid grid-cols-2 gap-8 border-y border-slate-100 py-8">
                  <div>
                    <p className="text-[10px] font-black text-slate-400 uppercase tracking-widest mb-2">Cliente</p>
                    <p className="font-black text-brand-text uppercase">{viewingInvoice.clientName}</p>
                    <p className="text-xs font-bold text-slate-500 mt-1">{viewingInvoice.clientCnpj}</p>
                    {viewingInvoice.nomeQuemFezPedido && (
                      <p className="text-xs font-bold text-slate-500 mt-1 italic">Pedido por: {viewingInvoice.nomeQuemFezPedido}</p>
                    )}
                  </div>
                  <div className="text-right">
                    <p className="text-[10px] font-black text-slate-400 uppercase tracking-widest mb-2">Pedido</p>
                    <p className="font-black text-brand-orange text-xl">{viewingInvoice.id}</p>
                    <div className="mt-2 flex justify-end">
                      <BarcodeGenerator value={viewingInvoice.id} className="h-12 w-auto" />
                    </div>
                    <p className="text-xs font-bold text-brand-orange uppercase mt-1 tracking-wider">{viewingInvoice.status}</p>
                    <p className="text-xs font-bold text-slate-500 mt-1">{new Date(viewingInvoice.createdAt).toLocaleString()}</p>
                  </div>
                </div>

                <div className="mb-8">
                  <div className="flex items-center gap-2 mb-4">
                    <div className="w-1 h-4 bg-brand-orange rounded-full" />
                    <h4 className="text-xs font-black text-slate-900 uppercase tracking-widest">Itens da Mercadoria</h4>
                  </div>
                  <div className="overflow-x-auto">
                    <table className="w-full text-left border-collapse">
                      <thead>
                        <tr className="bg-slate-50 border-y border-slate-200">
                          <th className="py-3 px-4 text-[10px] font-black text-slate-500 uppercase tracking-wider">EAN</th>
                          <th className="py-3 px-4 text-[10px] font-black text-slate-500 uppercase tracking-wider">Cód.</th>
                          <th className="py-3 px-4 text-[10px] font-black text-slate-500 uppercase tracking-wider">Descrição</th>
                          <th className="py-3 px-4 text-[10px] font-black text-slate-500 uppercase tracking-wider">Emb.</th>
                          <th className="py-3 px-4 text-[10px] font-black text-slate-500 uppercase tracking-wider text-center">Qtd</th>
                          <th className="py-3 px-4 text-[10px] font-black text-slate-500 uppercase tracking-wider text-right">Preço Und</th>
                          <th className="py-3 px-4 text-[10px] font-black text-slate-500 uppercase tracking-wider text-right">Preço Caixa</th>
                          <th className="py-3 px-4 text-[10px] font-black text-slate-500 uppercase tracking-wider text-right">Total</th>
                        </tr>
                      </thead>
                      <tbody>
                        {viewingInvoice.items.map((item, idx) => {
                          const currentProd = products.find(p => p.codigoInterno === item.codigoInterno || p.id === item.id);
                          const ean = currentProd?.barcodes?.[item.tipoVenda] || 
                                      currentProd?.barcodes?.unidade || 
                                      (currentProd?.barcode !== item.codigoInterno ? currentProd?.barcode : '') || 
                                      (item.barcode !== item.codigoInterno ? item.barcode : '');
                          
                          return (
                            <tr key={idx} className="border-b border-slate-100 hover:bg-slate-50/50 transition-colors">
                              <td className="py-3 px-4 text-[10px] font-mono text-slate-500">{ean || 'N/I'}</td>
                              <td className="py-3 px-4 text-[10px] font-bold text-slate-900">{item.codigoInterno}</td>
                              <td className="py-3 px-4 text-[11px] font-black text-slate-900 uppercase">{item.descricao}</td>
                              <td className="py-3 px-4 text-[10px] font-bold text-slate-500">{item.embalagem}</td>
                              <td className="py-3 px-4 text-[11px] font-black text-slate-900 text-center">{item.quantidade}</td>
                              <td className="py-3 px-4 text-[11px] font-bold text-slate-900 text-right">
                                {new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(item.precoUnitario)}
                              </td>
                              <td className="py-3 px-4 text-[11px] font-bold text-slate-900 text-right">
                                {new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(item.precoUnitario * item.multiplicador)}
                              </td>
                              <td className="py-3 px-4 text-[11px] font-black text-brand-orange text-right">
                                {new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(item.valorTotal)}
                              </td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                </div>

                {viewingInvoice.separador && (
                  <div className="bg-slate-50 p-6 rounded-2xl border-2 border-slate-100 mb-8">
                    <p className="text-[10px] font-black text-slate-400 uppercase tracking-widest mb-1">Separador Responsável</p>
                    <p className="font-black text-brand-text uppercase">{viewingInvoice.separador}</p>
                  </div>
                )}

                <div className="bg-slate-900 rounded-2xl p-6 text-white flex flex-col sm:flex-row justify-between items-center gap-4">
                  <div className="text-center sm:text-left">
                    <p className="text-[10px] font-black text-white/50 uppercase tracking-[0.2em] mb-1">Total de Itens</p>
                    <p className="text-2xl font-black">{viewingInvoice.items.reduce((acc, item) => acc + item.quantidade, 0)}</p>
                  </div>
                  <div className="text-center sm:text-right">
                    <p className="text-[10px] font-black text-brand-orange uppercase tracking-[0.2em] mb-1">Preço Total do Pedido</p>
                    <p className="text-4xl font-black text-brand-orange">
                      {new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(viewingInvoice.total || viewingInvoice.items.reduce((acc, item) => acc + item.valorTotal, 0))}
                    </p>
                  </div>
                </div>

                <div className="flex flex-col sm:flex-row gap-4">
                  <button 
                    onClick={() => generatePDF(viewingInvoice)}
                    className="flex-1 py-4 bg-brand-orange text-white rounded-2xl font-black text-sm uppercase tracking-widest flex items-center justify-center gap-2 hover:bg-brand-orange-dark transition-all active:scale-95 shadow-lg shadow-brand-orange/20"
                  >
                    <Printer size={18} />
                    Imprimir Nota (PDF)
                  </button>
                  <button 
                    onClick={() => generateSeparationPDF(viewingInvoice)}
                    className="flex-1 py-4 bg-brand-green text-white rounded-2xl font-black text-sm uppercase tracking-widest flex items-center justify-center gap-2 hover:bg-brand-green/90 transition-all active:scale-95 shadow-lg shadow-brand-green/20"
                  >
                    <Printer size={18} />
                    Imprimir Separação
                  </button>
                  <button 
                    onClick={() => setViewingInvoice(null)}
                    className="flex-1 sm:flex-none px-8 py-4 border-2 border-slate-200 text-slate-400 rounded-2xl font-black text-sm uppercase tracking-widest hover:bg-slate-50 transition-all"
                  >
                    FECHAR
                  </button>
                </div>
              </div>
              </motion.div>
            </div>
          )}
        </AnimatePresence>

      {/* Separator Modal */}
      <AnimatePresence>
        {orderToSetSeparator && (
          <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
            <motion.div 
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              onClick={() => setOrderToSetSeparator(null)}
              className="absolute inset-0 bg-slate-900/60 backdrop-blur-sm"
            />
            <motion.div 
              initial={{ opacity: 0, scale: 0.9, y: 20 }}
              animate={{ opacity: 1, scale: 1, y: 0 }}
              exit={{ opacity: 0, scale: 0.9, y: 20 }}
              className="relative bg-white w-full max-w-md rounded-[3rem] shadow-2xl overflow-hidden p-8"
            >
              <div className="text-center space-y-6">
                <div className="flex justify-center">
                  <div className="bg-brand-orange/10 p-4 rounded-3xl">
                    <Users className="text-brand-orange w-10 h-10" />
                  </div>
                </div>
                <div>
                  <h3 className="text-2xl font-black tracking-tighter uppercase italic text-brand-text">Identificar Separador</h3>
                  <p className="text-sm font-bold text-slate-500 mt-2">Quem foi o colaborador responsável pela separação deste pedido?</p>
                </div>

                <div className="space-y-4 text-left">
                  <div>
                    <label className="text-[10px] font-black uppercase tracking-widest text-slate-400 ml-1">Nome do Separador</label>
                    <input 
                      type="text"
                      value={separatorName}
                      onChange={(e) => setSeparatorName(e.target.value)}
                      placeholder="Ex: João Silva"
                      className="w-full bg-slate-50 border-2 border-slate-100 rounded-2xl px-6 py-4 text-sm font-bold focus:border-brand-orange outline-none transition-all"
                      autoFocus
                    />
                  </div>
                  <div className="flex gap-4">
                    <button 
                      onClick={() => setOrderToSetSeparator(null)}
                      className="flex-1 py-4 border-2 border-slate-100 text-slate-400 rounded-2xl font-black text-xs uppercase tracking-widest hover:bg-slate-50 transition-all"
                    >
                      Cancelar
                    </button>
                    <button 
                      onClick={handleSetSeparator}
                      disabled={!separatorName.trim()}
                      className="flex-1 py-4 bg-brand-orange text-white rounded-2xl font-black text-xs uppercase tracking-widest hover:bg-brand-orange/90 transition-all shadow-xl shadow-brand-orange/20 disabled:opacity-50 disabled:shadow-none"
                    >
                      Confirmar
                    </button>
                  </div>
                </div>
              </div>
            </motion.div>
          </div>
        )}
      </AnimatePresence>

      {/* Linking from Order Modal */}
      <AnimatePresence>
        {isLinkingFromOrder && (
          <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
            <motion.div 
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              onClick={() => setIsLinkingFromOrder(false)}
              className="absolute inset-0 bg-slate-900/60 backdrop-blur-sm"
            />
            <motion.div 
              initial={{ opacity: 0, scale: 0.95, y: 20 }}
              animate={{ opacity: 1, scale: 1, y: 0 }}
              exit={{ opacity: 0, scale: 0.95, y: 20 }}
              className="relative w-full max-w-md bg-white rounded-[2.5rem] shadow-2xl overflow-hidden border border-slate-100"
            >
              <div className="p-8 space-y-6">
                <div className="flex justify-between items-center">
                  <div className="flex items-center gap-3">
                    <div className="w-10 h-10 bg-amber-100 rounded-2xl flex items-center justify-center text-amber-600">
                      <LinkIcon size={20} />
                    </div>
                    <div>
                      <h3 className="text-lg font-black text-brand-text uppercase italic">Vincular Código</h3>
                      <p className="text-xs font-bold text-slate-500">Vincule o EAN ao produto interno.</p>
                    </div>
                  </div>
                  <button 
                    onClick={() => setIsLinkingFromOrder(false)}
                    className="p-2 hover:bg-slate-100 rounded-full text-slate-400 transition-colors"
                  >
                    <X size={20} />
                  </button>
                </div>

                <div className="p-4 bg-amber-50 rounded-2xl border border-amber-100">
                  <p className="text-[10px] font-black text-amber-600 uppercase tracking-widest mb-1">Código Lido</p>
                  <p className="text-xl font-black text-amber-900 font-mono">{scannedNotFound}</p>
                </div>

                <div className="space-y-4">
                  <p className="text-[10px] font-black text-slate-400 uppercase tracking-widest">Selecione o Produto Interno</p>
                  <div className="relative">
                    <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" size={18} />
                    <input 
                      type="text" 
                      placeholder="Buscar por código ou descrição..."
                      value={linkSearch}
                      onChange={(e) => setLinkSearch(e.target.value)}
                      className="w-full pl-10 pr-4 py-3 border border-slate-200 rounded-xl focus:ring-2 focus:ring-brand-orange outline-none font-bold text-sm"
                    />
                    
                    {linkSearch && !linkingProduct && (
                      <div className="absolute top-full left-0 right-0 bg-white border border-slate-200 rounded-xl mt-2 shadow-xl z-10 max-h-48 overflow-y-auto">
                        {products
                          .filter(p => {
                            const search = linkSearch.toLowerCase();
                            const desc = (p.descricao || p.description || '').toLowerCase();
                            const cod = (p.codigo || p.internalCode || '').toLowerCase();
                            return desc.includes(search) || cod.includes(search);
                          })
                          .slice(0, 15)
                          .map(p => (
                            <button
                              key={p.id}
                              onClick={() => {
                                setLinkingProduct(p);
                                setLinkSearch(p.descricao || p.description || '');
                              }}
                              className="w-full px-4 py-3 text-left hover:bg-slate-50 flex justify-between items-center border-b border-slate-50 last:border-0"
                            >
                              <span className="font-bold text-xs uppercase truncate">{p.descricao || p.description}</span>
                              <span className="text-[10px] font-black text-brand-orange">{p.codigo || p.internalCode}</span>
                            </button>
                          ))
                        }
                      </div>
                    )}
                  </div>

                  {linkError && (
                    <div className="p-3 bg-red-50 border border-red-200 rounded-xl text-red-600 text-xs font-bold">
                      {linkError}
                    </div>
                  )}

                  {linkSuccess && (
                    <div className="p-3 bg-green-50 border border-green-200 rounded-xl text-green-600 text-xs font-bold flex items-center gap-2">
                      <CheckCircle2 size={16} />
                      Vínculo realizado com sucesso!
                    </div>
                  )}

                  {linkingProduct && (
                    <motion.div 
                      initial={{ opacity: 0, height: 0 }}
                      animate={{ opacity: 1, height: 'auto' }}
                      className="p-4 bg-slate-50 rounded-2xl border border-slate-200"
                    >
                      <div className="flex justify-between items-center">
                        <div>
                          <p className="text-[10px] font-black text-slate-400 uppercase">Produto Selecionado</p>
                          <p className="text-sm font-black text-brand-text uppercase">{linkingProduct.descricao}</p>
                        </div>
                        <button onClick={() => setLinkingProduct(null)} className="text-slate-400 hover:text-red-500">
                          <X size={16} />
                        </button>
                      </div>
                    </motion.div>
                  )}
                </div>

                <button 
                  onClick={async () => {
                    console.log('Confirmar Vínculo clicked');
                    if (!linkingProduct || !scannedNotFound) {
                      console.log('Linking aborted: missing product or barcode', { linkingProduct, scannedNotFound });
                      setLinkError('Produto ou código não encontrado.');
                      return;
                    }
                    
                    setIsSavingLink(true);
                    setLinkError(null);
                    
                    const updatedProduct: Product = { 
                      ...linkingProduct,
                      codigoInterno: linkingProduct.codigoInterno || linkingProduct.codigo || linkingProduct.internalCode || '',
                      descricao: linkingProduct.descricao || linkingProduct.description || 'PRODUTO SEM DESCRIÇÃO',
                      embalagem: linkingProduct.embalagem || 'N/I',
                      precoUnitario: linkingProduct.precoUnitario || 0,
                      multiplicador: linkingProduct.multiplicador || 1,
                      barcodes: {
                        ...(linkingProduct.barcodes || {}),
                        unidade: scannedNotFound
                      },
                      barcode: scannedNotFound
                    };
                    console.log('Updating product (optimistic):', updatedProduct);
                    
                    // Optimistic update: update local state immediately
                    setProducts(prev => prev.map(p => p.id === updatedProduct.id ? updatedProduct : p));
                    setCurrentProduct(updatedProduct);
                    setItemPrice(updatedProduct.precoUnitario);
                    setItemMultiplier(updatedProduct.multiplicador || 1);
                    setLinkSuccess(true);
                    
                    // Call storage in background
                    storage.saveProduct(updatedProduct)
                      .then(() => {
                        console.log('Product saved successfully in background');
                      })
                      .catch((error: any) => {
                        console.error('Error saving product in background:', error);
                        // We don't revert here to keep it "instant", but in a real app we might show a retry toast
                      });

                    // Close modal quickly for "instant" feel
                    setTimeout(() => {
                      setBarcodeInput(scannedNotFound);
                      setScannedNotFound(null);
                      setIsLinkingFromOrder(false);
                      setLinkingProduct(null);
                      setLinkSearch('');
                      setLinkSuccess(false);
                      setIsSavingLink(false);
                      console.log('State reset after optimistic linking');
                    }, 800);
                  } }
                  disabled={!linkingProduct || isSavingLink}
                  className="w-full bg-brand-orange text-white py-4 rounded-2xl font-black uppercase tracking-widest text-xs hover:bg-brand-orange/90 transition-all disabled:opacity-50 disabled:cursor-not-allowed shadow-lg shadow-brand-orange/20 flex items-center justify-center gap-2"
                >
                  {isSavingLink ? (
                    <>
                      <Loader2 className="animate-spin" size={16} />
                      Salvando...
                    </>
                  ) : (
                    'Confirmar Vínculo'
                  )}
                </button>
              </div>
            </motion.div>
          </div>
        )}
      </AnimatePresence>
      {/* Product Modal */}
      <AnimatePresence>
        {isProductModalOpen && (
          <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
            <motion.div 
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              onClick={() => setIsProductModalOpen(false)}
              className="absolute inset-0 bg-slate-900/60 backdrop-blur-sm"
            />
            <motion.div 
              initial={{ opacity: 0, scale: 0.95, y: 20 }}
              animate={{ opacity: 1, scale: 1, y: 0 }}
              exit={{ opacity: 0, scale: 0.95, y: 20 }}
              className="relative w-full max-w-2xl bg-white rounded-[2.5rem] shadow-2xl overflow-hidden border border-slate-100"
            >
              <form onSubmit={handleSaveProduct} className="p-8 space-y-6 max-h-[90vh] overflow-y-auto">
                <div className="flex justify-between items-center">
                  <div className="flex items-center gap-3">
                    <div className="w-10 h-10 bg-brand-green/10 rounded-2xl flex items-center justify-center text-brand-green">
                      <Package size={20} />
                    </div>
                    <div>
                      <h3 className="text-lg font-black text-brand-text uppercase italic">
                        {editingProduct ? 'Editar Produto' : 'Novo Produto'}
                      </h3>
                      <p className="text-xs font-bold text-slate-500">Cadastre ou edite as informações do produto.</p>
                    </div>
                  </div>
                  <button 
                    type="button"
                    onClick={() => setIsProductModalOpen(false)}
                    className="p-2 hover:bg-slate-100 rounded-full text-slate-400 transition-colors"
                  >
                    <X size={20} />
                  </button>
                </div>

                <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                  <div className="space-y-4">
                    <div>
                      <label className="text-[10px] font-black uppercase tracking-widest text-slate-400 ml-1">Código Interno *</label>
                      <input 
                        type="text"
                        required
                        value={productForm.codigoInterno || ''}
                        onChange={(e) => setProductForm({ ...productForm, codigoInterno: e.target.value })}
                        className="w-full bg-slate-50 border-2 border-slate-100 rounded-2xl px-6 py-4 text-sm font-bold focus:border-brand-orange outline-none transition-all"
                        placeholder="Ex: 68399"
                      />
                    </div>
                    <div>
                      <label className="text-[10px] font-black uppercase tracking-widest text-slate-400 ml-1">Descrição *</label>
                      <input 
                        type="text"
                        required
                        value={productForm.descricao || ''}
                        onChange={(e) => setProductForm({ ...productForm, descricao: e.target.value })}
                        className="w-full bg-slate-50 border-2 border-slate-100 rounded-2xl px-6 py-4 text-sm font-bold focus:border-brand-orange outline-none transition-all"
                        placeholder="Ex: MOLHO PIMENTA CAMPILAR"
                      />
                    </div>
                    <div>
                      <label className="text-[10px] font-black uppercase tracking-widest text-slate-400 ml-1">Embalagem</label>
                      <input 
                        type="text"
                        value={productForm.embalagem || ''}
                        onChange={(e) => setProductForm({ ...productForm, embalagem: e.target.value })}
                        className="w-full bg-slate-50 border-2 border-slate-100 rounded-2xl px-6 py-4 text-sm font-bold focus:border-brand-orange outline-none transition-all"
                        placeholder="Ex: CXA 1 X 12 X 150ML"
                      />
                    </div>
                  </div>

                  <div className="space-y-4">
                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <label className="text-[10px] font-black uppercase tracking-widest text-slate-400 ml-1">Preço Unitário</label>
                        <input 
                          type="text"
                          value={new Intl.NumberFormat('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(productForm.precoUnitario || 0)}
                          onChange={(e) => {
                            const rawValue = e.target.value.replace(/\D/g, '');
                            const numericValue = Number(rawValue) / 100;
                            setProductForm({ ...productForm, precoUnitario: numericValue });
                          }}
                          className="w-full bg-slate-50 border-2 border-slate-100 rounded-2xl px-6 py-4 text-sm font-bold focus:border-brand-orange outline-none transition-all"
                        />
                      </div>
                      <div>
                        <label className="text-[10px] font-black uppercase tracking-widest text-slate-400 ml-1">Multiplicador</label>
                        <input 
                          type="number"
                          value={productForm.multiplicador || 1}
                          onChange={(e) => setProductForm({ ...productForm, multiplicador: Number(e.target.value) })}
                          className="w-full bg-slate-50 border-2 border-slate-100 rounded-2xl px-6 py-4 text-sm font-bold focus:border-brand-orange outline-none transition-all"
                        />
                      </div>
                    </div>
                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <label className="text-[10px] font-black uppercase tracking-widest text-slate-400 ml-1">EAN Unidade</label>
                        <input 
                          type="text"
                          value={productForm.barcodes?.unidade || ''}
                          onChange={(e) => setProductForm({ 
                            ...productForm, 
                            barcodes: { ...productForm.barcodes, unidade: e.target.value } 
                          })}
                          className="w-full bg-slate-50 border-2 border-slate-100 rounded-2xl px-6 py-4 text-sm font-bold focus:border-brand-orange outline-none transition-all"
                          placeholder="EAN Unidade"
                        />
                      </div>
                      <div>
                        <label className="text-[10px] font-black uppercase tracking-widest text-slate-400 ml-1">EAN Caixa</label>
                        <input 
                          type="text"
                          value={productForm.barcodes?.caixa || ''}
                          onChange={(e) => setProductForm({ 
                            ...productForm, 
                            barcodes: { ...productForm.barcodes, caixa: e.target.value } 
                          })}
                          className="w-full bg-slate-50 border-2 border-slate-100 rounded-2xl px-6 py-4 text-sm font-bold focus:border-brand-orange outline-none transition-all"
                          placeholder="EAN Caixa"
                        />
                      </div>
                    </div>
                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <label className="text-[10px] font-black uppercase tracking-widest text-slate-400 ml-1">Estoque</label>
                        <input 
                          type="number"
                          value={productForm.stock || 0}
                          onChange={(e) => setProductForm({ ...productForm, stock: Number(e.target.value) })}
                          className="w-full bg-slate-50 border-2 border-slate-100 rounded-2xl px-6 py-4 text-sm font-bold focus:border-brand-orange outline-none transition-all"
                        />
                      </div>
                      <div>
                        <label className="text-[10px] font-black uppercase tracking-widest text-slate-400 ml-1">Venda Diária</label>
                        <input 
                          type="number"
                          value={productForm.dailySales || 0}
                          onChange={(e) => setProductForm({ ...productForm, dailySales: Number(e.target.value) })}
                          className="w-full bg-slate-50 border-2 border-slate-100 rounded-2xl px-6 py-4 text-sm font-bold focus:border-brand-orange outline-none transition-all"
                        />
                      </div>
                    </div>
                  </div>
                </div>

                <div className="flex gap-4 pt-4">
                  <button 
                    type="button"
                    onClick={() => setIsProductModalOpen(false)}
                    className="flex-1 py-4 border-2 border-slate-100 text-slate-400 rounded-2xl font-black text-xs uppercase tracking-widest hover:bg-slate-50 transition-all"
                  >
                    Cancelar
                  </button>
                  <button 
                    type="submit"
                    className="flex-1 py-4 bg-brand-green text-white rounded-2xl font-black text-xs uppercase tracking-widest hover:bg-brand-green/90 transition-all shadow-xl shadow-brand-green/20"
                  >
                    Salvar Produto
                  </button>
                </div>
              </form>
            </motion.div>
          </div>
        )}
      </AnimatePresence>

      {/* Separation Modal */}
      <AnimatePresence>
        {isSeparationModalOpen && separatingOrder && (
          <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
            <motion.div 
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              onClick={() => setIsSeparationModalOpen(false)}
              className="absolute inset-0 bg-slate-900/60 backdrop-blur-sm"
            />
            <motion.div 
              initial={{ opacity: 0, scale: 0.95, y: 20 }}
              animate={{ opacity: 1, scale: 1, y: 0 }}
              exit={{ opacity: 0, scale: 0.95, y: 20 }}
              className="relative w-full max-w-4xl bg-white rounded-[2.5rem] shadow-2xl overflow-hidden border border-slate-100"
            >
              <div className="p-8 space-y-6 max-h-[90vh] overflow-y-auto">
                <div className="flex justify-between items-center">
                  <div className="flex items-center gap-3">
                    <div className="w-10 h-10 bg-brand-orange/10 rounded-2xl flex items-center justify-center text-brand-orange">
                      <PackageSearch size={20} />
                    </div>
                    <div>
                      <h3 className="text-lg font-black text-brand-text uppercase italic">
                        Separação de Pedido
                      </h3>
                      <p className="text-xs font-bold text-slate-500">Confira os itens para separação.</p>
                    </div>
                  </div>
                  <button 
                    onClick={() => setIsSeparationModalOpen(false)}
                    className="p-2 hover:bg-slate-100 rounded-full text-slate-400 transition-colors"
                  >
                    <X size={20} />
                  </button>
                </div>

                <div className="bg-slate-50 p-6 rounded-3xl border border-slate-100 space-y-4">
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                    <div>
                      <p className="text-[10px] font-black text-slate-400 uppercase tracking-widest">Pedido</p>
                      <p className="text-sm font-black text-brand-text">{separatingOrder.id}</p>
                    </div>
                    <div>
                      <p className="text-[10px] font-black text-slate-400 uppercase tracking-widest">Data</p>
                      <p className="text-sm font-black text-brand-text">{new Date(separatingOrder.createdAt).toLocaleDateString()}</p>
                    </div>
                    <div className="col-span-2">
                      <p className="text-[10px] font-black text-slate-400 uppercase tracking-widest">Cliente</p>
                      <p className="text-sm font-black text-brand-text uppercase">{separatingOrder.clientName}</p>
                    </div>
                  </div>
                </div>

                <div className="border-2 border-slate-100 rounded-3xl overflow-hidden">
                  <table className="w-full text-left border-collapse">
                    <thead>
                      <tr className="bg-slate-50 border-b-2 border-slate-100">
                        <th className="px-6 py-4 text-[10px] font-black text-slate-400 uppercase tracking-widest">Cód.</th>
                        <th className="px-6 py-4 text-[10px] font-black text-slate-400 uppercase tracking-widest">Descrição</th>
                        <th className="px-6 py-4 text-[10px] font-black text-slate-400 uppercase tracking-widest">EAN</th>
                        <th className="px-6 py-4 text-[10px] font-black text-slate-400 uppercase tracking-widest">Embalagem</th>
                        <th className="px-6 py-4 text-[10px] font-black text-slate-400 uppercase tracking-widest text-center">Qtd</th>
                      </tr>
                    </thead>
                    <tbody>
                      {separatingOrder.items.map((item, idx) => (
                        <tr key={idx} className="border-b border-slate-50 hover:bg-slate-50/50 transition-colors">
                          <td className="px-6 py-4 text-xs font-black text-brand-text">{item.codigoInterno}</td>
                          <td className="px-6 py-4 text-xs font-bold text-slate-600 uppercase">{item.descricao}</td>
                          <td className="px-6 py-4 text-xs font-mono text-slate-400">{item.barcode}</td>
                          <td className="px-6 py-4 text-xs font-bold text-slate-500 uppercase">{item.embalagem}</td>
                          <td className="px-6 py-4 text-xs font-black text-brand-orange text-center">{item.quantidade}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>

                <div className="flex gap-4 pt-4">
                  <button 
                    onClick={() => setIsSeparationModalOpen(false)}
                    className="flex-1 py-4 border-2 border-slate-100 text-slate-400 rounded-2xl font-black text-xs uppercase tracking-widest hover:bg-slate-50 transition-all"
                  >
                    Fechar
                  </button>
                  <button 
                    onClick={() => generateSeparationPDF(separatingOrder)}
                    className="flex-1 py-4 bg-brand-orange text-white rounded-2xl font-black text-xs uppercase tracking-widest hover:bg-brand-orange/90 transition-all shadow-xl shadow-brand-orange/20 flex items-center justify-center gap-2"
                  >
                    <Printer size={16} />
                    Imprimir Guia de Separação
                  </button>
                  <button 
                    onClick={() => {
                      advanceStatus(separatingOrder.id);
                      setIsSeparationModalOpen(false);
                    }}
                    className="flex-1 py-4 bg-brand-green text-white rounded-2xl font-black text-xs uppercase tracking-widest hover:bg-brand-green/90 transition-all shadow-xl shadow-brand-green/20 flex items-center justify-center gap-2"
                  >
                    <CheckCircle2 size={16} />
                    Finalizar Separação
                  </button>
                </div>
              </div>
            </motion.div>
          </div>
        )}
      </AnimatePresence>

      {/* Operator Modal */}
      <AnimatePresence>
        {isOperatorModalOpen && (
          <div className="fixed inset-0 bg-brand-text/60 backdrop-blur-sm flex items-center justify-center p-4 z-50">
            <motion.div 
              initial={{ scale: 0.95, opacity: 0 }}
              animate={{ scale: 1, opacity: 1 }}
              exit={{ scale: 0.95, opacity: 0 }}
              className="bg-white w-full max-w-md rounded-[2.5rem] shadow-2xl overflow-hidden border border-slate-100"
            >
              <div className="p-8 space-y-6">
                <div className="flex justify-between items-center">
                  <div className="flex items-center gap-3">
                    <div className="bg-brand-orange p-2 rounded-xl">
                      <UserCheck className="text-white w-6 h-6" />
                    </div>
                    <div>
                      <h2 className="text-xl font-black text-brand-text tracking-tighter uppercase italic">
                        {editingOperator ? 'Editar Operador' : 'Novo Operador'}
                      </h2>
                      <p className="text-[10px] font-black text-brand-orange uppercase tracking-[0.3em]">Cadastro de Equipe</p>
                    </div>
                  </div>
                  <button onClick={() => setIsOperatorModalOpen(false)} className="p-2 hover:bg-slate-100 rounded-full transition-colors">
                    <X size={24} className="text-slate-400" />
                  </button>
                </div>

                <div className="space-y-4">
                  <div>
                    <label className="block text-[10px] font-black text-slate-400 uppercase tracking-widest mb-1">Nome Completo</label>
                    <input 
                      type="text" 
                      value={operatorForm.nome || ''}
                      onChange={(e) => setOperatorForm({ ...operatorForm, nome: e.target.value })}
                      className="w-full px-4 py-3 border border-slate-200 rounded-xl focus:ring-2 focus:ring-brand-orange outline-none font-bold"
                      placeholder=""
                    />
                  </div>
                  <div>
                    <label className="block text-[10px] font-black text-slate-400 uppercase tracking-widest mb-1">Matrícula</label>
                    <input 
                      type="text" 
                      value={operatorForm.matricula || ''}
                      onChange={(e) => setOperatorForm({ ...operatorForm, matricula: e.target.value })}
                      className="w-full px-4 py-3 border border-slate-200 rounded-xl focus:ring-2 focus:ring-brand-orange outline-none font-bold"
                      placeholder=""
                    />
                  </div>
                </div>

                <div className="flex gap-3 pt-4">
                  <button 
                    onClick={() => setIsOperatorModalOpen(false)}
                    className="flex-1 py-4 rounded-2xl font-black uppercase tracking-widest text-xs text-slate-400 hover:bg-slate-50 transition-all"
                  >
                    Cancelar
                  </button>
                  <button 
                    onClick={() => checkPassword(handleSaveOperator)}
                    disabled={isSavingOperator}
                    className="flex-1 bg-brand-orange text-white py-4 rounded-2xl font-black uppercase tracking-widest text-xs hover:bg-brand-orange/90 transition-all shadow-lg shadow-brand-orange/20 disabled:opacity-50 disabled:cursor-not-allowed flex items-center justify-center gap-2"
                  >
                    {isSavingOperator && <Loader2 className="animate-spin w-4 h-4" />}
                    {editingOperator ? 'Atualizar' : 'Cadastrar'}
                  </button>
                </div>
              </div>
            </motion.div>
          </div>
        )}
        {isPasswordModalOpen && (
          <div className="fixed inset-0 bg-slate-900/60 backdrop-blur-sm flex items-center justify-center z-[100] p-4">
            <motion.div
              initial={{ opacity: 0, scale: 0.95, y: 20 }}
              animate={{ opacity: 1, scale: 1, y: 0 }}
              className="bg-white rounded-[2.5rem] shadow-2xl w-full max-w-md overflow-hidden border border-slate-100"
            >
              <div className="p-8 border-b border-slate-50 flex justify-between items-center bg-slate-50/50">
                <div className="flex items-center gap-3">
                  <div className="bg-slate-900 p-2 rounded-xl">
                    <Lock className="text-white w-5 h-5" />
                  </div>
                  <div>
                    <h3 className="text-lg font-black text-slate-900 tracking-tighter uppercase italic">
                      Acesso Restrito
                    </h3>
                    <p className="text-[10px] font-black text-slate-400 uppercase tracking-[0.3em]">Segurança do Sistema</p>
                  </div>
                </div>
                <button
                  onClick={() => setIsPasswordModalOpen(false)}
                  className="p-2 hover:bg-slate-200 rounded-full transition-colors"
                >
                  <X className="w-5 h-5 text-slate-400" />
                </button>
              </div>
              <form onSubmit={handlePasswordSubmit} className="p-8 space-y-6">
                <p className="text-xs font-bold text-slate-500 leading-relaxed">
                  Esta é uma ação administrativa protegida. Por favor, insira a senha de acesso para continuar.
                </p>
                <div className="space-y-2">
                  <label className="block text-[10px] font-black text-slate-400 uppercase tracking-widest">
                    Senha de Administrador
                  </label>
                  <input
                    type="password"
                    autoFocus
                    value={passwordInput}
                    onChange={(e) => setPasswordInput(e.target.value)}
                    className="w-full px-5 py-4 bg-slate-50 border border-slate-200 rounded-2xl focus:ring-2 focus:ring-slate-900 outline-none font-bold text-slate-900 placeholder:text-slate-300 transition-all"
                    placeholder="••••••••••••"
                  />
                </div>
                <div className="flex gap-3 pt-2">
                  <button
                    type="button"
                    onClick={() => setIsPasswordModalOpen(false)}
                    className="flex-1 py-4 rounded-2xl font-black uppercase tracking-widest text-xs text-slate-400 hover:bg-slate-50 transition-all"
                  >
                    Cancelar
                  </button>
                  <button
                    type="submit"
                    className="flex-1 bg-slate-900 text-white py-4 rounded-2xl font-black uppercase tracking-widest text-xs hover:bg-slate-800 transition-all shadow-lg shadow-slate-900/20"
                  >
                    Confirmar
                  </button>
                </div>
              </form>
            </motion.div>
          </div>
        )}
      </AnimatePresence>
    </div>
  );
}
