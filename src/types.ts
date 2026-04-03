export interface Client {
  id: string;
  razaoSocial: string;
  nomeFantasia: string;
  cnpj: string;
  inscricaoEstadual: string;
  telefone: string;
  email: string;
  contato: string;
  cep: string;
  endereco: string;
  numero: string;
  bairro: string;
  cidade: string;
  estado: string;
  complemento: string;
  observacoes: string;
  // For backward compatibility
  name?: string;
}

export type BarcodeType = 'unidade' | 'caixa' | 'fardo' | 'kg';

export interface Product {
  id: string;
  codigoInterno: string;
  descricao: string;
  embalagem: string; // e.g., "CXA 1 X 6 X 2L"
  barcodes: {
    unidade?: string;
    caixa?: string;
    fardo?: string;
    kg?: string;
  };
  precoUnitario: number;
  multiplicador: number; // extracted from embalagem, e.g., 6
  completo?: string;
  // For backward compatibility
  codigo?: string;
  barcode?: string;
  internalCode?: string;
  description?: string;
  stock?: number;
  dailySales?: number;
}

export interface OrderItem {
  id: string;
  codigoInterno: string;
  descricao: string;
  embalagem: string;
  barcode: string; // the one used for this sale
  quantidade: number; // quantity of packages (e.g., 2 boxes)
  tipoVenda: BarcodeType;
  precoUnitario: number; // price per unit
  multiplicador: number; // units per package
  valorTotal: number; // quantidade * multiplicador * precoUnitario
  completo?: string;
  // For backward compatibility
  codigo?: string;
  unidadeMedida?: 'CX' | 'UN';
  productId?: string;
  description?: string;
  quantity?: number;
}

export type OrderStatus = 'Separação' | 'Pedido Separado' | 'Pedido Faturado' | 'Pedido Aguardando Retirada' | 'Pedido Retirado';

export interface Order {
  id: string;
  numero: string;
  data: string;
  clientCnpj: string;
  clientName: string;
  items: OrderItem[];
  status: OrderStatus;
  createdAt: string;
  nomeQuemFezPedido: string;
  total: number;
  separador?: string;
  archivedAt?: string;
}

export interface Operator {
  id: string;
  nome: string;
  matricula: string;
  createdAt: string;
}
