import { Client, Product } from './types';

export const INITIAL_CLIENTS: Client[] = [
  { id: '1', cnpj: '12.345.678/0001-99', razaoSocial: 'MERCADO CENTRAL LTDA', nomeFantasia: 'MERCADO CENTRAL', inscricaoEstadual: '', telefone: '', email: '', contato: '', cep: '', endereco: '', numero: '', bairro: '', cidade: '', estado: '', complemento: '', observacoes: '', name: 'MERCADO CENTRAL LTDA' },
  { id: '2', cnpj: '98.765.432/0001-55', razaoSocial: 'SUPERMERCADO BOM PREÇO', nomeFantasia: 'BOM PREÇO', inscricaoEstadual: '', telefone: '', email: '', contato: '', cep: '', endereco: '', numero: '', bairro: '', cidade: '', estado: '', complemento: '', observacoes: '', name: 'SUPERMERCADO BOM PREÇO' },
  { id: '3', cnpj: '11.222.333/0001-44', razaoSocial: 'ATACADISTA SÃO JOÃO', nomeFantasia: 'SÃO JOÃO', inscricaoEstadual: '', telefone: '', email: '', contato: '', cep: '', endereco: '', numero: '', bairro: '', cidade: '', estado: '', complemento: '', observacoes: '', name: 'ATACADISTA SÃO JOÃO' },
];

export const INITIAL_PRODUCTS: Product[] = [
  {
    id: '1',
    codigoInterno: '1001',
    descricao: 'ARROZ 5KG',
    embalagem: 'FDO 1 X 6 X 5KG',
    barcodes: {
      unidade: '7891000100103',
      fardo: '7891000100104',
    },
    precoUnitario: 25.50,
    multiplicador: 6,
    completo: 'FDO 1 X 6 X 5KG',
    stock: 120,
    dailySales: 35,
  },
  {
    id: '2',
    codigoInterno: '1002',
    descricao: 'FEIJÃO 1KG',
    embalagem: 'FDO 1 X 10 X 1KG',
    barcodes: {
      unidade: '7891000100203',
      fardo: '7891000100204',
    },
    precoUnitario: 8.90,
    multiplicador: 10,
    completo: 'FDO 1 X 10 X 1KG',
    stock: 80,
    dailySales: 22,
  },
  {
    id: '3',
    codigoInterno: '1003',
    descricao: 'AÇÚCAR 1KG',
    embalagem: 'FDO 1 X 10 X 1KG',
    barcodes: {
      unidade: '7891000100303',
      fardo: '7891000100304',
    },
    precoUnitario: 4.50,
    multiplicador: 10,
    completo: 'FDO 1 X 10 X 1KG',
    stock: 200,
    dailySales: 50,
  },
  {
    id: '4',
    codigoInterno: '1004',
    descricao: 'DETERGENTE 2L',
    embalagem: 'CXA 1 X 6 X 2L',
    barcodes: {
      unidade: '7891000100403',
      caixa: '7891000100404',
    },
    precoUnitario: 12.30,
    multiplicador: 6,
    completo: 'CXA 1 X 6 X 2L',
    stock: 150,
    dailySales: 40,
  },
];

export const ORDER_STATUSES: string[] = [
  'Separação',
  'Pedido Separado',
  'Pedido Faturado',
  'Pedido Aguardando Retirada',
  'Pedido Retirado',
];
