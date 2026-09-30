export interface CustomerEvent {
  customerId: string;
  orderId: string;
  /** Müşteri bazında artan sıra numarası */
  seq: number;
  type: string;
}
