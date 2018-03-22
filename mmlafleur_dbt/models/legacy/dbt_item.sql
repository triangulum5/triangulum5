{{ config(materialized='view') }}

SELECT
  i.item_id AS item_id,
  i.order_id AS order_id,
  i.product_id AS product_id,
  i.name AS name,
  i.sku AS sku,
  i.qty_invoiced AS qty_invoiced,
  i.qty_ordered AS qty_ordered,
  i.qty_canceled AS qty_canceled,
  i.qty_shipped AS qty_shipped,
  i.qty_refunded AS qty_refunded,
  i.original_price AS price,
  IF ( o.status != 'canceled'
    AND i.sku NOT IN ('GIFT-CARD',
      'GIFT-BENTO'), IF ( o.mmlf_order_type IN ('Bento 1',
        'Bento 2+',
        'Dresses on Demand',
        'Dressing Room', 'Shopping Bag+', 'At Home'), IF ( o.status IN ('closed',
          'complete')
        AND i.qty_invoiced > 0, (i.original_price * i.qty_invoiced), 0 ), (i.original_price * i.qty_ordered) ), 0 ) AS gross,
  IF ( o.status != 'canceled'
    AND i.sku IN ('GIFT-CARD',
      'GIFT-BENTO'), IF ( o.mmlf_order_type IN ('Bento 1',
        'Bento 2+',
        'Dresses on Demand',
        'Dressing Room', 'Shopping Bag+', 'At Home'), IF ( o.status IN ('closed',
          'complete')
        AND i.qty_invoiced > 0, (i.original_price * i.qty_invoiced), 0 ), (i.original_price * i.qty_ordered) ), 0 ) AS gift_card_gross,
  IF ( o.status != 'canceled'
    AND i.sku NOT IN ('GIFT-CARD',
      'GIFT-BENTO'), IF ( o.mmlf_order_type IN ('Bento 1',
        'Bento 2+',
        'Dresses on Demand',
        'Dressing Room', 'Shopping Bag+', 'At Home')
      AND i.qty_invoiced = 0, 0, i.discount_amount + ((i.original_price - i.price) * i.qty_ordered) ), 0 ) AS discount_amount,
  IF ( o.status != 'canceled'
    AND i.sku IN ('GIFT-CARD',
      'GIFT-BENTO'), IF ( o.mmlf_order_type IN ('Bento 1',
        'Bento 2+',
        'Dresses on Demand',
        'Dressing Room', 'Shopping Bag+', 'At Home')
      AND i.qty_invoiced = 0, 0, i.discount_amount + ((i.original_price - i.price) * i.qty_ordered) ), 0 ) AS gift_card_discount_amount,
  IF ( i.sku NOT IN ('GIFT-CARD',
      'GIFT-BENTO'), i.amount_refunded, 0 ) AS amount_refunded,
  IF ( i.sku IN ('GIFT-CARD',
      'GIFT-BENTO'), i.amount_refunded, 0 ) AS gift_card_amount_refunded,
  IF ( i.sku NOT IN ('GIFT-CARD',
      'GIFT-BENTO'), i.discount_refunded, 0 ) AS discount_refunded,
  IF ( i.sku IN ('GIFT-CARD',
      'GIFT-BENTO'), i.discount_refunded, 0 ) AS gift_card_discount_refunded
FROM
  `warehouse-152021.website.mage_sales_flat_order_item` AS i
LEFT JOIN
  `warehouse-152021.website.mage_sales_flat_order` AS o
ON
  o.entity_id = i.order_id
WHERE
  i.parent_item_id IS NULL
  AND i.sku NOT IN ('DRESSING-ROOM-AUTH')
