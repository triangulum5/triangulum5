{{ config(materialized='view') }}

SELECT
  oi.item_id AS item_id,
  oi.order_id AS order_id,
  COALESCE(child.product_id,
    oi.product_id) AS product_id,
  type.value AS type,
  COALESCE(child.sku,
    oi.sku) AS sku,
  COALESCE(child.name,
    oi.name) AS name,
  color.value AS color,
  size.value AS size,
  oi.base_original_price AS price,
  -ABS(oi.base_discount_amount) AS discount,
  oi.qty_ordered AS qty_ordered,
  oi.qty_shipped AS qty_shipped,
  oi.qty_invoiced AS qty_invoiced,
  oi.qty_refunded AS qty_refunded,
  oi.qty_canceled AS qty_canceled,
  o.order_number AS order_number,
  o.created_at AS created_at,
  o.status AS status,
  o.order_type AS order_type,
  o.master_order_type AS master_order_type,
  o.cost_group AS cost_group,
  o.customer_email AS customer_email,
  o.book_of_business AS book_of_business,
  COALESCE(i.gross,
    0) AS gross-- , -COALESCE(i.amount_refunded, 0) AS gross_refunded, -COALESCE(ABS(i.discount_amount), 0) AS discount, COALESCE(i.discount_refunded, 0) AS discount_refunded
FROM
  `warehouse-152021.website.mage_sales_flat_order_item` AS oi
LEFT JOIN (
  SELECT
    i.parent_item_id,
    i.product_id,
    i.sku,
    i.name
  FROM
    `warehouse-152021.website.mage_sales_flat_order_item` AS i ) AS child
ON
  child.parent_item_id = oi.item_id
LEFT JOIN (
  SELECT
    p.entity_id,
    v.value
  FROM
    `warehouse-152021.website.mage_catalog_product_entity` AS p
  LEFT JOIN
    `warehouse-152021.website.mage_catalog_product_entity_int` AS i
  ON
    p.entity_id = i.entity_id
  LEFT JOIN
    `warehouse-152021.website.mage_eav_attribute_option_value` AS v
  ON
    i.value = v.option_id
  WHERE
    i.attribute_id = 92 ) AS color
ON
  COALESCE(child.product_id,
    oi.product_id) = color.entity_id
LEFT JOIN (
  SELECT
    p.entity_id,
    t.value
  FROM
    `warehouse-152021.website.mage_catalog_product_entity` AS p
  LEFT JOIN
    `warehouse-152021.website.mage_catalog_product_entity_varchar` AS t
  ON
    t.entity_id = p.entity_id
    AND t.attribute_id = 181 ) AS type
ON
  COALESCE(child.product_id,
    oi.product_id) = type.entity_id
LEFT JOIN (
  SELECT
    p.entity_id,
    v.value
  FROM
    `warehouse-152021.website.mage_catalog_product_entity` AS p
  LEFT JOIN
    `warehouse-152021.website.mage_catalog_product_entity_int` AS i
  ON
    p.entity_id = i.entity_id
  LEFT JOIN
    `warehouse-152021.website.mage_eav_attribute_option_value` AS v
  ON
    i.value = v.option_id
  WHERE
    i.attribute_id = 135 ) AS size
ON
  COALESCE(child.product_id,
    oi.product_id) = size.entity_id
JOIN
  {{ref('dbt_order')}} AS o
ON
  o.order_id = oi.order_id
JOIN
  {{ref('dbt_item')}} AS i
ON
  i.item_id = oi.item_id
LEFT JOIN
  `warehouse-152021.website.mage_sales_flat_creditmemo_item` AS ci
ON
  ci.order_item_id = oi.item_id
WHERE
  oi.parent_item_id IS NULL
