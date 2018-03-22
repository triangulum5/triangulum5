--{{ config(materialized='view') }}

WITH
  gift_card_discount_list AS (
  SELECT
    DISTINCT(giftcard_id) AS giftcard_id
  FROM
    `warehouse-152021.website.mage_aw_giftcard_history`
  WHERE
    action = 1
    AND REGEXP_CONTAINS(additional_info, '[0-9]+";}$')),
  credit_discount_list AS (
  SELECT
    DISTINCT(giftcard_id) AS giftcard_id
  FROM
    `warehouse-152021.website.mage_aw_giftcard_history`
  WHERE
    action = 1
    AND NOT REGEXP_CONTAINS(additional_info, '[0-9]+";}$'))
SELECT
  o.entity_id AS order_id,
  o.increment_id AS order_number,
  o.store_id AS store_id,
  DATE(o.created_at, "America/New_York") AS created_at,
  o.created_at AS created_at_timestamp,
  DATE_TRUNC(DATE(o.created_at, 'America/New_York'), YEAR) AS created_at_calendar_year_start,
  DATE_TRUNC(DATE(o.created_at, 'America/New_York'), MONTH) AS created_at_calendar_month_start,
  DATE_TRUNC(DATE(o.created_at, 'America/New_York'), WEEK) AS created_at_calendar_week_start,
  cal.year AS created_at_retail_year,
  cal.week_of_year AS created_at_retail_week_of_year,
  cal.month AS created_at_retail_month,
  cal.month_name AS created_at_retail_month_name,
  cal.week_start_date AS created_at_retail_week_start_date,
  cal.week_end_date AS created_at_retail_week_end_date,
  cal.quarter AS created_at_retail_quarter,
  cal.day_of_week AS created_at_retail_day_of_week,
  o.status AS status,
  location.name AS store_location,
  o.mmlf_order_type AS order_type,
  CASE
    WHEN o.mmlf_order_type IN ('Bento 1',  'Bento 2+',  'Dressing Room',  'Online Takeaway', 'Shopping Bag+', 'At Home') THEN 'Bento'
    WHEN o.mmlf_order_type IN ('Shopping Bag',
    'Shopping Bag via Stylist',
    'Dresses on Demand',
    'Waitlist') THEN 'Shopping Bag'
    WHEN o.mmlf_order_type IN ('Offline To Ship', 'Offline to Ship',  'Offline Takeaway',  'Home/Work - Ship',  'Home/Work - Takeaway') THEN 'Offline'
    ELSE 'Other'
  END AS master_order_type,
  o.mmlf_cost_group AS cost_group,
  book.value AS book_of_business,
  o.mmlf_stylist AS stylist,
  o.mmlf_handler AS handler,
  o.mmlf_special_offer AS special_offer,
  o.coupon_code AS coupon_code,
  oi.gross AS gross,
  oi.gross_refunded AS gross_refunded,
  oi.discount AS discount,
  oi.discount_refunded AS discount_refunded,
  COALESCE(-credit_memo.adjustment,
    0) AS credit_memo_adjustment,
  oi.gross + oi.gross_refunded + oi.discount + oi.discount_refunded - COALESCE(credit_memo.adjustment,
    0) AS net,
  oi.gift_card_gross AS gift_card_gross,
  oi.gift_card_gross_refunded AS gift_card_gross_refunded,
  oi.gift_card_discount AS gift_card_discount,
  oi.gift_card_discount_refunded AS gift_card_discount_refunded,
  oi.gift_card_gross + oi.gift_card_gross_refunded + oi.gift_card_discount + oi.gift_card_discount_refunded AS gift_card_net,
  IF(o.status != 'canceled',
    COALESCE(-ABS(credit_discount.amount),
      0),
    0) AS credit_use,
  COALESCE(credit_discount_refunded.amount,
    0) AS credit_use_refunded,
  IF(o.status != 'canceled',
    COALESCE(-ABS(gift_card_discount.amount),
      0),
    0) AS gift_card_use,
  COALESCE(gift_card_discount_refunded.amount,
    0) AS gift_card_use_refunded,
  IF(o.status != 'canceled',
    COALESCE(o.shipping_amount,
      0),
    0) AS shipping,
  COALESCE(-ABS(o.shipping_refunded),
    0) AS shipping_refunded,
  IF(o.status != 'canceled',
    COALESCE(o.shipping_amount,
      0),
    0) - COALESCE(ABS(o.shipping_refunded),
    0) AS shipping_net,
  IF(o.status != 'canceled',
    COALESCE(o.tax_amount,
      0),
    0) AS tax,
  COALESCE(-ABS(o.tax_refunded),
    0) AS tax_refunded,
  IF(o.status != 'canceled',
    COALESCE(o.tax_amount,
      0),
    0) - COALESCE(ABS(o.tax_refunded),
    0) AS tax_net,
  o.customer_id AS customer_id,
  csd.customer_created_at AS customer_created_at,
  DATE_TRUNC(csd.customer_created_at, YEAR) AS customer_calendar_year_start,
  DATE_TRUNC(csd.customer_created_at, MONTH) customer_calendar_month_start,
  DATE_TRUNC(csd.customer_created_at, WEEK) AS customer_calendar_week_start,
  cal2.year AS customer_retail_year,
  cal2.month AS customer_retail_month,
  cal2.week_start_date AS customer_retail_week,
  cal2.quarter AS customer_retail_quarter,
  FIRST_VALUE(o.mmlf_order_type) OVER (PARTITION BY o.customer_id ORDER BY o.created_at) AS customer_first_order_type,
  cg.customer_group_code AS customer_group,
  o.customer_email AS customer_email,
  o.customer_firstname AS customer_firstname,
  o.customer_lastname AS customer_lastname,
  a.city AS billing_city,
  r.code AS billing_region,
  a.postcode AS billing_postcode,
  a2.city AS shipping_city,
  r2.code AS shipping_region,
  a2.postcode AS shipping_postcode,
  con.customer_order_number AS customer_order_number,
  oitem.qty_ordered AS qty_ordered,
  oitem.qty_invoiced AS qty_invoiced,
  oitem.qty_canceled AS qty_canceled,
  oitem.qty_shipped AS qty_shipped,
  oitem.qty_refunded AS qty_refunded,
  oitem.value_ordered AS value_ordered,
  oitem.value_kept AS value_kept,
  oitem.avg_price_ordered AS avg_price_ordered,
  oitem.avg_price_kept AS avg_price_kept,
  IF((oitem.qty_ordered - oitem.qty_canceled) > 0,
    (oitem.qty_invoiced - oitem.qty_canceled) / (oitem.qty_ordered - oitem.qty_canceled),
    NULL) AS keep_rate,
  IF((oitem.qty_ordered - oitem.qty_canceled) > 0,
    oitem.qty_refunded / (oitem.qty_ordered - oitem.qty_canceled),
    NULL) AS item_return_rate,
  IF(oi.gross > 0,
    (oi.gross_refunded + oi.discount_refunded - COALESCE(credit_memo.adjustment,
        0)) / oi.gross,
    NULL) AS gross_return_rate,
  IF(o.status != 'canceled'
    AND oi.gross > 0,
    1,
    0) AS is_gross_purchase,
  IF( (LEAD(o.created_at) OVER (PARTITION BY o.customer_id ORDER BY o.created_at)) IS NOT NULL,
    1,
    0) AS ordered_again,
  LEAD(o.mmlf_order_type) OVER (PARTITION BY o.customer_id ORDER BY o.created_at) AS next_order_type,
  DATE_DIFF( DATE(LEAD(o.created_at) OVER (PARTITION BY o.customer_id ORDER BY o.created_at), 'America/New_York'), DATE(o.created_at, 'America/New_York'), DAY) / 7 AS weeks_until_next_order,
  DATE_DIFF( DATE(o.created_at, 'America/New_York'), DATE(LAG(o.created_at) OVER (PARTITION BY o.customer_id ORDER BY o.created_at), 'America/New_York'), DAY) / 7 AS weeks_since_prior_order,
  DATE_DIFF(CURRENT_DATE(), DATE(o.created_at, 'America/New_York'), DAY) AS days_since_order,
  DATE_DIFF(CURRENT_DATE(), DATE(o.created_at, 'America/New_York'), DAY) / 7 AS weeks_since_order
FROM
  `warehouse-152021.website.mage_sales_flat_order` AS o
LEFT JOIN
  `warehouse-152021.website.mage_mmlafleur_sales_location` AS location
ON
  location.id = o.mmlf_location_id
LEFT JOIN (
  SELECT
    order_id,
    COALESCE(SUM(gross),
      0) AS gross,
    -COALESCE(SUM(amount_refunded),
      0) AS gross_refunded,
    -COALESCE(ABS(SUM(discount_amount)),
      0) AS discount,
    COALESCE(SUM(discount_refunded),
      0) AS discount_refunded,
    COALESCE(SUM(gift_card_gross),
      0) AS gift_card_gross,
    -COALESCE(SUM(gift_card_amount_refunded),
      0) AS gift_card_gross_refunded,
    -COALESCE(ABS(SUM(gift_card_discount_amount)),
      0) AS gift_card_discount,
    COALESCE(SUM(gift_card_discount_refunded),
      0) AS gift_card_discount_refunded
  FROM
    {{ref('dbt_item')}}
  GROUP BY
    order_id ) AS oi
ON
  oi.order_id = o.entity_id
LEFT JOIN (
  SELECT
    order_id,
    SUM(adjustment) AS adjustment
  FROM
    `warehouse-152021.website.mage_sales_flat_creditmemo`
  GROUP BY
    order_id ) AS credit_memo
ON
  credit_memo.order_id = o.entity_id
LEFT JOIN (
  SELECT
    o.entity_id AS order_id,
    COALESCE(SUM(t.base_giftcard_amount),
      0) AS amount
  FROM
    `warehouse-152021.website.mage_sales_flat_order` AS o
  LEFT JOIN
    `warehouse-152021.website.mage_aw_giftcard_quote_totals` AS t
  ON
    t.quote_entity_id = o.quote_id
  WHERE
    t.giftcard_id IN (
    SELECT
      giftcard_id
    FROM
      credit_discount_list)
  GROUP BY
    o.entity_id ) AS credit_discount
ON
  credit_discount.order_id = o.entity_id
LEFT JOIN (
  SELECT
    o.entity_id AS order_id,
    COALESCE(SUM(t.base_giftcard_amount),
      0) AS amount
  FROM
    `warehouse-152021.website.mage_sales_flat_order` AS o
  LEFT JOIN
    `warehouse-152021.website.mage_sales_flat_creditmemo` AS c
  ON
    c.order_id = o.entity_id
  LEFT JOIN
    `warehouse-152021.website.mage_aw_giftcard_creditmemo_totals` AS t
  ON
    t.creditmemo_entity_id = c.entity_id
  WHERE
    t.giftcard_id IN (
    SELECT
      giftcard_id
    FROM
      credit_discount_list)
  GROUP BY
    o.entity_id ) AS credit_discount_refunded
ON
  credit_discount_refunded.order_id = o.entity_id
LEFT JOIN (
  SELECT
    o.entity_id AS order_id,
    COALESCE(SUM(t.base_giftcard_amount),
      0) AS amount
  FROM
    `warehouse-152021.website.mage_sales_flat_order` AS o
  LEFT JOIN
    `warehouse-152021.website.mage_aw_giftcard_quote_totals` AS t
  ON
    t.quote_entity_id = o.quote_id
  WHERE
    t.giftcard_id IN (
    SELECT
      giftcard_id
    FROM
      gift_card_discount_list)
  GROUP BY
    o.entity_id ) AS gift_card_discount
ON
  gift_card_discount.order_id = o.entity_id
LEFT JOIN (
  SELECT
    o.entity_id AS order_id,
    COALESCE(SUM(t.base_giftcard_amount),
      0) AS amount
  FROM
    `warehouse-152021.website.mage_sales_flat_order` AS o
  LEFT JOIN
    `warehouse-152021.website.mage_sales_flat_creditmemo` AS c
  ON
    c.order_id = o.entity_id
  LEFT JOIN
    `warehouse-152021.website.mage_aw_giftcard_creditmemo_totals` AS t
  ON
    t.creditmemo_entity_id = c.entity_id
  WHERE
    t.giftcard_id IN (
    SELECT
      giftcard_id
    FROM
      gift_card_discount_list)
  GROUP BY
    o.entity_id ) AS gift_card_discount_refunded
ON
  gift_card_discount_refunded.order_id = o.entity_id
LEFT JOIN
  (SELECT parent_id, address_type, postcode, city, region_id FROM `warehouse-152021.website.mage_sales_flat_order_address` GROUP BY parent_id, address_type, postcode, city, region_id) AS a
ON
  o.entity_id = a.parent_id
  AND a.address_type = 'billing'
LEFT JOIN
  `warehouse-152021.website.mage_directory_country_region` AS r
ON
  a.region_id = r.region_id
LEFT JOIN
  (SELECT parent_id, address_type, postcode, city, region_id FROM `warehouse-152021.website.mage_sales_flat_order_address` GROUP BY parent_id, address_type, postcode, city, region_id) AS a2
ON
  o.entity_id = a2.parent_id
  AND a2.address_type = 'shipping'
LEFT JOIN
  `warehouse-152021.website.mage_directory_country_region` AS r2
ON
  a2.region_id = r2.region_id
LEFT JOIN
  `warehouse-152021.website.mage_customer_entity` AS c
ON
  c.entity_id = o.customer_id
LEFT JOIN
  `warehouse-152021.website.mage_customer_group` AS cg
ON
  cg.customer_group_id = c.group_id
LEFT JOIN (
  SELECT
    entity_id,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at) AS customer_order_number
  FROM
    `warehouse-152021.website.mage_sales_flat_order`
  WHERE
    status NOT IN ('canceled')) AS con
ON
  con.entity_id = o.entity_id
LEFT JOIN
  `warehouse-152021.website.mage_customer_entity_varchar` AS book
ON
  book.entity_id = o.customer_id
  AND book.attribute_id = 177
LEFT JOIN
  `warehouse-152021.simple.calendar` AS cal
ON
  cal.date = DATE(o.created_at, "America/New_York")
LEFT JOIN (
  SELECT
    customer_id,
    MIN(DATE(created_at, 'America/New_York')) AS customer_created_at
  FROM
    `warehouse-152021.website.mage_sales_flat_order`
  WHERE
    status NOT IN ('canceled')
  GROUP BY
    customer_id ) csd
ON
  csd.customer_id = o.customer_id
LEFT JOIN
  `warehouse-152021.simple.calendar` AS cal2
ON
  cal2.date = csd.customer_created_at
LEFT JOIN (
  SELECT
    order_id,
    SUM(qty_ordered) AS qty_ordered,
    SUM(qty_invoiced) AS qty_invoiced,
    SUM(qty_canceled) AS qty_canceled,
    SUM(qty_shipped) AS qty_shipped,
    SUM(qty_refunded) AS qty_refunded,
    SUM(IF(qty_canceled = 0,
        price,
        0)) AS value_ordered,
    SUM(IF(qty_canceled = 0
        AND qty_invoiced > 0,
        price,
        0)) AS value_kept,
    AVG(IF(qty_canceled = 0,
        price,
        NULL)) AS avg_price_ordered,
    AVG(IF(qty_canceled = 0
        AND qty_invoiced > 0,
        price,
        NULL)) AS avg_price_kept
  FROM
    {{ref('dbt_item')}}
  WHERE
    sku NOT IN ('DRESSING-ROOM-AUTH',
      'bento-fee')
  GROUP BY
    order_id ) oitem
ON
  oitem.order_id = o.entity_id
