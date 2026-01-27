models:
  - name: analytics_orders
    description: >
      A table of Shopify orders. Each row represents a single order exactly as Shopify recorded it,
      without interpretation, metrics, or business logic applied.
    columns:
      - name: order_id
        description: Unique identifier for the order.
      - name: order_number
        description: Human-readable order number.
      - name: customer_id
        description: Customer associated with the order, if known.
      - name: created_at
        description: When the order was created in Shopify.
      - name: processed_at
        description: When the order was processed.
      - name: financial_status
        description: Shopify’s reported payment status for the order.
      - name: fulfillment_status
        description: Shopify’s reported fulfillment status.
      - name: currency
        description: Currency used for the order.
      - name: total_price
        description: Total order amount as reported by Shopify.
  - name: analytics_order_line_items
    description: >
      A detailed table of everything purchased in each order. Each row represents one product or
      variant inside an order.
    columns:
      - name: line_item_id
        description: Unique identifier for the line item.
      - name: order_id
        description: Order this line item belongs to.
      - name: product_id
        description: Product that was purchased, if available.
      - name: variant_id
        description: Product variant that was purchased, if available.
      - name: quantity
        description: Number of units purchased.
      - name: price
        description: Unit price of the item as reported by Shopify.
      - name: product_title
        description: Name of the product.
      - name: variant_title
        description: Name of the variant, if applicable.
  - name: analytics_customers
    description: >
      A table of customers that Shopify recognizes as customers. This table represents who the
      customer is, not what they have done.
    columns:
      - name: customer_id
        description: Unique identifier for the customer.
      - name: email
        description: Customer email address, if provided.
      - name: first_name
        description: Customer’s first name.
      - name: last_name
        description: Customer’s last name.
      - name: created_at
        description: When the customer record was created.
      - name: last_order_at
        description: Shopify’s recorded date of the customer’s most recent order.
      - name: accepts_marketing
        description: Whether the customer opted into marketing communications.
  - name: analytics_products
    description: >
      A catalog of products that exist in Shopify. This table describes what products exist,
      not how they perform.
    columns:
      - name: product_id
        description: Unique identifier for the product.
      - name: title
        description: Product name.
      - name: vendor
        description: Brand or vendor of the product.
      - name: product_type
        description: Shopify product category or type.
      - name: status
        description: Whether the product is active, archived, or draft.
      - name: created_at
        description: When the product was created.
      - name: updated_at
        description: When the product was last updated.
  - name: analytics_product_variants
    description: >
      A SKU-level table describing how products are configured, such as size or color.
      Each row represents one specific variant of a product.
    columns:
      - name: variant_id
        description: Unique identifier for the variant.
      - name: product_id
        description: Product this variant belongs to.
      - name: sku
        description: SKU code, if provided.
      - name: price
        description: Price of the variant.
      - name: compare_at_price
        description: Original price before a discount, if applicable.
      - name: inventory_quantity
        description: Shopify-reported inventory quantity.
      - name: created_at
        description: When the variant was created.
  - name: analytics_refunds
    description: >
      A table of refund events as Shopify recorded them. Each row represents a single
      refund action.
    columns:
      - name: refund_id
        description: Unique identifier for the refund event.
      - name: order_id
        description: Order associated with the refund.
      - name: created_at
        description: When the refund was created.
      - name: processed_at
        description: When the refund was processed.
      - name: currency
        description: Currency used for the refund.
      - name: note
        description: Internal note attached to the refund, if any.
