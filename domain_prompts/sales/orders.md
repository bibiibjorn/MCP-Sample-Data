# Sales Orders Domain Knowledge

## Order Lifecycle

```
Quote → Order → Shipment → Invoice → Payment
```

### Order Statuses
| Status | Code | Description |
|--------|------|-------------|
| Draft | D | Not yet submitted |
| Pending | P | Awaiting approval |
| Approved | A | Ready for fulfillment |
| Partial | R | Partially shipped |
| Complete | C | Fully shipped |
| Cancelled | X | Order cancelled |
| Closed | Z | Order closed |

## Fact Table: Sales Orders

| Column | Type | Description |
|--------|------|-------------|
| order_id | INT | Primary key |
| order_number | VARCHAR | Business key |
| order_date | DATE | When placed |
| customer_id | INT | FK to dim_customer |
| product_id | INT | FK to dim_product |
| date_id | INT | FK to dim_date |
| quantity | INT | Units ordered |
| unit_price | DECIMAL | Price per unit |
| discount_pct | DECIMAL | Discount percentage |
| line_total | DECIMAL | Extended amount |
| tax_amount | DECIMAL | Tax |
| order_status | VARCHAR | Current status |

## Key Metrics

- **Average Order Value (AOV)** = Total Revenue / Number of Orders
- **Units per Transaction** = Total Units / Number of Orders
- **Discount Rate** = Total Discounts / Gross Revenue

## Data Generation Tips

### Distribution Patterns
- **Order frequency**: Pareto - 20% of customers = 80% of orders
- **Order size**: Lognormal distribution
- **Product mix**: Some products much more popular
- **Seasonality**: Peaks around holidays, end of quarters

### Realistic Patterns
1. Orders cluster on business days
2. Larger orders often have larger discounts
3. B2B orders larger than B2C
4. Repeat customers order more frequently over time

## Related Dimensions

### Customer Dimension
- customer_id, customer_name, segment, region, country
- customer_since_date, tier, credit_limit

### Product Dimension
- product_id, product_name, category, subcategory
- brand, unit_cost, list_price, is_active

### Date Dimension
- Standard date dimension with fiscal periods
