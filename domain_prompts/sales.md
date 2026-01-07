# Sales Domain Guidance

## Overview
The sales domain covers revenue analysis, customer analytics, and order management for Power BI.

## Typical Star Schema

### Dimension Tables
- **Customers**: Customer details, segments, geography
- **Products**: Product hierarchy, categories, pricing
- **Date**: Standard calendar dimension
- **Geography**: Territories, regions, countries
- **Sales Reps**: Sales team members

### Fact Tables
- **Sales**: Order-level transactions
- **Sales Targets**: Quotas and targets by rep/territory
- **Returns**: Returned orders

## Key Relationships

```
Sales Fact
├── Date Dimension (date_key)
├── Customer Dimension (customer_key)
├── Product Dimension (product_key)
├── Geography Dimension (geography_key)
└── Sales Rep Dimension (sales_rep_key)
```

## Validation Rules

### Referential Integrity
All foreign keys in fact table must exist in dimensions.
Use `04_check_referential_integrity` tool.

### Business Rules
- Quantity > 0
- Unit price > 0
- Discount between 0 and 1 (or 0-100%)
- Sales amount = Quantity × Price × (1 - Discount)

### Data Quality
- No orphaned records
- Valid date ranges
- Consistent currency codes

## Recommended Tools

1. **Discovery**
   - `01_find_relationships`: Map fact to dimensions
   - `01_profile_column`: Understand value distributions

2. **Generation**
   - `02_generate_star_schema`: Create complete star schema
   - `02_generate_dimension`: Create individual dimensions
   - `02_generate_fact`: Create sales fact table

3. **Validation**
   - `04_check_referential_integrity`: Validate FK relationships
   - `04_detect_anomalies`: Find outliers in sales data
   - `04_validate_data`: Apply business rules

4. **Export**
   - `05_optimize_for_powerbi`: Optimize for import

## Common Patterns

### Customer Segmentation
```yaml
segments:
  - Enterprise (large companies, high value)
  - Mid-Market (medium companies)
  - Small Business (small companies)
  - Consumer (individuals)
```

### Product Hierarchy
```
Category → Subcategory → Product
```

### Date Grain
- Daily for detailed analysis
- Monthly for aggregated reporting
- Fiscal vs Calendar periods

## Sample Star Schema Generation

```yaml
# Generate complete sales star schema
02_generate_star_schema:
  schema_name: sales_analytics
  domain: sales
  output_dir: projects/sales/
  fact_rows: 100000

# This generates:
# - dim_customer.csv
# - dim_product.csv
# - dim_time.csv (date dimension)
# - fact_sales.csv
```

## Power BI Optimization Tips

1. **Use integer keys** for relationships
2. **Categorical columns** for low-cardinality strings
3. **Remove unused columns** from fact tables
4. **Star schema** preferred over snowflake
5. **Pre-aggregate** where possible for large datasets
