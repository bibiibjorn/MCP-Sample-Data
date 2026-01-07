# Star Schema Design Knowledge

## Star Schema Principles

### Fact Tables
- Contain measures (numeric values)
- At the grain of a business event
- Connect to dimensions via foreign keys
- Typically the largest tables (many rows)

### Dimension Tables
- Contain descriptive attributes
- Have a surrogate key (integer)
- Often have a natural/business key
- Typically smaller (thousands to millions of rows)

## Grain Definition

The grain defines the level of detail in a fact table:
- **Transaction grain**: One row per transaction
- **Periodic snapshot**: One row per period per entity
- **Accumulating snapshot**: One row per lifecycle

## Relationship Patterns

### Star Schema (Simple)
```
      dim_date
         |
dim_product -- fact_sales -- dim_customer
         |
     dim_store
```

### Snowflake (Normalized Dimensions)
```
category -- subcategory -- product -- fact_sales
```

### Constellation (Multiple Facts)
```
dim_product -- fact_sales
     |
dim_product -- fact_inventory
```

## Best Practices

1. **Denormalize dimensions**: Flatten for query performance
2. **Use surrogate keys**: Integer keys for joins
3. **Conform dimensions**: Shared across facts
4. **Avoid many-to-many**: Use bridge tables if needed
5. **Date dimension**: Always have a proper date table

## Power BI Specific

1. **Single direction relationships**: Prefer for clarity
2. **Bidirectional with care**: Can cause ambiguity
3. **No calculated columns in facts**: Use measures
4. **Hide technical keys**: Show only descriptive columns

## Validation Rules

1. **Referential integrity**: All FKs must exist in dimension
2. **No orphan records**: Every fact row has valid dimension keys
3. **Unique dimension keys**: PKs must be unique
4. **Date coverage**: Date dimension covers all fact dates
