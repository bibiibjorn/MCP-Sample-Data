# Financial Domain Guidance

## Overview
The financial domain covers financial reporting, accounting, and analysis use cases for Power BI.

## Typical Data Structures

### Dimension Tables
- **Chart of Accounts**: Account hierarchy with codes, names, types (Asset, Liability, Equity, Revenue, Expense)
- **Cost Centers**: Organizational units for cost allocation
- **Departments**: Business units and divisions
- **Date Dimension**: Standard calendar with fiscal periods

### Fact Tables
- **General Ledger**: Journal entries with debit/credit amounts
- **Trial Balance**: Period-end account balances
- **Budget**: Planned amounts by account and period
- **Actuals**: Actual financial transactions

## Key Validation Rules

### Balance Sheet Equation
```
Assets = Liabilities + Equity
```
Use `08_validate_balance_sheet` tool to validate.

### Journal Entry Balance
```
Total Debits = Total Credits (per transaction)
```
Use `04_validate_balance` tool to validate.

### Account Hierarchy Rollup
Parent accounts should equal sum of child accounts.
Use `08_rollup_through_hierarchy` tool to validate.

## Recommended Tools

1. **Discovery**
   - `01_analyze_file`: Understand file structure
   - `01_detect_domain`: Confirm financial domain classification

2. **Generation**
   - `02_generate_dimension`: Create chart of accounts
   - `02_generate_fact`: Create GL transactions
   - `02_generate_date_dimension`: Create date dimension with fiscal periods

3. **Validation**
   - `04_validate_balance`: Check debit/credit balance
   - `08_validate_balance_sheet`: Validate balance sheet equation
   - `08_rollup_through_hierarchy`: Validate account rollups

4. **Mapping**
   - `08_load_context`: Load related files
   - `08_discover_mappings`: Find column relationships

## Common Patterns

### Account Codes
- Often hierarchical (e.g., 1000 -> 1100 -> 1110)
- Parent codes are often shorter or have zeros in trailing positions

### Amounts
- May use signed amounts (positive/negative)
- Or separate debit/credit columns
- Currency precision matters (typically 2-4 decimal places)

### Periods
- Fiscal periods may differ from calendar
- Month-end close dates are important
- Year-end adjustments are common

## Sample Template Usage

```yaml
# Generate Chart of Accounts
02_generate_from_template:
  template_path: templates/financial_chart_of_accounts.yaml
  output_path: projects/finance/dim_accounts.csv
  row_count: 100

# Generate GL Transactions
02_generate_fact:
  fact_type: finance
  dimension_files:
    accounts: projects/finance/dim_accounts.csv
    date: projects/finance/dim_date.csv
  output_path: projects/finance/fact_gl.csv
  row_count: 50000
```
