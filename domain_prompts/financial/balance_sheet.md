# Balance Sheet Domain Knowledge

## Fundamental Rules

### The Accounting Equation
**Assets = Liabilities + Equity**

This must ALWAYS balance. Any sample data for balance sheets must satisfy this equation.

### Trial Balance Rule
For any trial balance data:
- **Sum of Debits = Sum of Credits** (per period)
- Imbalances indicate data quality issues

### Sign Conventions
| Account Type | Normal Balance | Increases With | Decreases With |
|--------------|----------------|----------------|----------------|
| Assets | Debit | Debit | Credit |
| Liabilities | Credit | Credit | Debit |
| Equity | Credit | Credit | Debit |
| Revenue | Credit | Credit | Debit |
| Expenses | Debit | Debit | Credit |

## Common Account Code Structures

### 4-Digit Structure
- `1xxx` - Assets
- `2xxx` - Liabilities
- `3xxx` - Equity
- `4xxx` - Revenue
- `5xxx` - Cost of Goods Sold
- `6xxx` - Operating Expenses
- `7xxx` - Other Income/Expenses
- `8xxx` - Taxes
- `9xxx` - Closing/Summary Accounts

### Hierarchical Structure
- Level 1: Account Category (1 digit)
- Level 2: Account Group (2 digits)
- Level 3: Account Subgroup (3 digits)
- Level 4: Account Detail (4 digits)

Example:
- `1` = Assets
- `11` = Current Assets
- `111` = Cash and Cash Equivalents
- `1110` = Petty Cash
- `1111` = Checking Account

## Validation Rules to Apply

1. **Balance Check**: Sum(Debit) = Sum(Credit) per period
2. **Account Hierarchy**: Every account code must exist in Chart of Accounts
3. **Period Continuity**: Ending balance = Beginning balance of next period
4. **No Negative Balances**: For certain account types (Cash, Inventory)
5. **Reasonableness**: Amounts should be within expected ranges

## Common Fact Table Structures

### GL Transactions (Fact)
| Column | Type | Description |
|--------|------|-------------|
| transaction_id | INTEGER | Primary key |
| transaction_date | DATE | When posted |
| period_id | INTEGER | FK to dim_period |
| account_id | INTEGER | FK to dim_account |
| entity_id | INTEGER | FK to dim_entity |
| debit_amount | DECIMAL(18,2) | Debit value |
| credit_amount | DECIMAL(18,2) | Credit value |
| description | VARCHAR | Transaction description |

### Trial Balance (Fact)
| Column | Type | Description |
|--------|------|-------------|
| period_id | INTEGER | FK to dim_period |
| account_id | INTEGER | FK to dim_account |
| entity_id | INTEGER | FK to dim_entity |
| beginning_balance | DECIMAL(18,2) | Period start |
| debit_activity | DECIMAL(18,2) | Period debits |
| credit_activity | DECIMAL(18,2) | Period credits |
| ending_balance | DECIMAL(18,2) | Period end |

## Related Dimension Tables

### Chart of Accounts (Dimension)
- account_id (PK)
- account_code
- account_name
- account_type (Asset, Liability, Equity, Revenue, Expense)
- account_category
- account_group
- parent_account_id (for hierarchy)
- is_posting_account (boolean)
- normal_balance (Debit/Credit)

### Period (Dimension)
- period_id (PK)
- period_key (YYYYMM)
- period_name
- fiscal_year
- fiscal_quarter
- fiscal_month
- is_year_end
- is_closed

## Data Generation Tips

1. **Generate dimensions first**, then facts
2. **Use realistic account distributions**: 80% of activity in 20% of accounts
3. **Generate balanced transactions**: Every debit has a corresponding credit
4. **Use lognormal distribution** for amounts (many small, few large)
5. **Ensure period coverage**: Data for all periods in date range
