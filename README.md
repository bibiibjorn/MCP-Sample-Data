# MCP-Sample-Data

**LLM-Orchestrated Sample Data Platform for Power BI**

A standalone MCP server that helps Power BI developers create, understand, edit, analyze, and validate sample data files (fact tables, dimension tables).

## Features

- **31 Tools** across 7 categories
- **Domain-aware** - Understands financial, sales, inventory, HR data patterns
- **Large file support** - DuckDB + Polars for efficient operations
- **Validation** - Balance checks, referential integrity, custom rules
- **Generation** - Create realistic sample data from templates
- **Power BI optimized** - Export formats optimized for Power BI import

## Quick Start

1. Run `setup-dev.bat` to install dependencies and configure Claude Desktop
2. Restart Claude Desktop
3. The "MCP-Sample-Data" server will appear in your MCP servers list

## Tool Categories

| Category | Tools | Purpose |
|----------|-------|---------|
| 01 - Discovery | 5 | Analyze and understand data files |
| 02 - Generation | 6 | Create sample data |
| 03 - Editing | 6 | Modify existing data |
| 04 - Validation | 5 | Check data quality and rules |
| 05 - Export | 3 | Export for Power BI |
| 06 - Projects | 4 | Manage data projects |
| 07 - Help | 2 | Documentation |

## Domain Knowledge

The server includes domain knowledge prompts for:
- **Financial** - Balance sheets, income statements, GL transactions
- **Sales** - Orders, customers, products
- **Inventory** - Stock levels, movements, warehouses
- **HR** - Employees, departments, payroll
- **Generic** - Star schema design, data quality

## Requirements

- Python 3.13+
- Windows 10/11
- Claude Desktop

## Manual Start

```bash
cd "C:\Users\bjorn.braet\powerbi-mcp-servers\MCP-Sample Data"
venv\Scripts\activate
python src/sample_data_server.py
```

## Documentation

See [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) for full technical documentation.
