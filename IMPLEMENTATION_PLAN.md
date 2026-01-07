# MCP-Sample-Data: Complete Implementation Plan

> **Version**: 1.0.0
> **Target Location**: `C:\Users\bjorn.braet\powerbi-mcp-servers\MCP-Sample Data`
> **Architecture**: LLM-Orchestrated Sample Data Platform for Power BI

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Architecture Overview](#2-architecture-overview)
3. [Directory Structure](#3-directory-structure)
4. [Core Dependencies](#4-core-dependencies)
5. [Tool Catalog](#5-tool-catalog)
6. [Domain Knowledge System](#6-domain-knowledge-system)
7. [Implementation Files](#7-implementation-files)
8. [Setup & Configuration](#8-setup--configuration)
9. [Testing Strategy](#9-testing-strategy)
10. [Usage Examples](#10-usage-examples)

---

## 1. Executive Summary

### Purpose

MCP-Sample-Data is a standalone MCP server designed to help Power BI developers create, understand, edit, analyze, and validate sample data files (fact tables, dimension tables). It leverages LLM reasoning for domain understanding while providing powerful tools for large-scale data operations.

### Key Capabilities

| Capability | Description |
|------------|-------------|
| **Understand** | Schema inference, domain detection, relationship discovery |
| **Create** | Generate fact/dimension tables with realistic data |
| **Edit** | SQL-like operations on large files via DuckDB |
| **Analyze** | Validation, balance checks, referential integrity |
| **Export** | Power BI optimized formats (CSV, Parquet) |

### Design Philosophy

The LLM (Claude) acts as the "brain" that:
- Understands domain context (financial, sales, inventory, etc.)
- Makes decisions about validation rules
- Orchestrates tool calls in the right sequence
- Provides natural language explanations

The MCP server provides:
- Fast data operations via DuckDB + Polars
- Rich metadata extraction for LLM reasoning
- Validation primitives the LLM can compose
- Domain knowledge prompts for context

---

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Claude (LLM Orchestrator)                     │
│   - Understands domain context from prompts                         │
│   - Reasons about data relationships                                 │
│   - Composes validation rules                                        │
│   - Provides natural language explanations                           │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ MCP Protocol (stdio)
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     MCP-Sample-Data Server                          │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │                    Server Layer                                │  │
│  │  - MCP Server (stdio)                                         │  │
│  │  - Handler Registry                                            │  │
│  │  - Tool Dispatcher                                             │  │
│  │  - Resource Manager (domain prompts)                           │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                              │                                      │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │                    Handler Layer                               │  │
│  │  - Discovery Handlers (analyze, detect, suggest)               │  │
│  │  - Generation Handlers (fact, dimension, fill)                 │  │
│  │  - Editing Handlers (query, update, transform)                 │  │
│  │  - Validation Handlers (rules, balance, integrity)             │  │
│  │  - Export Handlers (csv, parquet, powerbi)                     │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                              │                                      │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │                    Core Engine Layer                           │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │  │
│  │  │   DuckDB    │  │   Polars    │  │   Faker/Mimesis     │  │  │
│  │  │  (SQL ops)  │  │ (transforms)│  │  (data generation)  │  │  │
│  │  └─────────────┘  └─────────────┘  └─────────────────────┘  │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │  │
│  │  │   Pandera   │  │   Schema    │  │   Domain Rules      │  │  │
│  │  │(validation) │  │ (inference) │  │     Engine          │  │  │
│  │  └─────────────┘  └─────────────┘  └─────────────────────┘  │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                              │                                      │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │                    Storage Layer                               │  │
│  │  - Project Manager (multi-project support)                     │  │
│  │  - File Manager (CSV, Parquet, Excel)                          │  │
│  │  - Cache Manager (query results)                               │  │
│  └──────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 3. Directory Structure

```
MCP-Sample Data/
├── .claude/
│   └── settings.local.json
├── .gitignore
├── .vscode/
│   └── settings.json
├── config/
│   ├── default_config.json
│   └── local_config.json          # User overrides (gitignored)
├── core/
│   ├── __init__.py
│   ├── config/
│   │   ├── __init__.py
│   │   └── config_manager.py
│   ├── discovery/
│   │   ├── __init__.py
│   │   ├── schema_inference.py
│   │   ├── domain_detector.py
│   │   ├── relationship_finder.py
│   │   └── pattern_analyzer.py
│   ├── generation/
│   │   ├── __init__.py
│   │   ├── fact_generator.py
│   │   ├── dimension_generator.py
│   │   ├── template_engine.py
│   │   └── distribution_sampler.py
│   ├── editing/
│   │   ├── __init__.py
│   │   ├── duckdb_engine.py
│   │   ├── polars_engine.py
│   │   └── query_builder.py
│   ├── validation/
│   │   ├── __init__.py
│   │   ├── rule_engine.py
│   │   ├── balance_checker.py
│   │   ├── referential_checker.py
│   │   ├── statistical_validator.py
│   │   └── error_handler.py
│   ├── mapping/                        # NEW: Multi-file mapping support
│   │   ├── __init__.py
│   │   ├── mapping_discovery.py        # Fuzzy/semantic column matching
│   │   ├── mapping_manager.py          # Mapping definition storage
│   │   ├── hierarchy_analyzer.py       # Detect and navigate hierarchies
│   │   ├── context_loader.py           # Load multiple files as context
│   │   └── cross_file_validator.py     # Validate through mappings
│   ├── export/
│   │   ├── __init__.py
│   │   ├── csv_exporter.py
│   │   ├── parquet_exporter.py
│   │   └── powerbi_optimizer.py
│   └── storage/
│       ├── __init__.py
│       ├── project_manager.py
│       ├── file_manager.py
│       └── cache_manager.py
├── docs/
│   ├── USER_GUIDE.md
│   └── DOMAIN_KNOWLEDGE.md
├── domain_prompts/
│   ├── financial/
│   │   ├── balance_sheet.md
│   │   ├── income_statement.md
│   │   ├── general_ledger.md
│   │   └── chart_of_accounts.md
│   ├── sales/
│   │   ├── orders.md
│   │   ├── products.md
│   │   └── customers.md
│   ├── inventory/
│   │   ├── stock_levels.md
│   │   ├── warehouses.md
│   │   └── movements.md
│   ├── hr/
│   │   ├── employees.md
│   │   ├── departments.md
│   │   └── payroll.md
│   └── generic/
│       ├── star_schema.md
│       └── data_quality.md
├── exports/                        # Default export location
├── logs/
├── projects/                       # Project storage
├── server/
│   ├── __init__.py
│   ├── registry.py
│   ├── dispatch.py
│   ├── resources.py
│   ├── tool_schemas.py
│   └── handlers/
│       ├── __init__.py
│       ├── discovery_handlers.py
│       ├── generation_handlers.py
│       ├── editing_handlers.py
│       ├── validation_handlers.py
│       ├── mapping_handlers.py         # NEW: Multi-file mapping handlers
│       ├── export_handlers.py
│       ├── project_handlers.py
│       └── help_handlers.py
├── src/
│   ├── __init__.py
│   ├── __version__.py
│   └── sample_data_server.py       # Main entry point
├── templates/
│   ├── financial/
│   │   ├── trial_balance.yaml
│   │   ├── chart_of_accounts.yaml
│   │   └── gl_transactions.yaml
│   ├── sales/
│   │   ├── fact_sales.yaml
│   │   ├── dim_customer.yaml
│   │   ├── dim_product.yaml
│   │   └── dim_date.yaml
│   ├── inventory/
│   │   └── stock_movements.yaml
│   └── hr/
│       └── employee_master.yaml
├── tests/
│   ├── __init__.py
│   ├── test_discovery.py
│   ├── test_generation.py
│   ├── test_validation.py
│   └── test_integration.py
├── requirements.txt
├── setup-dev.bat
├── README.md
└── IMPLEMENTATION_PLAN.md          # This file
```

---

## 4. Core Dependencies

### requirements.txt

```
# MCP Framework
mcp>=1.0.0

# Data Processing
duckdb>=1.0.0
polars>=1.35.0
pyarrow>=15.0.0

# Data Generation
faker>=28.0.0
mimesis>=18.0.0

# Schema Validation
pandera>=0.20.0

# File Formats
openpyxl>=3.1.0

# Configuration
pyyaml>=6.0.0
orjson>=3.9.0

# Utilities
tqdm>=4.66.0
python-dateutil>=2.8.0

# Fuzzy Matching (for mapping discovery)
rapidfuzz>=3.9.0
```

---

## 5. Tool Catalog

### 5.1 Tool Categories Overview

| Category | Prefix | Tools | Purpose |
|----------|--------|-------|---------|
| Discovery | 01_ | 5 | Analyze and understand data files |
| Generation | 02_ | 6 | Create sample data |
| Editing | 03_ | 6 | Modify existing data |
| Validation | 04_ | 5 | Check data quality and rules |
| Export | 05_ | 3 | Export for Power BI |
| Projects | 06_ | 4 | Manage data projects |
| Help | 07_ | 2 | Documentation and guidance |
| **Mapping** | **08_** | **6** | **Multi-file relationships & cross-validation** |

**Total: 37 tools**

---

### 5.2 Discovery Tools (Category 01)

#### 01_Analyze_File
**Purpose**: Deep analysis of a data file for LLM consumption
**When to use**: First step when working with any data file
**Parameters**:
```json
{
  "path": {
    "type": "string",
    "description": "Path to CSV, Parquet, or Excel file",
    "required": true
  },
  "sample_size": {
    "type": "integer",
    "description": "Number of rows to analyze (default: 1000)",
    "default": 1000
  },
  "deep_analysis": {
    "type": "boolean",
    "description": "Include statistical analysis and pattern detection",
    "default": true
  }
}
```
**Returns**: Rich metadata including:
- File info (path, rows, columns, size)
- Column details (dtype, samples, nulls, uniques, statistics)
- Detected patterns (dates, codes, currencies, etc.)
- Semantic type inference (ID, Amount, Name, Date, etc.)
- Domain hints (financial keywords, sales indicators)
- Quality issues (nulls, duplicates, outliers)
- Potential relationships (key columns)

---

#### 01_Compare_Files
**Purpose**: Compare structure and content of two files
**When to use**: Understanding relationships between files, detecting schema drift
**Parameters**:
```json
{
  "path1": {
    "type": "string",
    "description": "Path to first file",
    "required": true
  },
  "path2": {
    "type": "string",
    "description": "Path to second file",
    "required": true
  },
  "compare_content": {
    "type": "boolean",
    "description": "Compare actual values (slower)",
    "default": false
  }
}
```
**Returns**: Schema differences, potential join keys, value overlaps

---

#### 01_Detect_Domain
**Purpose**: Classify the domain/type of data
**When to use**: Understanding what kind of data you're working with
**Parameters**:
```json
{
  "path": {
    "type": "string",
    "description": "Path to data file",
    "required": true
  }
}
```
**Returns**: Domain classification with confidence scores:
- Primary domain (financial, sales, inventory, hr, generic)
- Sub-type (balance_sheet, orders, stock_levels, etc.)
- Confidence score
- Detected indicators
- Suggested validation rules
- Recommended related tables

---

#### 01_Suggest_Schema
**Purpose**: Generate a recommended schema for the data
**When to use**: Preparing data for Power BI import
**Parameters**:
```json
{
  "path": {
    "type": "string",
    "description": "Path to data file",
    "required": true
  },
  "target": {
    "type": "string",
    "enum": ["powerbi", "generic"],
    "description": "Target platform for schema",
    "default": "powerbi"
  }
}
```
**Returns**: Recommended schema with:
- Data types optimized for target
- Column renames (if needed)
- Suggested measures
- Relationship candidates

---

#### 01_Find_Relationships
**Purpose**: Detect potential relationships between multiple files
**When to use**: Building a star schema from multiple files
**Parameters**:
```json
{
  "paths": {
    "type": "array",
    "items": {"type": "string"},
    "description": "List of file paths to analyze",
    "required": true
  },
  "confidence_threshold": {
    "type": "number",
    "description": "Minimum confidence for relationship detection (0-1)",
    "default": 0.7
  }
}
```
**Returns**: Detected relationships with:
- Source and target tables/columns
- Relationship type (1:1, 1:N, N:N)
- Confidence score
- Sample matching values
- Recommended star schema design

---

### 5.3 Generation Tools (Category 02)

#### 02_Generate_Dimension
**Purpose**: Generate a dimension table with realistic data
**When to use**: Creating dimension tables for testing
**Parameters**:
```json
{
  "name": {
    "type": "string",
    "description": "Table name (e.g., 'dim_customer')",
    "required": true
  },
  "columns": {
    "type": "array",
    "description": "Column definitions",
    "items": {
      "type": "object",
      "properties": {
        "name": {"type": "string"},
        "type": {"type": "string", "enum": ["string", "integer", "decimal", "date", "boolean"]},
        "generator": {"type": "string", "description": "Faker/mimesis generator or pattern"},
        "unique": {"type": "boolean", "default": false},
        "nullable": {"type": "boolean", "default": false},
        "values": {"type": "array", "description": "Fixed list of values to choose from"}
      }
    },
    "required": true
  },
  "row_count": {
    "type": "integer",
    "description": "Number of rows to generate",
    "required": true
  },
  "output_path": {
    "type": "string",
    "description": "Output file path (optional)"
  },
  "output_format": {
    "type": "string",
    "enum": ["csv", "parquet"],
    "default": "csv"
  },
  "locale": {
    "type": "string",
    "description": "Locale for data generation (e.g., 'en_US', 'nl_NL')",
    "default": "en_US"
  }
}
```
**Returns**: Generated dimension table with summary statistics

---

#### 02_Generate_Fact
**Purpose**: Generate a fact table linked to dimensions
**When to use**: Creating fact tables with proper foreign keys
**Parameters**:
```json
{
  "name": {
    "type": "string",
    "description": "Table name (e.g., 'fact_sales')",
    "required": true
  },
  "grain": {
    "type": "array",
    "items": {"type": "string"},
    "description": "Grain columns (foreign keys)",
    "required": true
  },
  "measures": {
    "type": "array",
    "description": "Measure column definitions",
    "items": {
      "type": "object",
      "properties": {
        "name": {"type": "string"},
        "type": {"type": "string", "enum": ["integer", "decimal"]},
        "distribution": {"type": "string", "enum": ["uniform", "normal", "lognormal", "exponential"]},
        "min": {"type": "number"},
        "max": {"type": "number"},
        "mean": {"type": "number"},
        "std": {"type": "number"}
      }
    },
    "required": true
  },
  "dimensions": {
    "type": "array",
    "description": "Dimension references",
    "items": {
      "type": "object",
      "properties": {
        "path": {"type": "string", "description": "Path to dimension file"},
        "key_column": {"type": "string", "description": "Column to use as foreign key"},
        "weight_column": {"type": "string", "description": "Column to weight selection (optional)"}
      }
    },
    "required": true
  },
  "row_count": {
    "type": "integer",
    "required": true
  },
  "output_path": {"type": "string"},
  "output_format": {
    "type": "string",
    "enum": ["csv", "parquet"],
    "default": "csv"
  }
}
```
**Returns**: Generated fact table with referential integrity guaranteed

---

#### 02_Generate_From_Template
**Purpose**: Generate data from predefined domain templates
**When to use**: Quick generation using built-in templates
**Parameters**:
```json
{
  "template": {
    "type": "string",
    "description": "Template name (e.g., 'financial/trial_balance', 'sales/fact_sales')",
    "required": true
  },
  "row_count": {
    "type": "integer",
    "description": "Number of rows",
    "required": true
  },
  "overrides": {
    "type": "object",
    "description": "Override template defaults"
  },
  "output_path": {"type": "string"},
  "output_format": {
    "type": "string",
    "enum": ["csv", "parquet"],
    "default": "csv"
  }
}
```
**Returns**: Generated data matching template schema

---

#### 02_Generate_Date_Dimension
**Purpose**: Generate a standard date dimension table
**When to use**: Every Power BI model needs a date table
**Parameters**:
```json
{
  "start_date": {
    "type": "string",
    "description": "Start date (YYYY-MM-DD)",
    "required": true
  },
  "end_date": {
    "type": "string",
    "description": "End date (YYYY-MM-DD)",
    "required": true
  },
  "fiscal_year_start_month": {
    "type": "integer",
    "description": "Fiscal year start month (1-12)",
    "default": 1
  },
  "include_holidays": {
    "type": "boolean",
    "description": "Include holiday flags",
    "default": false
  },
  "holiday_country": {
    "type": "string",
    "description": "Country code for holidays (e.g., 'US', 'NL')",
    "default": "US"
  },
  "output_path": {"type": "string"},
  "output_format": {
    "type": "string",
    "enum": ["csv", "parquet"],
    "default": "csv"
  }
}
```
**Returns**: Complete date dimension with:
- Date key (YYYYMMDD integer)
- Full date
- Year, Quarter, Month, Week, Day
- Month name, Day name
- Fiscal Year, Fiscal Quarter, Fiscal Month
- Is Weekend, Is Holiday
- Week of Year, Day of Year

---

#### 02_Fill_Column
**Purpose**: Fill missing values or generate a new column
**When to use**: Adding calculated or generated columns to existing data
**Parameters**:
```json
{
  "path": {
    "type": "string",
    "description": "Path to data file",
    "required": true
  },
  "column": {
    "type": "string",
    "description": "Column name to fill or create",
    "required": true
  },
  "strategy": {
    "type": "string",
    "enum": ["faker", "distribution", "formula", "lookup", "constant"],
    "description": "Fill strategy",
    "required": true
  },
  "config": {
    "type": "object",
    "description": "Strategy-specific configuration",
    "required": true
  },
  "output_path": {"type": "string"}
}
```
**Returns**: Updated data file with filled column

---

#### 02_Generate_Related_Records
**Purpose**: Generate records that relate to existing data
**When to use**: Extending existing dimension or fact data
**Parameters**:
```json
{
  "source_path": {
    "type": "string",
    "description": "Path to source data",
    "required": true
  },
  "key_column": {
    "type": "string",
    "description": "Column to use as relationship key",
    "required": true
  },
  "records_per_key": {
    "type": "object",
    "properties": {
      "min": {"type": "integer"},
      "max": {"type": "integer"},
      "distribution": {"type": "string", "enum": ["uniform", "poisson"]}
    },
    "required": true
  },
  "columns": {
    "type": "array",
    "description": "Column definitions for generated records"
  },
  "output_path": {"type": "string"},
  "output_format": {
    "type": "string",
    "enum": ["csv", "parquet"],
    "default": "csv"
  }
}
```
**Returns**: Generated related records with proper foreign keys

---

### 5.4 Editing Tools (Category 03)

#### 03_Query_Data
**Purpose**: Run SQL queries on data files
**When to use**: Filtering, aggregating, or joining data
**Parameters**:
```json
{
  "query": {
    "type": "string",
    "description": "SQL query (DuckDB syntax). Use 'data' as table name for single file, or file paths as table names",
    "required": true
  },
  "files": {
    "type": "object",
    "description": "Map of table aliases to file paths",
    "additionalProperties": {"type": "string"}
  },
  "limit": {
    "type": "integer",
    "description": "Maximum rows to return",
    "default": 1000
  },
  "output_path": {
    "type": "string",
    "description": "Save results to file (optional)"
  }
}
```
**Example**:
```json
{
  "query": "SELECT c.customer_name, SUM(s.amount) as total FROM sales s JOIN customers c ON s.customer_id = c.id GROUP BY c.customer_name ORDER BY total DESC",
  "files": {
    "sales": "C:/data/fact_sales.csv",
    "customers": "C:/data/dim_customers.csv"
  }
}
```
**Returns**: Query results with execution statistics

---

#### 03_Update_Where
**Purpose**: Update rows matching a condition
**When to use**: Bulk data corrections
**Parameters**:
```json
{
  "path": {
    "type": "string",
    "description": "Path to data file",
    "required": true
  },
  "updates": {
    "type": "object",
    "description": "Column updates (column_name: new_value or SQL expression)",
    "required": true
  },
  "where": {
    "type": "string",
    "description": "SQL WHERE clause condition",
    "required": true
  },
  "output_path": {
    "type": "string",
    "description": "Output path (defaults to overwrite)"
  },
  "dry_run": {
    "type": "boolean",
    "description": "Preview changes without applying",
    "default": true
  }
}
```
**Returns**: Update summary with affected row count

---

#### 03_Delete_Where
**Purpose**: Delete rows matching a condition
**When to use**: Removing invalid or test data
**Parameters**:
```json
{
  "path": {
    "type": "string",
    "required": true
  },
  "where": {
    "type": "string",
    "description": "SQL WHERE clause",
    "required": true
  },
  "output_path": {"type": "string"},
  "dry_run": {
    "type": "boolean",
    "default": true
  }
}
```
**Returns**: Deletion summary

---

#### 03_Transform_Column
**Purpose**: Apply transformations to a column
**When to use**: Data cleaning, type conversion, formatting
**Parameters**:
```json
{
  "path": {
    "type": "string",
    "required": true
  },
  "column": {
    "type": "string",
    "required": true
  },
  "transformation": {
    "type": "string",
    "enum": ["upper", "lower", "trim", "round", "abs", "cast", "replace", "extract", "custom_sql"],
    "required": true
  },
  "config": {
    "type": "object",
    "description": "Transformation-specific config"
  },
  "output_path": {"type": "string"}
}
```
**Returns**: Transformed data with preview

---

#### 03_Add_Column
**Purpose**: Add a calculated or derived column
**When to use**: Creating new columns based on existing data
**Parameters**:
```json
{
  "path": {
    "type": "string",
    "required": true
  },
  "column": {
    "type": "string",
    "description": "New column name",
    "required": true
  },
  "expression": {
    "type": "string",
    "description": "SQL expression for the column value",
    "required": true
  },
  "output_path": {"type": "string"}
}
```
**Example**:
```json
{
  "path": "C:/data/sales.csv",
  "column": "profit_margin",
  "expression": "(revenue - cost) / revenue * 100"
}
```

---

#### 03_Merge_Tables
**Purpose**: Join or union multiple tables
**When to use**: Combining data from multiple sources
**Parameters**:
```json
{
  "operation": {
    "type": "string",
    "enum": ["join", "union", "union_all"],
    "required": true
  },
  "tables": {
    "type": "array",
    "items": {
      "type": "object",
      "properties": {
        "path": {"type": "string"},
        "alias": {"type": "string"}
      }
    },
    "required": true
  },
  "join_config": {
    "type": "object",
    "properties": {
      "type": {"type": "string", "enum": ["inner", "left", "right", "full"]},
      "on": {"type": "string", "description": "Join condition"}
    }
  },
  "output_path": {"type": "string"},
  "output_format": {
    "type": "string",
    "enum": ["csv", "parquet"],
    "default": "csv"
  }
}
```

---

### 5.5 Validation Tools (Category 04)

#### 04_Validate_Schema
**Purpose**: Validate data against a schema definition
**When to use**: Ensuring data conforms to expected structure
**Parameters**:
```json
{
  "path": {
    "type": "string",
    "required": true
  },
  "schema": {
    "type": "object",
    "description": "Schema definition with column types and constraints",
    "required": true
  },
  "strict": {
    "type": "boolean",
    "description": "Fail on any violation vs collect all",
    "default": false
  }
}
```
**Returns**: Validation report with violations

---

#### 04_Validate_Rules
**Purpose**: Apply custom validation rules
**When to use**: Business rule validation
**Parameters**:
```json
{
  "path": {
    "type": "string",
    "required": true
  },
  "rules": {
    "type": "array",
    "items": {
      "type": "object",
      "properties": {
        "name": {"type": "string"},
        "type": {"type": "string", "enum": ["not_null", "unique", "range", "regex", "custom_sql", "referential"]},
        "columns": {"type": "array", "items": {"type": "string"}},
        "config": {"type": "object"}
      }
    },
    "required": true
  }
}
```
**Returns**: Rule validation results

---

#### 04_Check_Balance
**Purpose**: Validate financial balance rules (debit = credit)
**When to use**: Financial data validation
**Parameters**:
```json
{
  "path": {
    "type": "string",
    "required": true
  },
  "debit_column": {
    "type": "string",
    "description": "Column containing debit amounts",
    "required": true
  },
  "credit_column": {
    "type": "string",
    "description": "Column containing credit amounts",
    "required": true
  },
  "group_by": {
    "type": "array",
    "items": {"type": "string"},
    "description": "Columns to group balance check by (e.g., period, entity)"
  },
  "tolerance": {
    "type": "number",
    "description": "Acceptable imbalance tolerance",
    "default": 0.01
  }
}
```
**Returns**: Balance check results with imbalanced groups

---

#### 04_Check_Referential_Integrity
**Purpose**: Validate foreign key relationships
**When to use**: Ensuring fact-dimension relationships are valid
**Parameters**:
```json
{
  "fact_path": {
    "type": "string",
    "required": true
  },
  "dimension_path": {
    "type": "string",
    "required": true
  },
  "fact_key": {
    "type": "string",
    "description": "Foreign key column in fact table",
    "required": true
  },
  "dimension_key": {
    "type": "string",
    "description": "Primary key column in dimension table",
    "required": true
  }
}
```
**Returns**: Orphan records and integrity violations

---

#### 04_Check_Totals
**Purpose**: Validate that measures sum correctly across groups
**When to use**: Ensuring data consistency across hierarchies
**Parameters**:
```json
{
  "path": {
    "type": "string",
    "required": true
  },
  "measure_column": {
    "type": "string",
    "required": true
  },
  "group_columns": {
    "type": "array",
    "items": {"type": "string"},
    "description": "Hierarchy columns (e.g., ['year', 'quarter', 'month'])"
  },
  "expected_total": {
    "type": "number",
    "description": "Optional expected grand total"
  }
}
```
**Returns**: Total validation with hierarchical breakdown

---

### 5.6 Export Tools (Category 05)

#### 05_Export_CSV
**Purpose**: Export data as CSV
**Parameters**:
```json
{
  "path": {
    "type": "string",
    "description": "Source file or query result",
    "required": true
  },
  "output_path": {
    "type": "string",
    "required": true
  },
  "delimiter": {
    "type": "string",
    "default": ","
  },
  "include_header": {
    "type": "boolean",
    "default": true
  },
  "encoding": {
    "type": "string",
    "default": "utf-8"
  },
  "date_format": {
    "type": "string",
    "default": "%Y-%m-%d"
  }
}
```

---

#### 05_Export_Parquet
**Purpose**: Export data as Parquet (optimized for Power BI)
**Parameters**:
```json
{
  "path": {
    "type": "string",
    "required": true
  },
  "output_path": {
    "type": "string",
    "required": true
  },
  "compression": {
    "type": "string",
    "enum": ["snappy", "gzip", "zstd", "none"],
    "default": "snappy"
  },
  "row_group_size": {
    "type": "integer",
    "description": "Rows per row group",
    "default": 100000
  }
}
```

---

#### 05_Export_PowerBI_Package
**Purpose**: Export optimized package for Power BI import
**Parameters**:
```json
{
  "tables": {
    "type": "array",
    "items": {
      "type": "object",
      "properties": {
        "path": {"type": "string"},
        "name": {"type": "string"},
        "type": {"type": "string", "enum": ["fact", "dimension"]}
      }
    },
    "required": true
  },
  "output_dir": {
    "type": "string",
    "required": true
  },
  "format": {
    "type": "string",
    "enum": ["csv", "parquet"],
    "default": "parquet"
  },
  "include_schema": {
    "type": "boolean",
    "description": "Include schema.json for reference",
    "default": true
  },
  "include_relationships": {
    "type": "boolean",
    "description": "Include detected relationships",
    "default": true
  }
}
```
**Returns**: Package with optimized files and relationship metadata

---

### 5.7 Project Tools (Category 06)

#### 06_Create_Project
**Purpose**: Create a new sample data project
**Parameters**:
```json
{
  "name": {
    "type": "string",
    "required": true
  },
  "description": {
    "type": "string"
  },
  "domain": {
    "type": "string",
    "enum": ["financial", "sales", "inventory", "hr", "generic"],
    "default": "generic"
  }
}
```

---

#### 06_List_Projects
**Purpose**: List all projects

---

#### 06_Get_Project
**Purpose**: Get project details
**Parameters**:
```json
{
  "name": {
    "type": "string",
    "required": true
  }
}
```

---

#### 06_Delete_Project
**Purpose**: Delete a project and its data
**Parameters**:
```json
{
  "name": {
    "type": "string",
    "required": true
  },
  "confirm": {
    "type": "boolean",
    "description": "Must be true to delete",
    "required": true
  }
}
```

---

### 5.8 Help Tools (Category 07)

#### 07_Show_User_Guide
**Purpose**: Display comprehensive user guide

---

#### 07_List_Templates
**Purpose**: List available generation templates
**Parameters**:
```json
{
  "domain": {
    "type": "string",
    "enum": ["all", "financial", "sales", "inventory", "hr"],
    "default": "all"
  }
}
```
**Returns**: Available templates with descriptions

---

### 5.9 Mapping Tools (Category 08) - Multi-File Relationships

> **NEW SECTION**: These tools enable understanding and validating data across multiple related files (e.g., fact data + mapping tables + report structure).

#### 08_Discover_Mapping
**Purpose**: Automatically discover relationships between columns across multiple files
**When to use**: When you have source data and mapping/lookup tables and need to understand how they connect
**Key Capability**: Fuzzy/semantic matching to find relationships like `GL Typology="Stocks"` → `Nominator element="Stocks and contracts in progress"`
**Parameters**:
```json
{
  "files": {
    "type": "array",
    "items": {"type": "string"},
    "description": "List of file paths to analyze together",
    "required": true
  },
  "source_file": {
    "type": "string",
    "description": "Primary source data file (e.g., fact table)",
    "required": true
  },
  "match_threshold": {
    "type": "number",
    "description": "Fuzzy match threshold 0.0-1.0",
    "default": 0.7
  },
  "detect_hierarchies": {
    "type": "boolean",
    "description": "Look for hierarchical structures in mapping tables",
    "default": true
  }
}
```
**Returns**:
```json
{
  "discovered_mappings": [
    {
      "source_file": "balance_data.xlsx",
      "source_column": "GL Typology",
      "target_file": "s_report_lines.xlsx",
      "target_column": "Nominator element",
      "match_type": "fuzzy",
      "match_confidence": 0.85,
      "sample_matches": [
        {"source_value": "Stocks", "target_value": "Stocks and contracts in progress", "score": 0.78},
        {"source_value": "Cash at bank and in hand", "target_value": "Cash at bank and in hand", "score": 1.0}
      ]
    }
  ],
  "hierarchies_found": [
    {
      "file": "s_report_lines.xlsx",
      "hierarchy_columns": ["Line Name", "Nominator element"],
      "levels": 3,
      "aggregation_lines": ["Total Assets", "Total Equity", "Total Liabilities"]
    }
  ],
  "suggested_join_paths": [
    {
      "path": ["GL Typology → Nominator element → Total rollup"],
      "files_involved": ["balance_data.xlsx", "s_report_lines.xlsx"]
    }
  ]
}
```

---

#### 08_Define_Mapping
**Purpose**: Create or edit explicit mapping definitions between files
**When to use**: When automatic discovery needs refinement or manual mapping is required
**Parameters**:
```json
{
  "mapping_name": {
    "type": "string",
    "description": "Unique name for this mapping definition",
    "required": true
  },
  "source_file": {
    "type": "string",
    "description": "Path to source data file",
    "required": true
  },
  "source_column": {
    "type": "string",
    "description": "Column name in source file",
    "required": true
  },
  "target_file": {
    "type": "string",
    "description": "Path to target/mapping file",
    "required": true
  },
  "target_column": {
    "type": "string",
    "description": "Column name in target file",
    "required": true
  },
  "explicit_mappings": {
    "type": "object",
    "description": "Manual source→target value mappings",
    "additionalProperties": {"type": "string"},
    "default": {}
  },
  "hierarchy_config": {
    "type": "object",
    "description": "Hierarchy configuration for rollup calculations",
    "properties": {
      "level_columns": {"type": "array", "items": {"type": "string"}},
      "row_number_columns": {"type": "array", "items": {"type": "string"}},
      "operator_column": {"type": "string"},
      "multiplicator_column": {"type": "string"}
    }
  }
}
```
**Returns**: Mapping definition saved with ID

---

#### 08_Load_Mapping_Context
**Purpose**: Load multiple files as a unified context for analysis
**When to use**: Before running cross-file validation or analysis
**Key Capability**: Treats multiple files as a single logical dataset
**Parameters**:
```json
{
  "files": {
    "type": "array",
    "items": {
      "type": "object",
      "properties": {
        "path": {"type": "string"},
        "role": {"type": "string", "enum": ["source_data", "mapping", "hierarchy", "report_structure"]},
        "alias": {"type": "string"}
      }
    },
    "description": "Files to load with their roles",
    "required": true
  },
  "context_name": {
    "type": "string",
    "description": "Name for this context (for reuse)",
    "required": true
  }
}
```
**Returns**: Context ID and summary of loaded files

---

#### 08_Validate_Through_Mapping
**Purpose**: Validate source data against rules defined through mapping hierarchy
**When to use**: Validate that balance sheet balances (Assets = Liabilities + Equity) using mapping tables
**Key Capability**: Follows the mapping chain from detail data → mapping → report totals → validation rule
**Parameters**:
```json
{
  "context_name": {
    "type": "string",
    "description": "Previously loaded context name",
    "required": true
  },
  "validation_rule": {
    "type": "string",
    "enum": ["balance_sheet_equation", "sum_to_parent", "debit_credit_balance", "custom"],
    "description": "Predefined rule or custom",
    "required": true
  },
  "source_amount_column": {
    "type": "string",
    "description": "Column containing amounts in source data",
    "required": true
  },
  "source_category_column": {
    "type": "string",
    "description": "Column to map through hierarchy (e.g., GL Typology)",
    "required": true
  },
  "filter": {
    "type": "object",
    "description": "Optional filters (e.g., {\"Posting Period Seqnr\": 24288})"
  },
  "custom_equation": {
    "type": "string",
    "description": "For custom rules: e.g., 'Total Assets = Total Equity + Total Liabilities'"
  }
}
```
**Returns**:
```json
{
  "validation_passed": false,
  "rule_applied": "balance_sheet_equation",
  "details": {
    "total_assets": 616337.04,
    "total_equity": 293729.34,
    "total_liabilities": 3666345.24,
    "expected_balance": 3960074.58,
    "difference": -3343737.54
  },
  "unmapped_values": [
    {"value": "Untaxed reserves", "amount": 1036981.73, "classification": "unknown"}
  ],
  "mapping_coverage": {
    "mapped_rows": 8433,
    "unmapped_rows": 1000,
    "coverage_pct": 89.4
  },
  "breakdown_by_category": [
    {"category": "Asset", "total": 616337.04, "line_items": 12},
    {"category": "Liability", "total": 3666345.24, "line_items": 8},
    {"category": "Equity", "total": 293729.34, "line_items": 3},
    {"category": "Unknown", "total": 1036981.73, "line_items": 2}
  ]
}
```

---

#### 08_Rollup_Through_Hierarchy
**Purpose**: Aggregate source data through a hierarchical mapping structure
**When to use**: Calculate totals like EBITDA from detail lines using formula hierarchy
**Parameters**:
```json
{
  "source_file": {
    "type": "string",
    "description": "Path to source data file",
    "required": true
  },
  "formula_file": {
    "type": "string",
    "description": "Path to formula/hierarchy file",
    "required": true
  },
  "amount_column": {
    "type": "string",
    "description": "Column with values to aggregate",
    "required": true
  },
  "source_mapping_column": {
    "type": "string",
    "description": "Column to join source to formula elements",
    "required": true
  },
  "target_rollup": {
    "type": "string",
    "description": "Target total to calculate (e.g., 'EBITDA', 'Total Assets')",
    "required": true
  },
  "show_detail": {
    "type": "boolean",
    "description": "Show contribution of each element",
    "default": true
  }
}
```
**Returns**: Hierarchical rollup with contribution breakdown

---

#### 08_Compare_Structures
**Purpose**: Compare structure of source data vs expected report format
**When to use**: Verify all required report lines have corresponding source data
**Parameters**:
```json
{
  "source_file": {
    "type": "string",
    "description": "Source data file",
    "required": true
  },
  "report_structure_file": {
    "type": "string",
    "description": "Report definition file",
    "required": true
  },
  "source_column": {
    "type": "string",
    "description": "Column with categories in source"
  },
  "report_line_column": {
    "type": "string",
    "description": "Column with line items in report structure"
  }
}
```
**Returns**: Gap analysis showing missing/extra items

---

## 6. Domain Knowledge System

### 6.1 Domain Prompts Structure

Domain prompts are markdown files exposed as MCP resources. The LLM can read these to understand domain-specific rules.

---

### 6.2 Financial Domain Prompts

#### domain_prompts/financial/balance_sheet.md

```markdown
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
```

---

#### domain_prompts/financial/income_statement.md

```markdown
# Income Statement Domain Knowledge

## Structure

### Basic P&L Structure
```
Revenue (Sales)
- Cost of Goods Sold (COGS)
= Gross Profit

- Operating Expenses
  - Selling Expenses
  - General & Administrative
  - Research & Development
= Operating Income (EBIT)

+/- Other Income/Expenses
- Interest Expense
= Income Before Tax

- Income Tax Expense
= Net Income
```

## Key Metrics & Validations

### Margin Calculations
- **Gross Margin** = Gross Profit / Revenue
- **Operating Margin** = Operating Income / Revenue
- **Net Margin** = Net Income / Revenue

### Validation Rules
1. Revenue should be positive (unless adjustments)
2. COGS should be positive and < Revenue
3. Gross Profit = Revenue - COGS
4. Operating Income = Gross Profit - Operating Expenses
5. Expenses should be categorized consistently

## Common Account Mappings

| P&L Line | Account Code Range |
|----------|-------------------|
| Revenue | 4000-4999 |
| COGS | 5000-5999 |
| Operating Expenses | 6000-6999 |
| Other Income/Expenses | 7000-7999 |
| Taxes | 8000-8999 |

## Data Generation Considerations

1. **Seasonality**: Revenue often varies by quarter/month
2. **Expense ratios**: Maintain realistic expense-to-revenue ratios
3. **Trend**: Consider year-over-year growth patterns
4. **Variance**: Include some variance/noise in the data
```

---

#### domain_prompts/financial/general_ledger.md

```markdown
# General Ledger Domain Knowledge

## GL Entry Structure

Every GL entry consists of:
1. **Header**: Transaction-level information
2. **Lines**: Individual debit/credit postings

### Journal Entry Rules
- Minimum 2 lines per entry
- Sum of debits = Sum of credits (always)
- At least one debit and one credit line

## Entry Types

### Standard Entry Types
| Type | Code | Description |
|------|------|-------------|
| Journal Entry | JE | Manual adjustments |
| Cash Receipt | CR | Customer payments |
| Cash Disbursement | CD | Vendor payments |
| Sales Invoice | SI | Revenue recognition |
| Purchase Invoice | PI | Expense/liability |
| Payroll | PR | Payroll entries |
| Depreciation | DP | Asset depreciation |
| Accrual | AC | Period-end accruals |
| Reversal | RV | Reversal entries |

## Posting Rules

### Auto-Balance Rules
- System should reject unbalanced entries
- Tolerance typically 0.01 for rounding

### Period Control
- Entries cannot post to closed periods
- Year-end entries may have special handling

## Sample GL Transaction Generation

```python
# Pseudo-code for balanced transaction generation
def generate_balanced_entry(account_pairs, amount):
    """
    Generate a balanced GL entry.
    account_pairs: [(debit_account, credit_account, ratio), ...]
    """
    lines = []
    for debit_acct, credit_acct, ratio in account_pairs:
        line_amount = amount * ratio
        lines.append({'account': debit_acct, 'debit': line_amount, 'credit': 0})
        lines.append({'account': credit_acct, 'debit': 0, 'credit': line_amount})
    return lines
```

## Reconciliation Points

1. **GL to Sub-ledger**: GL balances should match sub-ledger totals
2. **GL to Trial Balance**: TB is a summary of GL
3. **GL to Financial Statements**: FS derived from GL
```

---

#### domain_prompts/financial/chart_of_accounts.md

```markdown
# Chart of Accounts Domain Knowledge

## CoA Design Principles

### Segment Structure
Many organizations use segmented account strings:
```
Company-Department-Account-Product-Project
01-100-4000-001-0000
```

### Best Practices
1. **Consistent length**: All account codes same format
2. **Logical grouping**: Related accounts grouped together
3. **Room for growth**: Leave gaps for new accounts
4. **Meaningful codes**: Codes should be somewhat intuitive

## Standard CoA Templates

### Small Business CoA
```
1000-1999: Assets
  1000-1099: Current Assets
    1000: Checking Account
    1010: Savings Account
    1020: Petty Cash
    1100: Accounts Receivable
    1200: Inventory
  1500-1599: Fixed Assets
    1500: Equipment
    1510: Accumulated Depreciation
2000-2999: Liabilities
  2000-2099: Current Liabilities
    2000: Accounts Payable
    2100: Accrued Expenses
    2200: Payroll Liabilities
  2500-2599: Long-term Liabilities
    2500: Notes Payable
3000-3999: Equity
  3000: Owner's Equity
  3100: Retained Earnings
4000-4999: Revenue
  4000: Sales Revenue
  4100: Service Revenue
  4200: Other Income
5000-5999: COGS
  5000: Cost of Goods Sold
  5100: Direct Labor
  5200: Direct Materials
6000-6999: Operating Expenses
  6000: Salaries & Wages
  6100: Rent Expense
  6200: Utilities
  6300: Insurance
  6400: Depreciation
  6500: Office Supplies
  6600: Professional Fees
```

## CoA Dimension Table Structure

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| account_id | INT | Yes | Surrogate key |
| account_code | VARCHAR(20) | Yes | Natural key |
| account_name | VARCHAR(100) | Yes | Display name |
| account_type | VARCHAR(20) | Yes | Asset/Liability/Equity/Revenue/Expense |
| account_category | VARCHAR(50) | Yes | Sub-classification |
| account_group | VARCHAR(50) | No | Grouping for reporting |
| parent_account_code | VARCHAR(20) | No | For hierarchy |
| level | INT | No | Hierarchy level |
| is_active | BOOLEAN | Yes | Active flag |
| is_posting | BOOLEAN | Yes | Can post transactions |
| normal_balance | VARCHAR(10) | Yes | Debit/Credit |
| created_date | DATE | No | When created |

## Validation Rules

1. **Unique codes**: No duplicate account codes
2. **Valid parent**: Parent account must exist (if specified)
3. **Type consistency**: Child must have same type as parent
4. **Active hierarchy**: Inactive account can't have active children
5. **Posting only at leaves**: Only leaf accounts should be posting accounts
```

---

### 6.3 Sales Domain Prompts

#### domain_prompts/sales/orders.md

```markdown
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
```

---

#### domain_prompts/sales/customers.md

```markdown
# Customer Dimension Knowledge

## Customer Segmentation

### Common Segments
- **Enterprise**: Large corporations
- **Mid-Market**: Medium-sized businesses
- **SMB**: Small and medium businesses
- **Consumer**: Individual consumers

### RFM Segmentation
- **Recency**: Days since last purchase
- **Frequency**: Number of purchases
- **Monetary**: Total spend

## Customer Dimension Structure

| Column | Type | Description |
|--------|------|-------------|
| customer_id | INT | Surrogate key |
| customer_key | VARCHAR | Natural key |
| customer_name | VARCHAR | Full name |
| segment | VARCHAR | Customer segment |
| industry | VARCHAR | Industry vertical |
| region | VARCHAR | Geographic region |
| country | VARCHAR | Country |
| city | VARCHAR | City |
| postal_code | VARCHAR | Postal/ZIP code |
| customer_since | DATE | First purchase date |
| tier | VARCHAR | Loyalty tier |
| credit_limit | DECIMAL | Credit limit |
| account_manager | VARCHAR | Assigned manager |
| is_active | BOOLEAN | Active flag |

## SCD Type 2 Considerations

For slowly changing dimensions:
- effective_date, expiration_date
- is_current flag
- version number

## Generation Guidelines

1. **Geographic distribution**: Realistic by region/country
2. **Industry mix**: Varies by segment
3. **Age distribution**: Customer tenure varies
4. **Active ratio**: 70-80% typically active
```

---

#### domain_prompts/sales/products.md

```markdown
# Product Dimension Knowledge

## Product Hierarchy

```
Category
└── Subcategory
    └── Brand
        └── Product
            └── SKU
```

## Product Dimension Structure

| Column | Type | Description |
|--------|------|-------------|
| product_id | INT | Surrogate key |
| product_key | VARCHAR | SKU/Item number |
| product_name | VARCHAR | Display name |
| description | VARCHAR | Long description |
| category | VARCHAR | Top-level category |
| subcategory | VARCHAR | Sub-category |
| brand | VARCHAR | Brand name |
| unit_cost | DECIMAL | Cost to acquire |
| list_price | DECIMAL | Standard price |
| is_active | BOOLEAN | Currently sold |
| launch_date | DATE | When launched |
| discontinue_date | DATE | When discontinued |
| weight | DECIMAL | Unit weight |
| size | VARCHAR | Size/dimensions |

## Product Metrics

- **Margin** = (List Price - Unit Cost) / List Price
- **Price Point**: Budget, Mid-range, Premium
- **Velocity**: Fast-moving, Slow-moving

## Generation Guidelines

1. **Price ranges**: Vary by category
2. **Margin ranges**: Realistic by category (10-60%)
3. **Active ratio**: 60-80% active products
4. **Category distribution**: Pareto - few categories dominate
```

---

### 6.4 Inventory Domain Prompts

#### domain_prompts/inventory/stock_levels.md

```markdown
# Inventory Stock Levels Knowledge

## Stock Movement Types

| Type | Code | Effect on Qty |
|------|------|---------------|
| Receipt | REC | + |
| Sale | SAL | - |
| Return | RET | + |
| Adjustment + | ADJ+ | + |
| Adjustment - | ADJ- | - |
| Transfer In | TRF+ | + |
| Transfer Out | TRF- | - |
| Write-off | WRO | - |

## Stock Fact Table

| Column | Type | Description |
|--------|------|-------------|
| movement_id | INT | Primary key |
| movement_date | DATE | When occurred |
| product_id | INT | FK to product |
| warehouse_id | INT | FK to warehouse |
| movement_type | VARCHAR | Type code |
| quantity | INT | Units moved |
| unit_cost | DECIMAL | Cost per unit |
| total_cost | DECIMAL | Extended cost |
| reference | VARCHAR | Source document |

## Stock Snapshot Fact Table

| Column | Type | Description |
|--------|------|-------------|
| snapshot_date | DATE | As-of date |
| product_id | INT | FK to product |
| warehouse_id | INT | FK to warehouse |
| quantity_on_hand | INT | Current stock |
| quantity_reserved | INT | Reserved for orders |
| quantity_available | INT | Available to sell |
| unit_cost | DECIMAL | Average cost |
| total_value | DECIMAL | Inventory value |

## Validation Rules

1. **Non-negative stock**: Quantity on hand >= 0 (usually)
2. **Balance check**: Opening + Receipts - Issues = Closing
3. **Valuation consistency**: Total value = Qty * Unit cost
4. **Reserved <= On hand**: Can't reserve more than available

## Generation Tips

1. **Stock levels**: Follow realistic distribution (some items high, most low)
2. **Movement frequency**: Fast movers have more transactions
3. **Seasonality**: Stock builds before peak seasons
4. **Aging**: Some stock sits longer (slow movers)
```

---

### 6.5 HR Domain Prompts

#### domain_prompts/hr/employees.md

```markdown
# Employee Dimension Knowledge

## Employee Hierarchy

```
Company
└── Division
    └── Department
        └── Team
            └── Employee
```

## Employee Dimension Structure

| Column | Type | Description |
|--------|------|-------------|
| employee_id | INT | Surrogate key |
| employee_number | VARCHAR | Badge/ID number |
| first_name | VARCHAR | First name |
| last_name | VARCHAR | Last name |
| full_name | VARCHAR | Display name |
| email | VARCHAR | Email address |
| hire_date | DATE | Start date |
| termination_date | DATE | End date (null if active) |
| department_id | INT | FK to department |
| job_title | VARCHAR | Current title |
| job_level | INT | Level/grade |
| manager_id | INT | FK to manager (self-ref) |
| employment_type | VARCHAR | Full-time/Part-time/Contract |
| location | VARCHAR | Work location |
| salary | DECIMAL | Annual salary |
| is_active | BOOLEAN | Currently employed |

## Key Metrics

- **Headcount**: Active employees at a point in time
- **FTE**: Full-time equivalent
- **Turnover**: Terminations / Average headcount
- **Tenure**: Years of service

## Validation Rules

1. **Hire date**: Must exist, not in future
2. **Termination date**: Null or >= hire date
3. **Manager hierarchy**: No circular references
4. **Salary range**: Within job level bands
5. **Active flag consistency**: Active if no termination date

## Generation Guidelines

1. **Department sizes**: Vary (3-50 people typical)
2. **Span of control**: Managers have 5-10 direct reports
3. **Tenure distribution**: Mix of new and long-tenured
4. **Salary distribution**: Normal within level, increases with level
5. **Turnover rate**: 10-20% annually typical
```

---

### 6.6 Generic Domain Prompts

#### domain_prompts/generic/star_schema.md

```markdown
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
```

---

#### domain_prompts/generic/data_quality.md

```markdown
# Data Quality Knowledge

## Data Quality Dimensions

### Accuracy
- Data correctly represents reality
- Values are within valid ranges
- No typos or errors

### Completeness
- All required fields populated
- No missing records
- Full coverage of expected scope

### Consistency
- Data agrees with itself
- Same values across systems
- Business rules satisfied

### Timeliness
- Data is current/recent enough
- Updates happen as expected
- Historical data preserved correctly

### Uniqueness
- No duplicate records
- Primary keys are unique
- Business keys don't conflict

### Validity
- Data conforms to format rules
- Values match expected patterns
- Referential integrity maintained

## Common Quality Issues

### Null Values
- Missing required data
- Null vs empty string confusion
- Default value usage

### Duplicates
- Same entity multiple times
- Near-duplicates (fuzzy matches)
- Merge/purge issues

### Referential Integrity
- Orphan records in facts
- Missing dimension members
- Cascading update failures

### Format Issues
- Date format inconsistencies
- Number formatting (commas, decimals)
- Case sensitivity

## Quality Metrics

| Metric | Calculation |
|--------|-------------|
| Completeness Rate | (Non-null values / Total values) * 100 |
| Uniqueness Rate | (Unique values / Total rows) * 100 |
| Validity Rate | (Valid values / Total values) * 100 |
| Referential Integrity | (Matched FKs / Total FKs) * 100 |

## Remediation Strategies

1. **Standardization**: Apply consistent formats
2. **Deduplication**: Identify and merge duplicates
3. **Enrichment**: Fill missing values
4. **Validation**: Apply business rules
5. **Cleansing**: Fix known issues
```

---

## 7. Implementation Files

### 7.1 Main Server Entry Point

#### src/sample_data_server.py

```python
#!/usr/bin/env python3
"""
MCP-Sample-Data Server v1.0
LLM-Orchestrated Sample Data Platform for Power BI
"""

import asyncio
import json
import logging
import sys
import os
import time
from pathlib import Path
from typing import Any, List
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent, Resource

# Add parent directory to path
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from __version__ import __version__
from core.config.config_manager import config
from core.validation.error_handler import ErrorHandler
from core.storage.cache_manager import CacheManager

# Import handler registry system
from server.registry import get_registry
from server.dispatch import ToolDispatcher
from server.handlers import register_all_handlers
from server.resources import get_resource_manager

# Configure logging
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("mcp_sample_data")

# Configure file-based logging
try:
    logs_dir = os.path.join(parent_dir, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    LOG_PATH = os.path.join(logs_dir, "sample_data.log")

    from logging.handlers import MemoryHandler
    _fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
    _fh.setLevel(logging.WARNING)
    _fh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    _mh = MemoryHandler(capacity=100, flushLevel=logging.ERROR, target=_fh)
    logging.getLogger().addHandler(_mh)
    logger.warning("File logging enabled: %s", LOG_PATH)
except Exception as e:
    logger.warning("Could not set up file logging: %s", e)

# Track server start time
start_time = time.time()

# Initialize cache manager
cache_manager = CacheManager(config.get('cache', {}))

# Initialize handler registry and dispatcher
registry = get_registry()
register_all_handlers(registry)
dispatcher = ToolDispatcher()

# Initialize MCP server
app = Server("MCP-Sample-Data")


@app.list_tools()
async def list_tools() -> List[Tool]:
    """List all available tools from registry"""
    return registry.get_all_tools_as_mcp()


@app.list_resources()
async def list_resources() -> List[Resource]:
    """List domain knowledge prompts as resources"""
    try:
        resource_manager = get_resource_manager()
        return resource_manager.list_resources()
    except Exception as e:
        logger.error(f"Error listing resources: {e}", exc_info=True)
        return []


@app.read_resource()
async def read_resource(uri: str) -> str:
    """Read a domain knowledge prompt by URI"""
    try:
        resource_manager = get_resource_manager()
        content = resource_manager.read_resource(uri)
        return content
    except Exception as e:
        logger.error(f"Error reading resource {uri}: {e}", exc_info=True)
        raise ValueError(f"Failed to read resource {uri}: {str(e)}")


@app.call_tool()
async def call_tool(name: str, arguments: Any) -> List[TextContent]:
    """Execute tool via dispatcher"""
    try:
        _t0 = time.time()

        # Dispatch to handler
        result = dispatcher.dispatch(name, arguments or {})

        # Convert result to JSON
        if isinstance(result, dict):
            content = json.dumps(result, indent=2, default=str, ensure_ascii=False)
        else:
            content = str(result)

        elapsed = time.time() - _t0
        logger.debug(f"Tool {name} completed in {elapsed:.2f}s")

        return [TextContent(type="text", text=content)]

    except Exception as e:
        logger.error(f"Error executing tool {name}: {e}", exc_info=True)
        error_result = ErrorHandler.handle_unexpected_error(name, e)
        return [TextContent(type="text", text=json.dumps(error_result, indent=2))]


async def main():
    """Main entry point"""
    logger.info(f"Starting MCP-Sample-Data Server v{__version__}")

    async with stdio_server() as (read_stream, write_stream):
        await app.run(
            read_stream,
            write_stream,
            app.create_initialization_options()
        )


if __name__ == "__main__":
    asyncio.run(main())
```

---

### 7.2 Version File

#### src/__version__.py

```python
"""Version information for MCP-Sample-Data Server."""

__version__ = "1.0.0"
__author__ = "Finvision"
__description__ = "LLM-Orchestrated Sample Data Platform for Power BI - Create, understand, edit, and validate sample data"
```

---

### 7.3 Handler Registry

#### server/registry.py

```python
"""
Handler Registry System
Manages registration and lookup of tool handlers
"""
from typing import Dict, Callable, Any, List
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class ToolDefinition:
    """Definition of a tool with its handler"""
    name: str
    description: str
    handler: Callable
    input_schema: Dict[str, Any]
    category: str = "general"
    sort_order: int = 999


class HandlerRegistry:
    """Central registry for all tool handlers"""

    def __init__(self):
        self._handlers: Dict[str, ToolDefinition] = {}
        self._categories: Dict[str, List[str]] = {}

    def register(self, tool_def: ToolDefinition) -> None:
        """Register a tool handler"""
        self._handlers[tool_def.name] = tool_def

        if tool_def.category not in self._categories:
            self._categories[tool_def.category] = []
        self._categories[tool_def.category].append(tool_def.name)

        logger.debug(f"Registered tool: {tool_def.name} (category: {tool_def.category})")

    def get_handler(self, tool_name: str) -> Callable:
        """Get handler function for a tool"""
        if tool_name not in self._handlers:
            raise KeyError(f"Unknown tool: {tool_name}")
        return self._handlers[tool_name].handler

    def get_tool_def(self, tool_name: str) -> ToolDefinition:
        """Get full tool definition"""
        if tool_name not in self._handlers:
            raise KeyError(f"Unknown tool: {tool_name}")
        return self._handlers[tool_name]

    def get_all_tools(self) -> List[ToolDefinition]:
        """Get all registered tools"""
        return list(self._handlers.values())

    def get_all_tools_as_mcp(self):
        """Get all tools as MCP Tool objects"""
        from mcp.types import Tool
        from server.dispatch import ToolDispatcher

        reverse_map = {v: k for k, v in ToolDispatcher.TOOL_NAME_MAP.items()}

        tools = []
        sorted_defs = sorted(self._handlers.values(), key=lambda x: (x.sort_order, x.name))

        for tool_def in sorted_defs:
            mcp_name = reverse_map.get(tool_def.name, tool_def.name)
            tools.append(Tool(
                name=mcp_name,
                description=tool_def.description,
                inputSchema=tool_def.input_schema
            ))
        return tools

    def has_tool(self, tool_name: str) -> bool:
        """Check if tool is registered"""
        return tool_name in self._handlers

    def list_categories(self) -> List[str]:
        """List all categories"""
        return list(self._categories.keys())


# Global registry instance
_registry = HandlerRegistry()


def get_registry() -> HandlerRegistry:
    """Get the global handler registry"""
    return _registry
```

---

### 7.4 Tool Dispatcher

#### server/dispatch.py

```python
"""
Central Tool Dispatcher
Routes tool calls to appropriate handlers with error handling
"""
from typing import Dict, Any
import logging
from server.registry import get_registry
from core.validation.error_handler import ErrorHandler

logger = logging.getLogger(__name__)


class ToolDispatcher:
    """Dispatches tool calls to registered handlers"""

    # Mapping of numbered tool names to internal handler names
    TOOL_NAME_MAP = {
        # 01 - Discovery (5 tools)
        '01_Analyze_File': 'analyze_file',
        '01_Compare_Files': 'compare_files',
        '01_Detect_Domain': 'detect_domain',
        '01_Suggest_Schema': 'suggest_schema',
        '01_Find_Relationships': 'find_relationships',

        # 02 - Generation (6 tools)
        '02_Generate_Dimension': 'generate_dimension',
        '02_Generate_Fact': 'generate_fact',
        '02_Generate_From_Template': 'generate_from_template',
        '02_Generate_Date_Dimension': 'generate_date_dimension',
        '02_Fill_Column': 'fill_column',
        '02_Generate_Related_Records': 'generate_related_records',

        # 03 - Editing (6 tools)
        '03_Query_Data': 'query_data',
        '03_Update_Where': 'update_where',
        '03_Delete_Where': 'delete_where',
        '03_Transform_Column': 'transform_column',
        '03_Add_Column': 'add_column',
        '03_Merge_Tables': 'merge_tables',

        # 04 - Validation (5 tools)
        '04_Validate_Schema': 'validate_schema',
        '04_Validate_Rules': 'validate_rules',
        '04_Check_Balance': 'check_balance',
        '04_Check_Referential_Integrity': 'check_referential_integrity',
        '04_Check_Totals': 'check_totals',

        # 05 - Export (3 tools)
        '05_Export_CSV': 'export_csv',
        '05_Export_Parquet': 'export_parquet',
        '05_Export_PowerBI_Package': 'export_powerbi_package',

        # 06 - Projects (4 tools)
        '06_Create_Project': 'create_project',
        '06_List_Projects': 'list_projects',
        '06_Get_Project': 'get_project',
        '06_Delete_Project': 'delete_project',

        # 07 - Help (2 tools)
        '07_Show_User_Guide': 'show_user_guide',
        '07_List_Templates': 'list_templates',

        # 08 - Mapping (6 tools) - Multi-file relationships
        '08_Discover_Mapping': 'discover_mapping',
        '08_Define_Mapping': 'define_mapping',
        '08_Load_Mapping_Context': 'load_mapping_context',
        '08_Validate_Through_Mapping': 'validate_through_mapping',
        '08_Rollup_Through_Hierarchy': 'rollup_through_hierarchy',
        '08_Compare_Structures': 'compare_structures',
    }

    def __init__(self):
        self.registry = get_registry()
        self._call_count = 0

    def _resolve_tool_name(self, tool_name: str) -> str:
        """Resolve tool name to internal handler name"""
        if tool_name in self.TOOL_NAME_MAP:
            return self.TOOL_NAME_MAP[tool_name]
        return tool_name

    def dispatch(self, tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Dispatch a tool call to its handler"""
        self._call_count += 1

        try:
            internal_name = self._resolve_tool_name(tool_name)

            if not self.registry.has_tool(internal_name):
                logger.warning(f"Unknown tool requested: {tool_name}")
                return {
                    'success': False,
                    'error': f'Unknown tool: {tool_name}',
                    'available_tools': [t.name for t in self.registry.get_all_tools()[:10]]
                }

            handler = self.registry.get_handler(internal_name)
            logger.debug(f"Dispatching tool: {tool_name} -> {internal_name}")
            result = handler(arguments)

            if not isinstance(result, dict):
                result = {'success': True, 'result': result}

            return result

        except Exception as e:
            logger.error(f"Error dispatching tool {tool_name}: {e}", exc_info=True)
            return ErrorHandler.handle_unexpected_error(tool_name, e)

    def get_stats(self) -> Dict[str, Any]:
        """Get dispatcher statistics"""
        return {
            'total_calls': self._call_count,
            'registered_tools': len(self.registry.get_all_tools()),
            'categories': self.registry.list_categories()
        }
```

---

### 7.5 Tool Schemas

#### server/tool_schemas.py

```python
"""
Tool Input Schemas
JSON Schema definitions for all tool inputs
"""

TOOL_SCHEMAS = {
    # ==================== Discovery Tools ====================
    'analyze_file': {
        "type": "object",
        "properties": {
            "path": {
                "type": "string",
                "description": "Path to CSV, Parquet, or Excel file"
            },
            "sample_size": {
                "type": "integer",
                "description": "Number of rows to analyze",
                "default": 1000
            },
            "deep_analysis": {
                "type": "boolean",
                "description": "Include statistical analysis and pattern detection",
                "default": True
            }
        },
        "required": ["path"]
    },

    'compare_files': {
        "type": "object",
        "properties": {
            "path1": {"type": "string", "description": "Path to first file"},
            "path2": {"type": "string", "description": "Path to second file"},
            "compare_content": {
                "type": "boolean",
                "description": "Compare actual values",
                "default": False
            }
        },
        "required": ["path1", "path2"]
    },

    'detect_domain': {
        "type": "object",
        "properties": {
            "path": {"type": "string", "description": "Path to data file"}
        },
        "required": ["path"]
    },

    'suggest_schema': {
        "type": "object",
        "properties": {
            "path": {"type": "string", "description": "Path to data file"},
            "target": {
                "type": "string",
                "enum": ["powerbi", "generic"],
                "default": "powerbi"
            }
        },
        "required": ["path"]
    },

    'find_relationships': {
        "type": "object",
        "properties": {
            "paths": {
                "type": "array",
                "items": {"type": "string"},
                "description": "List of file paths to analyze"
            },
            "confidence_threshold": {
                "type": "number",
                "description": "Minimum confidence (0-1)",
                "default": 0.7
            }
        },
        "required": ["paths"]
    },

    # ==================== Generation Tools ====================
    'generate_dimension': {
        "type": "object",
        "properties": {
            "name": {"type": "string", "description": "Table name"},
            "columns": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "type": {"type": "string"},
                        "generator": {"type": "string"},
                        "unique": {"type": "boolean"},
                        "nullable": {"type": "boolean"},
                        "values": {"type": "array"}
                    },
                    "required": ["name", "type"]
                }
            },
            "row_count": {"type": "integer"},
            "output_path": {"type": "string"},
            "output_format": {"type": "string", "enum": ["csv", "parquet"], "default": "csv"},
            "locale": {"type": "string", "default": "en_US"}
        },
        "required": ["name", "columns", "row_count"]
    },

    'generate_fact': {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "grain": {"type": "array", "items": {"type": "string"}},
            "measures": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "type": {"type": "string"},
                        "distribution": {"type": "string"},
                        "min": {"type": "number"},
                        "max": {"type": "number"},
                        "mean": {"type": "number"},
                        "std": {"type": "number"}
                    },
                    "required": ["name", "type"]
                }
            },
            "dimensions": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "path": {"type": "string"},
                        "key_column": {"type": "string"},
                        "weight_column": {"type": "string"}
                    },
                    "required": ["path", "key_column"]
                }
            },
            "row_count": {"type": "integer"},
            "output_path": {"type": "string"},
            "output_format": {"type": "string", "enum": ["csv", "parquet"], "default": "csv"}
        },
        "required": ["name", "grain", "measures", "dimensions", "row_count"]
    },

    'generate_from_template': {
        "type": "object",
        "properties": {
            "template": {"type": "string", "description": "Template name (e.g., 'financial/trial_balance')"},
            "row_count": {"type": "integer"},
            "overrides": {"type": "object"},
            "output_path": {"type": "string"},
            "output_format": {"type": "string", "enum": ["csv", "parquet"], "default": "csv"}
        },
        "required": ["template", "row_count"]
    },

    'generate_date_dimension': {
        "type": "object",
        "properties": {
            "start_date": {"type": "string", "description": "Start date (YYYY-MM-DD)"},
            "end_date": {"type": "string", "description": "End date (YYYY-MM-DD)"},
            "fiscal_year_start_month": {"type": "integer", "default": 1},
            "include_holidays": {"type": "boolean", "default": False},
            "holiday_country": {"type": "string", "default": "US"},
            "output_path": {"type": "string"},
            "output_format": {"type": "string", "enum": ["csv", "parquet"], "default": "csv"}
        },
        "required": ["start_date", "end_date"]
    },

    'fill_column': {
        "type": "object",
        "properties": {
            "path": {"type": "string"},
            "column": {"type": "string"},
            "strategy": {"type": "string", "enum": ["faker", "distribution", "formula", "lookup", "constant"]},
            "config": {"type": "object"},
            "output_path": {"type": "string"}
        },
        "required": ["path", "column", "strategy", "config"]
    },

    'generate_related_records': {
        "type": "object",
        "properties": {
            "source_path": {"type": "string"},
            "key_column": {"type": "string"},
            "records_per_key": {
                "type": "object",
                "properties": {
                    "min": {"type": "integer"},
                    "max": {"type": "integer"},
                    "distribution": {"type": "string"}
                }
            },
            "columns": {"type": "array"},
            "output_path": {"type": "string"},
            "output_format": {"type": "string", "enum": ["csv", "parquet"], "default": "csv"}
        },
        "required": ["source_path", "key_column", "records_per_key"]
    },

    # ==================== Editing Tools ====================
    'query_data': {
        "type": "object",
        "properties": {
            "query": {"type": "string", "description": "SQL query (DuckDB syntax)"},
            "files": {
                "type": "object",
                "additionalProperties": {"type": "string"},
                "description": "Map of table aliases to file paths"
            },
            "limit": {"type": "integer", "default": 1000},
            "output_path": {"type": "string"}
        },
        "required": ["query"]
    },

    'update_where': {
        "type": "object",
        "properties": {
            "path": {"type": "string"},
            "updates": {"type": "object", "description": "Column updates"},
            "where": {"type": "string", "description": "SQL WHERE clause"},
            "output_path": {"type": "string"},
            "dry_run": {"type": "boolean", "default": True}
        },
        "required": ["path", "updates", "where"]
    },

    'delete_where': {
        "type": "object",
        "properties": {
            "path": {"type": "string"},
            "where": {"type": "string"},
            "output_path": {"type": "string"},
            "dry_run": {"type": "boolean", "default": True}
        },
        "required": ["path", "where"]
    },

    'transform_column': {
        "type": "object",
        "properties": {
            "path": {"type": "string"},
            "column": {"type": "string"},
            "transformation": {
                "type": "string",
                "enum": ["upper", "lower", "trim", "round", "abs", "cast", "replace", "extract", "custom_sql"]
            },
            "config": {"type": "object"},
            "output_path": {"type": "string"}
        },
        "required": ["path", "column", "transformation"]
    },

    'add_column': {
        "type": "object",
        "properties": {
            "path": {"type": "string"},
            "column": {"type": "string"},
            "expression": {"type": "string", "description": "SQL expression"},
            "output_path": {"type": "string"}
        },
        "required": ["path", "column", "expression"]
    },

    'merge_tables': {
        "type": "object",
        "properties": {
            "operation": {"type": "string", "enum": ["join", "union", "union_all"]},
            "tables": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "path": {"type": "string"},
                        "alias": {"type": "string"}
                    }
                }
            },
            "join_config": {
                "type": "object",
                "properties": {
                    "type": {"type": "string", "enum": ["inner", "left", "right", "full"]},
                    "on": {"type": "string"}
                }
            },
            "output_path": {"type": "string"},
            "output_format": {"type": "string", "enum": ["csv", "parquet"], "default": "csv"}
        },
        "required": ["operation", "tables"]
    },

    # ==================== Validation Tools ====================
    'validate_schema': {
        "type": "object",
        "properties": {
            "path": {"type": "string"},
            "schema": {"type": "object"},
            "strict": {"type": "boolean", "default": False}
        },
        "required": ["path", "schema"]
    },

    'validate_rules': {
        "type": "object",
        "properties": {
            "path": {"type": "string"},
            "rules": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "type": {"type": "string"},
                        "columns": {"type": "array"},
                        "config": {"type": "object"}
                    }
                }
            }
        },
        "required": ["path", "rules"]
    },

    'check_balance': {
        "type": "object",
        "properties": {
            "path": {"type": "string"},
            "debit_column": {"type": "string"},
            "credit_column": {"type": "string"},
            "group_by": {"type": "array", "items": {"type": "string"}},
            "tolerance": {"type": "number", "default": 0.01}
        },
        "required": ["path", "debit_column", "credit_column"]
    },

    'check_referential_integrity': {
        "type": "object",
        "properties": {
            "fact_path": {"type": "string"},
            "dimension_path": {"type": "string"},
            "fact_key": {"type": "string"},
            "dimension_key": {"type": "string"}
        },
        "required": ["fact_path", "dimension_path", "fact_key", "dimension_key"]
    },

    'check_totals': {
        "type": "object",
        "properties": {
            "path": {"type": "string"},
            "measure_column": {"type": "string"},
            "group_columns": {"type": "array", "items": {"type": "string"}},
            "expected_total": {"type": "number"}
        },
        "required": ["path", "measure_column"]
    },

    # ==================== Export Tools ====================
    'export_csv': {
        "type": "object",
        "properties": {
            "path": {"type": "string"},
            "output_path": {"type": "string"},
            "delimiter": {"type": "string", "default": ","},
            "include_header": {"type": "boolean", "default": True},
            "encoding": {"type": "string", "default": "utf-8"},
            "date_format": {"type": "string", "default": "%Y-%m-%d"}
        },
        "required": ["path", "output_path"]
    },

    'export_parquet': {
        "type": "object",
        "properties": {
            "path": {"type": "string"},
            "output_path": {"type": "string"},
            "compression": {"type": "string", "enum": ["snappy", "gzip", "zstd", "none"], "default": "snappy"},
            "row_group_size": {"type": "integer", "default": 100000}
        },
        "required": ["path", "output_path"]
    },

    'export_powerbi_package': {
        "type": "object",
        "properties": {
            "tables": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "path": {"type": "string"},
                        "name": {"type": "string"},
                        "type": {"type": "string", "enum": ["fact", "dimension"]}
                    }
                }
            },
            "output_dir": {"type": "string"},
            "format": {"type": "string", "enum": ["csv", "parquet"], "default": "parquet"},
            "include_schema": {"type": "boolean", "default": True},
            "include_relationships": {"type": "boolean", "default": True}
        },
        "required": ["tables", "output_dir"]
    },

    # ==================== Project Tools ====================
    'create_project': {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "description": {"type": "string"},
            "domain": {"type": "string", "enum": ["financial", "sales", "inventory", "hr", "generic"], "default": "generic"}
        },
        "required": ["name"]
    },

    'list_projects': {
        "type": "object",
        "properties": {}
    },

    'get_project': {
        "type": "object",
        "properties": {
            "name": {"type": "string"}
        },
        "required": ["name"]
    },

    'delete_project': {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "confirm": {"type": "boolean"}
        },
        "required": ["name", "confirm"]
    },

    # ==================== Help Tools ====================
    'show_user_guide': {
        "type": "object",
        "properties": {}
    },

    'list_templates': {
        "type": "object",
        "properties": {
            "domain": {"type": "string", "enum": ["all", "financial", "sales", "inventory", "hr"], "default": "all"}
        }
    }
}
```

---

### 7.6 Handlers Init

#### server/handlers/__init__.py

```python
"""
Server Handlers Package
Individual handler modules for different tool categories
"""
from server.handlers.discovery_handlers import register_discovery_handlers
from server.handlers.generation_handlers import register_generation_handlers
from server.handlers.editing_handlers import register_editing_handlers
from server.handlers.validation_handlers import register_validation_handlers
from server.handlers.export_handlers import register_export_handlers
from server.handlers.project_handlers import register_project_handlers
from server.handlers.help_handlers import register_help_handlers


def register_all_handlers(registry):
    """Register all handlers with the registry"""
    register_discovery_handlers(registry)
    register_generation_handlers(registry)
    register_editing_handlers(registry)
    register_validation_handlers(registry)
    register_export_handlers(registry)
    register_project_handlers(registry)
    register_help_handlers(registry)


__all__ = ['register_all_handlers']
```

---

### 7.7 Discovery Handlers (Example)

#### server/handlers/discovery_handlers.py

```python
"""
Discovery Handlers
Tools for analyzing and understanding data files
"""
from typing import Dict, Any
import logging
import os
from server.registry import ToolDefinition
from server.tool_schemas import TOOL_SCHEMAS

logger = logging.getLogger(__name__)


def handle_analyze_file(args: Dict[str, Any]) -> Dict[str, Any]:
    """Deep analysis of a data file for LLM consumption"""
    try:
        import polars as pl
        import duckdb
        from core.discovery.schema_inference import infer_schema
        from core.discovery.pattern_analyzer import detect_patterns
        from core.discovery.domain_detector import detect_domain_hints

        path = args.get('path')
        sample_size = args.get('sample_size', 1000)
        deep_analysis = args.get('deep_analysis', True)

        if not path or not os.path.exists(path):
            return {'success': False, 'error': f'File not found: {path}'}

        # Determine file type and read
        ext = os.path.splitext(path)[1].lower()

        if ext == '.csv':
            df = pl.read_csv(path, n_rows=sample_size, infer_schema_length=sample_size)
            total_rows = sum(1 for _ in open(path, encoding='utf-8', errors='ignore')) - 1
        elif ext == '.parquet':
            df = pl.read_parquet(path, n_rows=sample_size)
            total_rows = pl.scan_parquet(path).select(pl.count()).collect().item()
        elif ext in ['.xlsx', '.xls']:
            df = pl.read_excel(path, read_csv_options={'n_rows': sample_size})
            total_rows = len(pl.read_excel(path))
        else:
            return {'success': False, 'error': f'Unsupported file type: {ext}'}

        # Build column analysis
        columns = []
        for col in df.columns:
            col_data = df[col]
            col_info = {
                'name': col,
                'dtype': str(col_data.dtype),
                'sample_values': col_data.head(5).to_list(),
                'null_count': col_data.null_count(),
                'null_pct': round(col_data.null_count() / len(df) * 100, 2),
                'unique_count': col_data.n_unique(),
                'unique_pct': round(col_data.n_unique() / len(df) * 100, 2),
            }

            # Add statistics for numeric columns
            if col_data.dtype in [pl.Float64, pl.Float32, pl.Int64, pl.Int32, pl.Int16, pl.Int8]:
                col_info['statistics'] = {
                    'min': col_data.min(),
                    'max': col_data.max(),
                    'mean': round(col_data.mean(), 2) if col_data.mean() else None,
                    'median': col_data.median(),
                    'std': round(col_data.std(), 2) if col_data.std() else None
                }

            if deep_analysis:
                col_info['patterns_detected'] = detect_patterns(col_data)
                col_info['likely_semantic_type'] = infer_semantic_type(col_data, col)

            columns.append(col_info)

        result = {
            'success': True,
            'file_info': {
                'path': path,
                'total_rows': total_rows,
                'sample_rows': len(df),
                'columns': len(df.columns),
                'file_size_mb': round(os.path.getsize(path) / 1024 / 1024, 2)
            },
            'columns': columns
        }

        if deep_analysis:
            result['domain_hints'] = detect_domain_hints(df)
            result['potential_keys'] = detect_potential_keys(df)
            result['quality_issues'] = detect_quality_issues(df)

        return result

    except Exception as e:
        logger.error(f"Error analyzing file: {e}", exc_info=True)
        return {'success': False, 'error': str(e)}


def infer_semantic_type(col_data, col_name: str) -> str:
    """Infer the semantic type of a column"""
    name_lower = col_name.lower()

    # Check name patterns
    if any(x in name_lower for x in ['_id', 'id_', '_key', 'key_']):
        return 'ID'
    if any(x in name_lower for x in ['date', 'time', 'created', 'updated', 'timestamp']):
        return 'DateTime'
    if any(x in name_lower for x in ['amount', 'total', 'sum', 'price', 'cost', 'revenue', 'sales']):
        return 'Amount'
    if any(x in name_lower for x in ['qty', 'quantity', 'count', 'number']):
        return 'Quantity'
    if any(x in name_lower for x in ['pct', 'percent', 'rate', 'ratio']):
        return 'Percentage'
    if any(x in name_lower for x in ['name', 'description', 'title', 'label']):
        return 'Name'
    if any(x in name_lower for x in ['code', 'sku', 'number']):
        return 'Code'
    if any(x in name_lower for x in ['email', 'phone', 'address']):
        return 'Contact'
    if any(x in name_lower for x in ['flag', 'is_', 'has_']):
        return 'Boolean'

    return 'Unknown'


def detect_potential_keys(df) -> list:
    """Detect columns that could be keys"""
    import polars as pl

    potential_keys = []
    for col in df.columns:
        unique_ratio = df[col].n_unique() / len(df)
        if unique_ratio > 0.95:
            potential_keys.append({
                'column': col,
                'type': 'primary_key_candidate',
                'unique_ratio': round(unique_ratio, 4)
            })
        elif unique_ratio < 0.5 and df[col].n_unique() > 1:
            potential_keys.append({
                'column': col,
                'type': 'foreign_key_candidate',
                'unique_ratio': round(unique_ratio, 4)
            })

    return potential_keys


def detect_quality_issues(df) -> list:
    """Detect data quality issues"""
    issues = []

    for col in df.columns:
        null_pct = df[col].null_count() / len(df) * 100
        if null_pct > 50:
            issues.append({
                'column': col,
                'issue': 'high_null_rate',
                'severity': 'warning',
                'detail': f'{null_pct:.1f}% null values'
            })

        if df[col].n_unique() == 1:
            issues.append({
                'column': col,
                'issue': 'single_value',
                'severity': 'info',
                'detail': 'Column contains only one unique value'
            })

    return issues


def handle_compare_files(args: Dict[str, Any]) -> Dict[str, Any]:
    """Compare structure and content of two files"""
    try:
        import polars as pl

        path1 = args.get('path1')
        path2 = args.get('path2')
        compare_content = args.get('compare_content', False)

        # Read both files
        df1 = pl.read_csv(path1) if path1.endswith('.csv') else pl.read_parquet(path1)
        df2 = pl.read_csv(path2) if path2.endswith('.csv') else pl.read_parquet(path2)

        # Compare schemas
        cols1 = set(df1.columns)
        cols2 = set(df2.columns)

        result = {
            'success': True,
            'schema_comparison': {
                'file1_only': list(cols1 - cols2),
                'file2_only': list(cols2 - cols1),
                'common': list(cols1 & cols2)
            },
            'row_counts': {
                'file1': len(df1),
                'file2': len(df2)
            }
        }

        # Type comparison for common columns
        type_diffs = []
        for col in cols1 & cols2:
            if str(df1[col].dtype) != str(df2[col].dtype):
                type_diffs.append({
                    'column': col,
                    'file1_type': str(df1[col].dtype),
                    'file2_type': str(df2[col].dtype)
                })
        result['type_differences'] = type_diffs

        # Content comparison for potential join keys
        if compare_content:
            potential_joins = []
            for col in cols1 & cols2:
                vals1 = set(df1[col].unique().to_list())
                vals2 = set(df2[col].unique().to_list())
                overlap = len(vals1 & vals2) / max(len(vals1), len(vals2)) if vals1 or vals2 else 0
                if overlap > 0.5:
                    potential_joins.append({
                        'column': col,
                        'overlap_ratio': round(overlap, 2),
                        'file1_unique': len(vals1),
                        'file2_unique': len(vals2)
                    })
            result['potential_join_keys'] = potential_joins

        return result

    except Exception as e:
        logger.error(f"Error comparing files: {e}", exc_info=True)
        return {'success': False, 'error': str(e)}


def handle_detect_domain(args: Dict[str, Any]) -> Dict[str, Any]:
    """Classify the domain/type of data"""
    try:
        import polars as pl
        from core.discovery.domain_detector import DomainDetector

        path = args.get('path')

        df = pl.read_csv(path, n_rows=1000) if path.endswith('.csv') else pl.read_parquet(path, n_rows=1000)

        detector = DomainDetector()
        result = detector.detect(df)

        return {'success': True, **result}

    except Exception as e:
        logger.error(f"Error detecting domain: {e}", exc_info=True)
        return {'success': False, 'error': str(e)}


def handle_suggest_schema(args: Dict[str, Any]) -> Dict[str, Any]:
    """Generate a recommended schema for the data"""
    try:
        import polars as pl
        from core.discovery.schema_inference import SchemaInferrer

        path = args.get('path')
        target = args.get('target', 'powerbi')

        df = pl.read_csv(path, n_rows=1000) if path.endswith('.csv') else pl.read_parquet(path, n_rows=1000)

        inferrer = SchemaInferrer(target=target)
        schema = inferrer.infer(df)

        return {'success': True, 'schema': schema}

    except Exception as e:
        logger.error(f"Error suggesting schema: {e}", exc_info=True)
        return {'success': False, 'error': str(e)}


def handle_find_relationships(args: Dict[str, Any]) -> Dict[str, Any]:
    """Detect potential relationships between multiple files"""
    try:
        import polars as pl
        from core.discovery.relationship_finder import RelationshipFinder

        paths = args.get('paths', [])
        confidence_threshold = args.get('confidence_threshold', 0.7)

        # Load all files
        tables = {}
        for path in paths:
            name = os.path.splitext(os.path.basename(path))[0]
            df = pl.read_csv(path, n_rows=5000) if path.endswith('.csv') else pl.read_parquet(path, n_rows=5000)
            tables[name] = {'path': path, 'df': df}

        finder = RelationshipFinder(confidence_threshold=confidence_threshold)
        relationships = finder.find(tables)

        return {
            'success': True,
            'tables_analyzed': list(tables.keys()),
            'relationships': relationships
        }

    except Exception as e:
        logger.error(f"Error finding relationships: {e}", exc_info=True)
        return {'success': False, 'error': str(e)}


def register_discovery_handlers(registry):
    """Register discovery handlers"""
    tools = [
        ToolDefinition(
            name="analyze_file",
            description="Deep analysis of a data file - returns rich metadata about columns, types, patterns, and quality for LLM reasoning",
            handler=handle_analyze_file,
            input_schema=TOOL_SCHEMAS.get('analyze_file', {}),
            category="discovery",
            sort_order=1
        ),
        ToolDefinition(
            name="compare_files",
            description="Compare structure and content of two data files - identify schema differences and potential join keys",
            handler=handle_compare_files,
            input_schema=TOOL_SCHEMAS.get('compare_files', {}),
            category="discovery",
            sort_order=2
        ),
        ToolDefinition(
            name="detect_domain",
            description="Classify the domain/type of data (financial, sales, inventory, HR) with confidence scores",
            handler=handle_detect_domain,
            input_schema=TOOL_SCHEMAS.get('detect_domain', {}),
            category="discovery",
            sort_order=3
        ),
        ToolDefinition(
            name="suggest_schema",
            description="Generate a recommended schema optimized for Power BI import",
            handler=handle_suggest_schema,
            input_schema=TOOL_SCHEMAS.get('suggest_schema', {}),
            category="discovery",
            sort_order=4
        ),
        ToolDefinition(
            name="find_relationships",
            description="Detect potential relationships between multiple files for star schema design",
            handler=handle_find_relationships,
            input_schema=TOOL_SCHEMAS.get('find_relationships', {}),
            category="discovery",
            sort_order=5
        ),
    ]

    for tool in tools:
        registry.register(tool)

    logger.info(f"Registered {len(tools)} discovery handlers")
```

---

### 7.8 Core Modules (Examples)

#### core/discovery/schema_inference.py

```python
"""
Schema Inference Module
Automatically infer data types and semantic types from data
"""
import polars as pl
from typing import Dict, Any, List
import re


class SchemaInferrer:
    """Infers optimal schema for data"""

    def __init__(self, target: str = 'powerbi'):
        self.target = target
        self.type_mappings = {
            'powerbi': {
                pl.Int64: 'Whole Number',
                pl.Int32: 'Whole Number',
                pl.Float64: 'Decimal Number',
                pl.Float32: 'Decimal Number',
                pl.Utf8: 'Text',
                pl.Boolean: 'True/False',
                pl.Date: 'Date',
                pl.Datetime: 'Date/Time'
            }
        }

    def infer(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Infer schema from DataFrame"""
        columns = []

        for col in df.columns:
            col_data = df[col]
            dtype = col_data.dtype

            col_schema = {
                'name': col,
                'source_type': str(dtype),
                'target_type': self.type_mappings.get(self.target, {}).get(dtype, 'Text'),
                'nullable': col_data.null_count() > 0,
                'unique': col_data.n_unique() == len(df),
                'semantic_type': self._infer_semantic_type(col_data, col)
            }

            # Suggest optimizations
            if dtype == pl.Int64:
                max_val = col_data.max()
                min_val = col_data.min()
                if min_val >= 0 and max_val < 256:
                    col_schema['optimization'] = 'Could be UInt8'
                elif min_val >= -128 and max_val < 128:
                    col_schema['optimization'] = 'Could be Int8'

            columns.append(col_schema)

        return {
            'columns': columns,
            'row_count': len(df),
            'recommendations': self._get_recommendations(df)
        }

    def _infer_semantic_type(self, col_data: pl.Series, col_name: str) -> str:
        """Infer semantic type from column name and data"""
        name_lower = col_name.lower()

        # Pattern matching on column name
        patterns = {
            'ID': r'(^id$|_id$|^id_|_key$|^key_)',
            'Date': r'(date|time|created|updated|timestamp)',
            'Amount': r'(amount|total|sum|price|cost|revenue|sales)',
            'Quantity': r'(qty|quantity|count|number)',
            'Percentage': r'(pct|percent|rate|ratio)',
            'Name': r'(name|description|title|label)',
            'Code': r'(code|sku)',
            'Boolean': r'(flag|is_|has_|can_)'
        }

        for semantic_type, pattern in patterns.items():
            if re.search(pattern, name_lower):
                return semantic_type

        return 'Unknown'

    def _get_recommendations(self, df: pl.DataFrame) -> List[str]:
        """Get schema optimization recommendations"""
        recommendations = []

        # Check for potential issues
        for col in df.columns:
            if df[col].null_count() / len(df) > 0.5:
                recommendations.append(f"Column '{col}' has >50% null values - consider removing or handling")

            if df[col].n_unique() == 1:
                recommendations.append(f"Column '{col}' has only one value - may not be useful")

        return recommendations
```

---

#### core/validation/balance_checker.py

```python
"""
Balance Checker Module
Validates financial balance rules (debit = credit)
"""
import polars as pl
from typing import Dict, Any, List, Optional
import logging

logger = logging.getLogger(__name__)


class BalanceChecker:
    """Checks financial balance rules"""

    def __init__(self, tolerance: float = 0.01):
        self.tolerance = tolerance

    def check_balance(
        self,
        df: pl.DataFrame,
        debit_column: str,
        credit_column: str,
        group_by: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Check that debits equal credits.

        Args:
            df: DataFrame to check
            debit_column: Column containing debit amounts
            credit_column: Column containing credit amounts
            group_by: Optional columns to group by (e.g., period, entity)

        Returns:
            Balance check results
        """
        try:
            # Ensure columns exist
            if debit_column not in df.columns:
                return {'success': False, 'error': f'Debit column not found: {debit_column}'}
            if credit_column not in df.columns:
                return {'success': False, 'error': f'Credit column not found: {credit_column}'}

            if group_by:
                # Group by and check each group
                grouped = df.group_by(group_by).agg([
                    pl.col(debit_column).sum().alias('total_debit'),
                    pl.col(credit_column).sum().alias('total_credit')
                ]).with_columns([
                    (pl.col('total_debit') - pl.col('total_credit')).alias('difference'),
                    (pl.col('total_debit') - pl.col('total_credit')).abs().alias('abs_difference')
                ])

                # Find imbalanced groups
                imbalanced = grouped.filter(pl.col('abs_difference') > self.tolerance)

                result = {
                    'success': True,
                    'balanced': len(imbalanced) == 0,
                    'total_groups': len(grouped),
                    'balanced_groups': len(grouped) - len(imbalanced),
                    'imbalanced_groups': len(imbalanced),
                    'tolerance': self.tolerance
                }

                if len(imbalanced) > 0:
                    result['imbalanced_details'] = imbalanced.to_dicts()

                # Grand totals
                result['grand_totals'] = {
                    'total_debit': df[debit_column].sum(),
                    'total_credit': df[credit_column].sum(),
                    'difference': df[debit_column].sum() - df[credit_column].sum()
                }

            else:
                # Check overall balance
                total_debit = df[debit_column].sum()
                total_credit = df[credit_column].sum()
                difference = total_debit - total_credit

                result = {
                    'success': True,
                    'balanced': abs(difference) <= self.tolerance,
                    'total_debit': total_debit,
                    'total_credit': total_credit,
                    'difference': difference,
                    'tolerance': self.tolerance
                }

            return result

        except Exception as e:
            logger.error(f"Error checking balance: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def check_trial_balance(
        self,
        df: pl.DataFrame,
        period_column: str,
        debit_column: str,
        credit_column: str
    ) -> Dict[str, Any]:
        """
        Check trial balance per period.

        Args:
            df: Trial balance DataFrame
            period_column: Column containing period identifier
            debit_column: Column containing debit amounts
            credit_column: Column containing credit amounts

        Returns:
            Trial balance validation results
        """
        return self.check_balance(df, debit_column, credit_column, group_by=[period_column])
```

---

#### core/editing/duckdb_engine.py

```python
"""
DuckDB Engine Module
SQL operations on data files using DuckDB
"""
import duckdb
import polars as pl
from typing import Dict, Any, List, Optional
import logging
import os

logger = logging.getLogger(__name__)


class DuckDBEngine:
    """Executes SQL operations on data files"""

    def __init__(self):
        self.conn = duckdb.connect(':memory:')

    def execute_query(
        self,
        query: str,
        files: Optional[Dict[str, str]] = None,
        limit: int = 1000
    ) -> Dict[str, Any]:
        """
        Execute a SQL query on data files.

        Args:
            query: SQL query to execute
            files: Map of table aliases to file paths
            limit: Maximum rows to return

        Returns:
            Query results
        """
        try:
            # Register file paths as tables
            if files:
                for alias, path in files.items():
                    if path.endswith('.csv'):
                        self.conn.execute(f"CREATE OR REPLACE VIEW {alias} AS SELECT * FROM read_csv_auto('{path}')")
                    elif path.endswith('.parquet'):
                        self.conn.execute(f"CREATE OR REPLACE VIEW {alias} AS SELECT * FROM read_parquet('{path}')")

            # Add LIMIT if not present
            query_upper = query.upper()
            if 'LIMIT' not in query_upper:
                query = f"{query} LIMIT {limit}"

            # Execute query
            result = self.conn.execute(query).pl()

            return {
                'success': True,
                'row_count': len(result),
                'columns': result.columns,
                'data': result.to_dicts()
            }

        except Exception as e:
            logger.error(f"Error executing query: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def update_where(
        self,
        path: str,
        updates: Dict[str, Any],
        where: str,
        output_path: Optional[str] = None,
        dry_run: bool = True
    ) -> Dict[str, Any]:
        """
        Update rows matching a condition.

        Args:
            path: Path to data file
            updates: Column updates (column_name: new_value)
            where: SQL WHERE clause
            output_path: Output path (defaults to overwrite)
            dry_run: Preview changes without applying

        Returns:
            Update results
        """
        try:
            # Load data
            if path.endswith('.csv'):
                df = pl.read_csv(path)
            else:
                df = pl.read_parquet(path)

            # Register as table
            self.conn.execute("CREATE OR REPLACE VIEW data AS SELECT * FROM df")

            # Count affected rows
            count_query = f"SELECT COUNT(*) as cnt FROM data WHERE {where}"
            affected = self.conn.execute(count_query).fetchone()[0]

            if dry_run:
                # Show preview of changes
                preview_query = f"SELECT * FROM data WHERE {where} LIMIT 10"
                preview = self.conn.execute(preview_query).pl()

                return {
                    'success': True,
                    'dry_run': True,
                    'affected_rows': affected,
                    'preview': preview.to_dicts(),
                    'updates_to_apply': updates
                }
            else:
                # Build update query using CASE statements
                set_clauses = []
                for col, value in updates.items():
                    if isinstance(value, str) and not value.startswith('('):
                        value = f"'{value}'"
                    set_clauses.append(f"CASE WHEN {where} THEN {value} ELSE {col} END as {col}")

                # Select with updates
                other_cols = [c for c in df.columns if c not in updates]
                select_cols = other_cols + [f"({clause})" for clause in set_clauses]

                update_query = f"SELECT {', '.join(select_cols)} FROM data"
                updated_df = self.conn.execute(update_query).pl()

                # Save
                output = output_path or path
                if output.endswith('.csv'):
                    updated_df.write_csv(output)
                else:
                    updated_df.write_parquet(output)

                return {
                    'success': True,
                    'dry_run': False,
                    'affected_rows': affected,
                    'output_path': output
                }

        except Exception as e:
            logger.error(f"Error updating data: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def delete_where(
        self,
        path: str,
        where: str,
        output_path: Optional[str] = None,
        dry_run: bool = True
    ) -> Dict[str, Any]:
        """Delete rows matching a condition"""
        try:
            if path.endswith('.csv'):
                df = pl.read_csv(path)
            else:
                df = pl.read_parquet(path)

            self.conn.execute("CREATE OR REPLACE VIEW data AS SELECT * FROM df")

            # Count affected rows
            count_query = f"SELECT COUNT(*) as cnt FROM data WHERE {where}"
            affected = self.conn.execute(count_query).fetchone()[0]

            if dry_run:
                preview_query = f"SELECT * FROM data WHERE {where} LIMIT 10"
                preview = self.conn.execute(preview_query).pl()

                return {
                    'success': True,
                    'dry_run': True,
                    'rows_to_delete': affected,
                    'preview': preview.to_dicts()
                }
            else:
                # Delete by selecting NOT matching
                delete_query = f"SELECT * FROM data WHERE NOT ({where})"
                remaining_df = self.conn.execute(delete_query).pl()

                output = output_path or path
                if output.endswith('.csv'):
                    remaining_df.write_csv(output)
                else:
                    remaining_df.write_parquet(output)

                return {
                    'success': True,
                    'dry_run': False,
                    'rows_deleted': affected,
                    'rows_remaining': len(remaining_df),
                    'output_path': output
                }

        except Exception as e:
            logger.error(f"Error deleting data: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}
```

---

### 7.9 Configuration

#### config/default_config.json

```json
{
  "server": {
    "name": "MCP-Sample-Data",
    "version": "1.0.0"
  },
  "cache": {
    "enabled": true,
    "max_size": 100,
    "ttl_seconds": 300
  },
  "generation": {
    "default_locale": "en_US",
    "max_rows_per_generation": 1000000,
    "default_output_format": "csv"
  },
  "editing": {
    "default_query_limit": 1000,
    "max_query_limit": 100000,
    "dry_run_default": true
  },
  "validation": {
    "balance_tolerance": 0.01,
    "referential_integrity_sample_size": 10000
  },
  "export": {
    "default_format": "csv",
    "parquet_compression": "snappy",
    "csv_encoding": "utf-8"
  },
  "projects": {
    "storage_path": "projects"
  },
  "logging": {
    "level": "WARNING",
    "file": "logs/sample_data.log"
  }
}
```

---

## 8. Setup & Configuration

### 8.1 setup-dev.bat

```batch
@echo off
setlocal enabledelayedexpansion

echo.
echo ========================================
echo   MCP-Sample-Data Dev Setup
echo ========================================
echo.

:: Check if Python 3.13 is available
echo Checking for Python 3.13...
set "PYTHON_CMD="

:: Try py launcher with 3.13
py -3.13 --version >nul 2>&1
if %errorlevel%==0 (
    set "PYTHON_CMD=py -3.13"
    goto :python_found
)

:: Try common installation paths
if exist "%LOCALAPPDATA%\Programs\Python\Python313\python.exe" (
    set "PYTHON_CMD=%LOCALAPPDATA%\Programs\Python\Python313\python.exe"
    goto :python_found
)

if exist "%PROGRAMFILES%\Python313\python.exe" (
    set "PYTHON_CMD=%PROGRAMFILES%\Python313\python.exe"
    goto :python_found
)

echo.
echo ERROR: Python 3.13 not found!
echo Please install Python 3.13 from: https://www.python.org/downloads/
echo.
pause
exit /b 1

:python_found
for /f "tokens=*" %%i in ('!PYTHON_CMD! --version') do echo Found: %%i
echo Using: !PYTHON_CMD!
echo.

:: Get script directory
set "SCRIPT_DIR=%~dp0"
set "PROJECT_DIR=%SCRIPT_DIR:~0,-1%"

echo Project directory: %PROJECT_DIR%
echo.

:: Create virtual environment
echo Step 1/4: Creating virtual environment...
!PYTHON_CMD! -m venv "%PROJECT_DIR%\venv"

if not exist "%PROJECT_DIR%\venv\Scripts\python.exe" (
    echo ERROR: Virtual environment creation failed!
    pause
    exit /b 1
)

echo Virtual environment created successfully.
echo.

:: Activate and install dependencies
echo Step 2/4: Installing dependencies...
call "%PROJECT_DIR%\venv\Scripts\activate.bat"
pip install -r requirements.txt

if errorlevel 1 (
    echo WARNING: Some dependencies may have failed to install.
)

:: Create required directories
echo.
echo Step 3/4: Creating directories...
if not exist "%PROJECT_DIR%\logs" mkdir "%PROJECT_DIR%\logs"
if not exist "%PROJECT_DIR%\exports" mkdir "%PROJECT_DIR%\exports"
if not exist "%PROJECT_DIR%\projects" mkdir "%PROJECT_DIR%\projects"
echo Directories created.

:: Configure Claude Desktop
echo.
echo ========================================
echo   Claude Desktop Configuration
echo ========================================
echo.

set "configPath=%APPDATA%\Claude\claude_desktop_config.json"

echo Step 4/4: Updating Claude Desktop config...

:: Use PowerShell to handle JSON
powershell -ExecutionPolicy Bypass -Command ^
    "$configPath = '%configPath%';" ^
    "$projectDir = '%PROJECT_DIR%';" ^
    "$serverName = 'MCP-Sample-Data';" ^
    "$pythonPath = Join-Path $projectDir 'venv\Scripts\python.exe';" ^
    "$scriptPath = Join-Path $projectDir 'src\sample_data_server.py';" ^
    "$mcpServer = @{ 'command' = $pythonPath; 'args' = @($scriptPath) };" ^
    "if (Test-Path $configPath) { $config = Get-Content $configPath -Raw | ConvertFrom-Json } else { $config = [PSCustomObject]@{} };" ^
    "if (-not $config.PSObject.Properties['mcpServers']) { $config | Add-Member -NotePropertyName 'mcpServers' -NotePropertyValue ([PSCustomObject]@{}) };" ^
    "if ($config.mcpServers.PSObject.Properties[$serverName]) { $config.mcpServers.$serverName = $mcpServer } else { $config.mcpServers | Add-Member -NotePropertyName $serverName -NotePropertyValue $mcpServer };" ^
    "$json = $config | ConvertTo-Json -Depth 10;" ^
    "[System.IO.File]::WriteAllText($configPath, $json, [System.Text.UTF8Encoding]::new($false));" ^
    "Write-Host 'Config saved to:' $configPath -ForegroundColor Cyan;" ^
    "Write-Host 'MCP Server added as:' $serverName -ForegroundColor Cyan;"

:: Success
echo.
echo ========================================
echo   Setup Complete!
echo ========================================
echo.
echo Project path: %PROJECT_DIR%
echo Claude config: %configPath%
echo MCP Server: MCP-Sample-Data
echo.
echo IMPORTANT: Restart Claude Desktop for changes to take effect!
echo.
echo To start manually:
echo   1. cd "%PROJECT_DIR%"
echo   2. venv\Scripts\activate.bat
echo   3. python src/sample_data_server.py
echo.
pause
```

---

### 8.2 .gitignore

```gitignore
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# Virtual Environment
venv/
ENV/

# IDE
.idea/
.vscode/
*.swp
*.swo
*~

# Logs
logs/
*.log

# Local config
config/local_config.json

# Exports and projects (user data)
exports/
projects/

# Cache
.cache/
*.cache

# OS
.DS_Store
Thumbs.db

# Test artifacts
.pytest_cache/
.coverage
htmlcov/
```

---

## 9. Testing Strategy

### 9.1 Test Structure

```
tests/
├── __init__.py
├── conftest.py              # Pytest fixtures
├── test_discovery.py        # Discovery handler tests
├── test_generation.py       # Generation handler tests
├── test_validation.py       # Validation handler tests
├── test_editing.py          # Editing handler tests
├── test_integration.py      # End-to-end tests
└── fixtures/
    ├── sample_csv.csv
    ├── sample_parquet.parquet
    └── sample_templates/
```

### 9.2 Example Test

```python
# tests/test_validation.py
import pytest
import polars as pl
from core.validation.balance_checker import BalanceChecker


@pytest.fixture
def balanced_data():
    """Create balanced trial balance data"""
    return pl.DataFrame({
        'period': ['2024-01', '2024-01', '2024-02', '2024-02'],
        'account': ['1000', '2000', '1000', '2000'],
        'debit': [100.0, 0.0, 200.0, 0.0],
        'credit': [0.0, 100.0, 0.0, 200.0]
    })


@pytest.fixture
def imbalanced_data():
    """Create imbalanced data"""
    return pl.DataFrame({
        'period': ['2024-01', '2024-01'],
        'account': ['1000', '2000'],
        'debit': [100.0, 0.0],
        'credit': [0.0, 90.0]  # 10 difference
    })


def test_balance_check_passes(balanced_data):
    checker = BalanceChecker()
    result = checker.check_balance(balanced_data, 'debit', 'credit', group_by=['period'])

    assert result['success'] is True
    assert result['balanced'] is True
    assert result['imbalanced_groups'] == 0


def test_balance_check_fails(imbalanced_data):
    checker = BalanceChecker()
    result = checker.check_balance(imbalanced_data, 'debit', 'credit', group_by=['period'])

    assert result['success'] is True
    assert result['balanced'] is False
    assert result['imbalanced_groups'] == 1


def test_balance_with_tolerance():
    checker = BalanceChecker(tolerance=0.01)
    df = pl.DataFrame({
        'debit': [100.0],
        'credit': [99.995]  # Within tolerance
    })

    result = checker.check_balance(df, 'debit', 'credit')
    assert result['balanced'] is True
```

---

## 10. Usage Examples

### 10.1 Analyzing and Validating Financial Data

```
User: "I have a trial balance at C:/data/trial_balance.csv and a chart of accounts
      at C:/data/coa.csv. Can you analyze both and check if the trial balance is valid?"

Claude:
1. Uses 01_Analyze_File on trial_balance.csv
   → Gets column info: period, account_code, debit_amount, credit_amount
   → Domain hints: financial indicators detected

2. Uses 01_Analyze_File on coa.csv
   → Gets column info: account_code, account_name, account_type
   → Detects this is a chart of accounts

3. Uses 04_Check_Balance on trial_balance.csv
   → Checks debit=credit per period
   → Finds 2024-03 is imbalanced by $12.50

4. Uses 04_Check_Referential_Integrity
   → fact_path: trial_balance.csv
   → dimension_path: coa.csv
   → fact_key: account_code
   → dimension_key: account_code
   → Finds 5 orphan account codes

Claude responds: "Your trial balance has two issues:
1. Period 2024-03 is out of balance by $12.50 (debits > credits)
2. 5 account codes in the trial balance don't exist in your chart of accounts:
   4510, 6210, 6220, 7100, 7200

Would you like me to help fix these issues?"
```

### 10.2 Generating Sample Sales Data

```
User: "I need sample sales data for a Power BI demo. Create a fact table with
      50,000 orders and the necessary dimension tables."

Claude:
1. Uses 02_Generate_Date_Dimension
   → start_date: 2022-01-01, end_date: 2024-12-31
   → Generates 1,096 date records

2. Uses 02_Generate_Dimension for dim_customer
   → 500 customers with name, segment, region, country

3. Uses 02_Generate_Dimension for dim_product
   → 200 products with name, category, price

4. Uses 02_Generate_Fact for fact_sales
   → 50,000 rows linked to all dimensions
   → Measures: quantity, unit_price, discount, amount

5. Uses 05_Export_PowerBI_Package
   → Exports all tables as parquet
   → Includes relationships.json

Claude responds: "I've created your sample sales data:
- fact_sales: 50,000 orders
- dim_customer: 500 customers
- dim_product: 200 products
- dim_date: 1,096 dates (2022-2024)

All files are in C:/exports/sales_demo/ with a relationships.json
describing the star schema. Ready to import into Power BI!"
```

### 10.3 Editing Large Datasets

```
User: "I need to update all transactions in March 2024 to change the
      department from 'Marketing' to 'Growth Marketing'."

Claude:
1. Uses 03_Update_Where with dry_run=true
   → path: C:/data/transactions.csv
   → updates: {"department": "Growth Marketing"}
   → where: "month = '2024-03' AND department = 'Marketing'"
   → Returns: 1,247 rows would be affected

Claude asks: "This will update 1,247 transactions. Should I proceed?"

User: "Yes, go ahead"

2. Uses 03_Update_Where with dry_run=false
   → Same parameters
   → Updates applied, file saved

Claude responds: "Done! Updated 1,247 transactions. The original file
has been modified. Would you like me to create a backup?"
```

### 10.4 Multi-File Balance Sheet Validation with Mapping

```
User: "I have 3 files in C:/data/finance/:
      - Balance sheet sample data.xlsx (GL transactions)
      - S_formulas.xlsx (mapping/hierarchy)
      - s_report_lines orig.xlsx (report structure)

      Can you check if my balance sheet balances (Assets = Liabilities + Equity)
      for period 24288?"

Claude:
1. Uses 08_Load_Mapping_Context to load all 3 files
   → files: [
       {path: "Balance sheet sample data.xlsx", role: "source_data", alias: "bs_data"},
       {path: "S_formulas.xlsx", role: "hierarchy", alias: "formulas"},
       {path: "s_report_lines orig.xlsx", role: "report_structure", alias: "report"}
     ]
   → context_name: "balance_sheet_check"
   → Successfully loaded 3 files: 9,433 source rows, 276 formula rows, 251 report lines

2. Uses 08_Discover_Mapping to understand relationships
   → files: all 3 files
   → source_file: "Balance sheet sample data.xlsx"
   → detect_hierarchies: true
   → Discovers:
     - GL Typology → Nominator element (fuzzy match, 85% confidence)
     - Report structure has BS lines with rollup totals (Total Assets, Total Equity, Total Liabilities)
     - 7 direct matches, 18 fuzzy matches found

3. Uses 08_Compare_Structures to check coverage
   → source_file: "Balance sheet sample data.xlsx"
   → report_structure_file: "s_report_lines orig.xlsx"
   → source_column: "GL Typology"
   → report_line_column: "Nominator element"
   → Result:
     - 25 unique source categories
     - 34 BS report lines
     - Mapped: 23/25 (92%)
     - Unmapped source values: "Untaxed reserves", "Fixed assets under construction..."

4. Uses 08_Validate_Through_Mapping
   → context_name: "balance_sheet_check"
   → validation_rule: "balance_sheet_equation"
   → source_amount_column: "Amount (company code  currency)"
   → source_category_column: "GL Typology"
   → filter: {"Posting Period Seqnr": 24288}
   → Result:
     {
       "validation_passed": false,
       "details": {
         "total_assets": 616337.04,
         "total_equity": 293729.34,
         "total_liabilities": 3666345.24,
         "equation_result": -3343737.54
       },
       "unmapped_values": [
         {"value": "Untaxed reserves", "amount": 1036981.73}
       ],
       "mapping_coverage": 89.4%
     }

Claude responds: "I've analyzed your balance sheet data for period 24288.

**Result: Balance Sheet DOES NOT BALANCE** ❌

| Category | Amount |
|----------|--------|
| Total Assets | €616,337.04 |
| Total Equity | €293,729.34 |
| Total Liabilities | €3,666,345.24 |
| **Expected (Liab + Equity)** | **€3,960,074.58** |
| **Difference** | **-€3,343,737.54** |

**Issues Found:**

1. **Unmapped GL Typology values** (€1,036,981.73):
   - 'Untaxed reserves' - needs classification (Equity or Liability?)
   - 'Fixed assets under construction...' - likely belongs in Assets

2. **Mapping coverage is 89.4%** - Some GL categories aren't mapped to report lines.

**Recommendations:**
1. Review 'Untaxed reserves' - typically classified as Equity in most jurisdictions
2. Check if 'Fixed assets under construction' should roll up to 'Tangible fixed assets'
3. Verify the sign conventions match between source data and report structure

Would you like me to:
- Show detailed breakdown by category?
- Help define explicit mappings for unmapped values?
- Generate a corrected mapping file?"
```

### 10.5 P&L Rollup Through Formula Hierarchy

```
User: "Using the same files, can you calculate EBITDA from the formula hierarchy?"

Claude:
1. Uses 08_Rollup_Through_Hierarchy
   → source_file: "Balance sheet sample data.xlsx" (or P&L data if available)
   → formula_file: "S_formulas.xlsx"
   → amount_column: "Amount (company code  currency)"
   → source_mapping_column: "P&L Line"
   → target_rollup: "EBITDA"
   → show_detail: true
   → Result: Shows hierarchical breakdown of all formula elements contributing to EBITDA

Claude responds: "Here's the EBITDA calculation following your formula hierarchy:

EBITDA = €X,XXX,XXX
├── EBIT (s) = €X,XXX,XXX
│   ├── Statutory sales margin = €X,XXX,XXX
│   │   ├── Direct country margin = €X,XXX,XXX
│   │   │   ├── Direct country cost = (€XXX,XXX)
│   │   │   │   ├── Direct Sales - Travel expenses = (€XXX)
│   │   │   │   ├── Direct Sales - mgt fees and W&S = (€XXX)
│   │   │   │   └── ... (15 more elements)
...

Note: Your source data appears to be Balance Sheet focused. For P&L calculations,
you'll need transaction data with P&L Line classifications populated."
```

---

## Summary

This implementation plan provides a complete blueprint for building the MCP-Sample-Data server with:

- **37 tools** across 8 categories (including new Mapping category)
- **5 domain prompt sets** (Financial, Sales, Inventory, HR, Generic)
- **Full code templates** for all major components
- **Multi-file mapping support** for cross-file validation
- **Fuzzy matching** for automatic relationship discovery
- **Hierarchical rollup validation** for financial equations
- **Setup automation** for Claude Desktop integration
- **Testing strategy** with example tests
- **Usage examples** showing real-world workflows

The architecture follows the same patterns as your existing MCP-PowerBi-Finvision server, ensuring consistency and maintainability.

### Key Capabilities Added for Your Use Case

The new **Mapping Tools (Category 08)** specifically address your balance sheet validation scenario:

| Tool | Purpose | Your Use Case |
|------|---------|---------------|
| 08_Discover_Mapping | Fuzzy/semantic column matching | Links `GL Typology` to `Nominator element` automatically |
| 08_Load_Mapping_Context | Load multiple files together | Combines balance data + formulas + report structure |
| 08_Validate_Through_Mapping | Validate through hierarchies | Checks Assets = Liabilities + Equity |
| 08_Rollup_Through_Hierarchy | Calculate totals via formula | Computes EBITDA from formula hierarchy |
| 08_Compare_Structures | Gap analysis | Shows unmapped GL categories |
| 08_Define_Mapping | Manual mapping definition | Define explicit mappings when fuzzy match fails |

---

**Next Steps:**
1. Create the directory structure
2. Implement core modules (discovery, generation, validation, **mapping**)
3. Implement handlers for each tool category
4. Create domain prompts
5. Add templates
6. Test with sample data
7. Configure Claude Desktop
