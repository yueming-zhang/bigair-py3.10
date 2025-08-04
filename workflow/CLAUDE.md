# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a BigAir workflow project focused on data processing with Presto/Hive integration. The project contains workflow definitions using the BigAir framework and utilities for database operations.

## Key Dependencies

The project uses Python 3.10 with these core dependencies:
- `bigair-client`: Workflow orchestration framework (installed as `bigair`)
- `pyhive`: Presto/Hive database connectivity
- `pandas`: Data manipulation
- `boto3`: AWS SDK for identity/role management
- `pytest`: Testing framework
- `requests`: HTTP client library

## Development Commands

### Environment Setup
```bash
# Activate the virtual environment (if not already active)
source /Users/ming_yuezhang/Dev/bigair-py3.10/.venv/bin/activate

# Install dependencies
pip install -r /Users/ming_yuezhang/Dev/bigair-py3.10/requirements.txt
```

### Testing
```bash
# Run tests using pytest
python3 -m pytest

# Note: Currently no tests are configured in this project
```

### Running Workflows
Workflows are not executed directly as Python scripts. They use the BigAir framework for orchestration through their platform.

## Code Architecture

### BigAir Framework Integration
- **Workflow Definition**: Uses `@workflow` decorator to define data processing pipelines
- **Step Definition**: Uses `@step` decorator for individual processing units
- **Type Safety**: Leverages `OutputTuple` for typed step outputs
- **Context Handling**: Steps receive context parameters for runtime information

### Core Components

#### 1. Workflow Files (`hello_world.py`)
- `hello_workflow()`: Main workflow definition with step orchestration
- `print_runtime_info()`: AWS identity and environment introspection step
- `step_one()` / `step_two()`: Example data processing steps
- Context access pattern: Steps receive `**context` for runtime metadata

#### 2. Database Integration (`hive_presto_client.py`)
- **Connection Management**: Thread-safe connection pooling with caching
- **Configuration**: Connects to `presto-gateway-production.presto-gateway-production:6375`
- **Schema**: Targets `silver` catalog, primarily works with `tmp` schema tables
- **Connection Lifecycle**: 
  - Max age: 600 seconds
  - Max reuse: 10 times per connection
  - Timeout: 300 seconds

### Database Operations Architecture

#### Core Functions
- `execute_hive_query(sql)`: Execute raw SQL with connection management
- `get_table_columns(table_name)`: Retrieve and cache table schema
- `insert_data(table_name, json_data, lead_id)`: Single record insertion
- `insert_data_batch(table_name, json_data_array, lead_id)`: Batch operations
- `create_hive_table(table_name)`: Table creation from SQL files

#### Data Mapping Strategy
- **JSON to Table**: Automatic field mapping with case-insensitive matching
- **Nested Data**: Supports 2-level nesting with underscore notation (`parent_child`)
- **Type Handling**: Automatic type conversion for numeric vs. string columns
- **Special Fields**: Auto-generation of `id` (UUID), timestamps, and date partitions

#### Threading and Caching
- **Thread Safety**: Each thread maintains its own database connection
- **Schema Caching**: Table column information cached globally with thread-safe access
- **Connection Pool**: Automatic cleanup and connection validation

### File Structure Patterns

The project expects SQL files for table creation in a `hive_sql/` subdirectory (referenced but not present in current structure).

## Important Notes

- **AWS Integration**: Workflows introspect AWS identity using boto3 for debugging/monitoring
- **Date Handling**: Uses `yesterday_ds` (2 days ago) for data partitioning
- **Security**: Careful handling of credentials - environment variables logged by name only
- **Error Handling**: Comprehensive logging with prefixed messages ("CGE -")
- **Connection Management**: Automatic connection cleanup prevents resource leaks