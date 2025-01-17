# Sales & Forecast Data Integration Project

## Project Overview

### Business Context

The company's sales and forecasting teams work with separate Excel files, each maintaining their own format. The challenge is to create a unified, quality-assured database that serves as the backend for a web application comparing sales against forecasts.

For detailed documentation:

- [Data Quality Rules](docs/data_quality.md)
- [Database Schema](docs/ER_diyagram_.png)
- [Automation Process](docs/automation.md)
- [Business Decisions](docs/decisions.md)

### Technical Solution

#### 1. Data Architecture

- **Dimensional Model**: Implements star schema for efficient querying

  - Fact tables: fact_sales, fact_forecast
  - Dimension tables: dim_material, dim_time, dim_region

#### 2. Data Quality Framework

- **Validation Rules**:

  - Required fields validation
  - Business rule validation (e.g., net sales ≤ gross sales)
  - Material number standardization (8-digit format)
  - Region code validation (1=EMEA, 2=Americas, 4=Asia)

- **Error Handling**:
  - Invalid record removal
  - Detailed error logging
  - Data quality reporting

#### 3. Data Standardization

- **Material Numbers**:
  - 8-digit format
  - Leading zero padding
  - Decimal point removal
- **Time Periods**:

  - YYYY.MM format
  - Year extraction
  - Period validation

- **Region Codes**:
  - Standardized mapping
  - File-based assignment
  - Validation rules

## Technical Implementation

### Directory Structure

### Key Components

1. **Data Extraction**

   - Handles multiple file formats (Excel, CSV)
   - Error handling and logging
   - File integrity checks

2. **Data Quality Framework**

   - Rule-based validation engine
   - Custom validation rules
   - Quality reporting

3. **Data Transformation**

   - Business logic implementation
   - Data standardization
   - Aggregation rules

4. **Database Layer**
   - Star schema implementation
   - Optimized indexes
   - Materialized views for performance

## Setup and Usage

1. Environment Setup

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

2. Configuration

```bash
# Update config/dev.txt with file paths
# Update config/data_types.yaml with validation rules
```

3. Run Pipeline

```bash
python main.py
```

## Testing Strategy

1. **Unit Tests**

   - Data extraction validation
   - Quality rule testing
   - Transformation logic verification

2. **Integration Tests**

3. **Quality Assurance**
   - Data quality metrics
   - Business rule validation
   - Cross-reference checking

## Monitoring and Maintenance

1. **Logging**

   - Detailed process logging
   - Error tracking
   - Performance metrics

2. **Quality Monitoring**
   - Data quality dashboards
   - Validation rule monitoring
   - Error rate tracking
