# Design Decisions

## Data Normalization

### Material Numbers

```python
def _standardize_material_number(self, value: any) -> str:
    """Standardize material number format"""
    clean_value = str(value).replace('.0', '')
    clean_value = ''.join(filter(str.isdigit, clean_value))
    return clean_value.zfill(8)  # Pad with leading zeros
```

- **Decision**: Standardize to 8-digit format
- **Rationale**: Ensures consistency across regions
- **Reference**: src/database/loader.py:\_standardize_material_number

### Time Periods

```sql
CREATE TABLE dim_time (
    time_id INTEGER PRIMARY KEY AUTOINCREMENT,
    period TEXT NOT NULL UNIQUE,
    year INTEGER NOT NULL
);
```

- **Decision**: Use YYYY.MM format
- **Rationale**: Supports both monthly and yearly analysis
- **Reference**: sql/schema.sql:dim_time

## Business Rules

1. Region Codes

   - EMEA: '1'
   - Americas: '2'
   - Asia Pacific: '4'
   - **Rationale**: Maintain data consistency
   - **Reference**: src/quality/validator.py:\_validate_business_rules

2. Sales Validation
   - NET_SALES ≤ GROSS_SALES (with 1% tolerance)
   - **Rationale**: Business logic requirement
   - **Reference**: src/quality/validator.py:\_validate_business_rules
