# Data Quality Framework

## Validation Rules

1. Required Fields

```python
required_columns = {
    'sales': ['PERIOD', 'MATERIAL_NBR', 'GROSS_SALES', 'NET_SALES', 'REGION_CODE'],
    'forecast': ['MATERIAL_NUMBER', 'YEAR', 'FORECAST_VAL']
}
```

- **Reference**: src/quality/validator.py:required_columns

2. Data Type Validation

- Material Numbers: Standardized 8-digit format
- Sales Values: Decimal(18,2)
- Dates: YYYY.MM format

## Error Handling

1. Missing Data

```python
def validate_sales_data(self, df: pd.DataFrame) -> pd.DataFrame:
    required_fields_mask = df[self.required_columns['sales']].notna().all(axis=1)
    valid_df = df[required_fields_mask]
```

- **Reference**: src/quality/validator.py:validate_sales_data

2. Invalid Values

- Log invalid records
- Skip invalid records
- Continue processing valid data

## Monitoring

1. Quality Metrics

- Total records processed
- Invalid records count
- Processing time
- Error rates

2. Logging

```python
logging.info(f"Forecast data statistics:")
logging.info(f"Total records: {len(fact_forecast)}")
logging.info(f"Unique materials: {fact_forecast['material_id'].nunique()}")
```
