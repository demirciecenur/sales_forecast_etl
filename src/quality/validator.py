"""
Data validation module
"""
import pandas as pd
import logging
from typing import Dict, List, Optional
from pathlib import Path
from datetime import datetime

class DataValidator:
    def __init__(self, config: dict = None):
        self.config = config or {}
        self._setup_logging()
        self.quality_metrics = {
            'total_records': 0,
            'invalid_sales': 0,
            'invalid_regions': 0
        }
        self.required_columns = {
            'sales': [
                'PERIOD', 
                'MATERIAL_NBR', 
                'GROSS_SALES', 
                'NET_SALES', 
                'REGION_CODE'
            ],
            'forecast': [
                'MATERIAL_NUMBER',
                'YEAR', 
                'FORECAST_VAL'  
            ]
        }

    def _setup_logging(self):
        """Configure logging format"""
        log_dir = Path('logs')
        log_dir.mkdir(exist_ok=True)
        
        logging.basicConfig(
            filename='logs/etl.log',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

    def validate_sales_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate sales data"""
        try:
            if df.empty:
                return pd.DataFrame()
            
            # Required fields check
            required_fields = ['PERIOD', 'MATERIAL_NBR', 'GROSS_SALES', 'NET_SALES', 'REGION_CODE']
            if not all(field in df.columns for field in required_fields):
                logging.error(f"Missing required fields. Found: {df.columns.tolist()}")
                return pd.DataFrame()
            
            # Remove rows with missing values
            valid_df = df.dropna(subset=required_fields)
            
            # Business rule: NET_SALES <= GROSS_SALES
            invalid_sales = valid_df[valid_df['NET_SALES'] > valid_df['GROSS_SALES']]
            if not invalid_sales.empty:
                logging.warning(f"Found {len(invalid_sales)} records with NET_SALES > GROSS_SALES")
                valid_df = valid_df[~valid_df.index.isin(invalid_sales.index)]
            
            return valid_df
        
        except Exception as e:
            logging.error(f"Error validating sales data: {str(e)}")
            return pd.DataFrame()

    def _validate_numeric_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate numeric columns"""
        try:
            numeric_columns = ['GROSS_SALES', 'NET_SALES']
            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                invalid_rows = df[df[col].isna()]
                if not invalid_rows.empty:
                    df = df[~df.index.isin(invalid_rows.index)]
            return df

        except Exception as e:
            logging.error(f"Error in numeric columns validation: {str(e)}")
            return pd.DataFrame()

    def _validate_business_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate business rules and return valid records"""
        try:
            valid_df = df.copy()
            total_rows = len(valid_df)
            
            # Check NET_SALES <= GROSS_SALES with tolerance
            tolerance = 0.01  # 1% tolerance
            invalid_sales = valid_df[
                valid_df['NET_SALES'] > valid_df['GROSS_SALES'] * (1 + tolerance)
            ]
            if not invalid_sales.empty:
                logging.warning(f"Found {len(invalid_sales)} records with NET_SALES > GROSS_SALES")
                valid_df = valid_df[~valid_df.index.isin(invalid_sales.index)]
            
            # Region code validation is now handled in normalizer
            
            if len(valid_df) < total_rows:
                logging.info(f"Removed {total_rows - len(valid_df)} invalid records after business rules validation")
            
            return valid_df

        except Exception as e:
            logging.error(f"Error in business rules validation: {str(e)}")
            return pd.DataFrame()

    def validate_forecast_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate forecast data and return valid records"""
        try:
            valid_df = df.copy()
            
            # Check for any required column variations
            required_cols = [
                ['MATERIAL_NUMBER', 'MATERIAL_NBR'],  # Either one is acceptable
                ['YEAR', 'PERIOD'],  # Either one is acceptable
                ['FORECAST_VAL', 'FORECAST_VALUE']  # Either one is acceptable
            ]
            
            # Check if at least one column from each required group exists
            for col_group in required_cols:
                if not any(col in valid_df.columns for col in col_group):
                    logging.error(f"Missing required column from: {col_group}")
                    return pd.DataFrame()
            
            # Convert numeric values
            for value_col in ['FORECAST_VAL', 'FORECAST_VALUE']:
                if value_col in valid_df.columns:
                    valid_df[value_col] = pd.to_numeric(valid_df[value_col], errors='coerce')
            
            # Remove rows with missing values
            valid_df = valid_df.dropna(subset=[col for col in valid_df.columns 
                                             if any(req in col for req in ['MATERIAL', 'FORECAST', 'YEAR', 'PERIOD'])])
            
            if valid_df.empty:
                logging.warning("No valid forecast records after validation")
                return pd.DataFrame()
            
            return valid_df

        except Exception as e:
            logging.error(f"Forecast data validation error: {str(e)}")
            return pd.DataFrame()