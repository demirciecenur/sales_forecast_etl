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
        """Validate sales data and return valid records"""
        try:
            valid_df = df.copy()
            total_rows = len(valid_df)
            
            # Check required columns
            missing_cols = [col for col in self.required_columns['sales'] 
                          if col not in valid_df.columns]
            if missing_cols:
                logging.error(f"Missing required columns in sales data: {missing_cols}")
                logging.info(f"Available columns: {valid_df.columns.tolist()}")
                return pd.DataFrame()

            # Remove rows with missing required fields
            required_fields_mask = valid_df[self.required_columns['sales']].notna().all(axis=1)
            valid_df = valid_df[required_fields_mask]
            
            if len(valid_df) < total_rows:
                logging.info(f"Removed {total_rows - len(valid_df)} rows with missing required fields")

            # Business rules validation
            valid_df = self._validate_business_rules(valid_df)
            
            if not valid_df.empty:
                logging.info(f"Validated {len(valid_df)} sales records successfully")
            
            return valid_df

        except Exception as e:
            logging.error(f"Sales data validation error: {str(e)}")
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