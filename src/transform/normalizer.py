import pandas as pd
import logging
from typing import Optional

class DataNormalizer:
    def __init__(self, business_rules: dict = None):
        self.business_rules = business_rules or {}

    def normalize_sales_data(self, df: pd.DataFrame, file_path: str = None) -> pd.DataFrame:
        """Fix data problems and standartize format"""
        try:
            # Make copy to avoid changing original
            norm_df = df.copy()
            
            # Fix column names - they are always different :(
            col_map = {
                'MATERIAL': 'material_number',  # why do they use different names??
                'MATERIAL_NO': 'material_number',
                'MATERIAL_NUMBER': 'material_number',
                'SALES_GROSS': 'gross_sales', 
                'SALES_NET': 'net_sales'
            }
            
            # Column mapping
            column_mapping = {
                'PERIOD': 'period',
                'MATERIAL_NBR': 'material_number',
                'GROSS_SALES': 'gross_sales',
                'NET_SALES': 'net_sales',
                'REGION_CODE': 'region_code'
            }
            
            # Rename columns
            norm_df = norm_df.rename(columns=column_mapping)
            
            # Map region codes based on file path
            if file_path:
                file_path = file_path.lower()
                if 'asia' in file_path:
                    norm_df['region_code'] = '4'  # Asia Pacific
                elif 'emea' in file_path:
                    norm_df['region_code'] = '1'  # EMEA
                elif 'americas' in file_path:
                    norm_df['region_code'] = '2'  # Americas
                else:
                    # Use existing mapping for unknown sources
                    region_mapping = {
                        '0': '4', '5': '4', '6': '4', '7': '4',  # Asia variants
                        '1': '1',  # EMEA
                        '2': '2',  # Americas
                        '4': '4'   # Asia Pacific
                    }
                    norm_df['region_code'] = norm_df['region_code'].astype(str).map(region_mapping)
            
            # Data type conversions
            norm_df['period'] = norm_df['period'].astype(str).str.replace('.', '')
            norm_df['year'] = norm_df['period'].str[:4].astype(int)
            norm_df['material_number'] = norm_df['material_number'].astype(str).str.strip()
            
            # Select final columns
            final_columns = [
                'period', 'material_number', 'gross_sales', 
                'net_sales', 'region_code', 'year'
            ]
            
            return norm_df[final_columns]

        except Exception as e:
            logging.error(f"Error normalizing sales data: {str(e)}")
            return pd.DataFrame()

    def _map_regions(self, df: pd.DataFrame, mapping: dict) -> Optional[pd.DataFrame]:
        """Map region codes and names"""
        try:
            # Handle different region column scenarios
            if 'region_description' in df.columns:
                # Map from region description
                df['region_code'] = df['region_description'].apply(
                    lambda x: next((v['code'] for k, v in mapping.items() 
                                  if k in str(x).upper()), 'UNKNOWN')
                )
                df['region_name'] = df['region_description'].apply(
                    lambda x: next((v['name'] for k, v in mapping.items() 
                                  if k in str(x).upper()), 'Unknown')
                )
            elif 'region_code' in df.columns:
                # Map from region code
                code_to_name = {
                    '1': 'Europe, Middle East and Africa',
                    '2': 'Americas',
                    '4': 'Asia Pacific'
                }
                df['region_name'] = df['region_code'].map(code_to_name)
            else:
                logging.error("No region information found in data")
                return None
            
            # Clean up region data
            df['region_code'] = df['region_code'].fillna('UNKNOWN')
            df['region_name'] = df['region_name'].fillna('Unknown')
            
            # Filter out invalid regions
            valid_df = df[
                (df['region_code'] != 'UNKNOWN') & 
                (df['region_name'] != 'Unknown')
            ].copy()
            
            if valid_df.empty:
                logging.error("No valid regions found after mapping")
                return None
            
            return valid_df
        
        except Exception as e:
            logging.error(f"Error mapping regions: {str(e)}")
            return None

    def normalize_forecast_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize forecast data to match database schema"""
        try:
            normalized_df = df.copy()
            
            # Standardize column names
            column_mapping = {
                'MATERIAL_NUMBER': 'material_number',
                'MATERIAL_NBR': 'material_number',  # Alternatif kolon adı
                'FORECAST_VAL': 'forecast_value',
                'FORECAST_VALUE': 'forecast_value'  # Alternatif kolon adı
            }
            
            # Rename only existing columns
            for old_col, new_col in column_mapping.items():
                if old_col in normalized_df.columns:
                    normalized_df = normalized_df.rename(columns={old_col: new_col})
            
            # Convert year to proper format if it's in period format
            if 'PERIOD' in normalized_df.columns:
                normalized_df['year'] = normalized_df['PERIOD'].astype(str).str[:4].astype(int)
            elif 'YEAR' in normalized_df.columns:
                normalized_df['year'] = normalized_df['YEAR']
            
            # Ensure numeric values
            if 'forecast_value' in normalized_df.columns:
                normalized_df['forecast_value'] = pd.to_numeric(normalized_df['forecast_value'], errors='coerce')
            
            # Select and validate final columns
            required_columns = ['material_number', 'year', 'forecast_value']
            if not all(col in normalized_df.columns for col in required_columns):
                logging.error(f"Missing required columns after normalization. Found: {normalized_df.columns.tolist()}")
                return pd.DataFrame()
            
            return normalized_df[required_columns]

        except Exception as e:
            logging.error(f"Error normalizing forecast data: {str(e)}")
            return pd.DataFrame()