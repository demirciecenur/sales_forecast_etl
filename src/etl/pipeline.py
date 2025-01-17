"""
Core ETL pipeline implementation
"""
import pandas as pd
import logging
import yaml
from pathlib import Path
from typing import Dict, Optional
from ..transform.normalizer import DataNormalizer
from ..database.loader import DatabaseLoader
from src.extract.extract import DataExtractor
from src.quality.validator import DataValidator

class ETLPipeline:
    def __init__(self, config: Dict = None, config_path: str = "config/dev.yaml"):
        """
        Initialize ETL components with configuration
        """
        if config is not None:
            self.config = config
        else:
            self.config = self._load_config(config_path)
            
        self.extractor = DataExtractor(base_path=str(Path.cwd()))
        self.validator = DataValidator()
        self.normalizer = DataNormalizer(self.config.get('business_rules', {}))
        self.db_loader = DatabaseLoader(
            db_path=self.config['database']['path']
        )

    def _load_config(self, config_path: str) -> dict:
        """Load configuration from YAML file"""
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            logging.info(f"Configuration loaded from {config_path}")
            return config
        except Exception as e:
            logging.error(f"Error loading config from {config_path}: {e}")
            raise

    def _read_file(self, file_path: str) -> Optional[pd.DataFrame]:
        """Read data file based on extension"""
        try:
            path = Path(file_path)
            if not path.exists():
                logging.error(f"File not found: {file_path}")
                return None

            if path.suffix.lower() == '.csv':
                df = pd.read_csv(file_path, encoding='utf-8')
                logging.info(f"Successfully read CSV file: {file_path}")
                return df
            elif path.suffix.lower() in ['.xlsx', '.xls']:
                df = pd.read_excel(file_path)
                logging.info(f"Successfully read Excel file: {file_path}")
                return df
            else:
                logging.error(f"Unsupported file format: {path.suffix}")
                return None

        except Exception as e:
            logging.error(f"Error reading file {file_path}: {str(e)}")
            return None

    def run(self, input_files: Dict) -> bool:
        """Run the complete ETL pipeline"""
        try:
            # Process sales data
            if 'sales' in input_files:
                all_sales = []
                for region, path in input_files['sales'].items():
                    df = self.extractor.read_file(path)
                    if df is not None and not df.empty:
                        # Validate data
                        valid_df = self.validator.validate_sales_data(df)
                        if not valid_df.empty:
                            # Pass file path to normalizer
                            normalized_df = self.normalizer.normalize_sales_data(valid_df, file_path=path)
                            if not normalized_df.empty:
                                all_sales.append(normalized_df)
                                logging.info(f"Processed {region} data: {len(normalized_df)} rows")
                        else:
                            logging.warning(f"No valid data found in {path}")

                if all_sales:
                    sales_df = pd.concat(all_sales, ignore_index=True)
                    if not self.db_loader.load_sales_data(sales_df):
                        logging.error("Sales data loading failed")
                        return False
                else:
                    logging.error("No valid sales data to process")
                    return False

            # Process forecast data
            if 'forecast' in input_files:
                forecast_path = input_files['forecast']
                forecast_df = self.extractor.read_file(forecast_path)
                if forecast_df is not None and not forecast_df.empty:
                    valid_forecast = self.validator.validate_forecast_data(forecast_df)
                    if not valid_forecast.empty:
                        normalized_forecast = self.normalizer.normalize_forecast_data(valid_forecast)
                        if not normalized_forecast.empty:
                            if not self.db_loader.load_forecast_data(normalized_forecast):
                                logging.error("Forecast data loading failed")
                                return False
                        else:
                            logging.error("No valid forecast data after normalization")
                            return False
                    else:
                        logging.error("No valid forecast data after validation")
                        return False

            return True

        except Exception as e:
            logging.error(f"Pipeline execution failed: {str(e)}")
            return False