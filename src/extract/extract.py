import pandas as pd
import logging
from pathlib import Path
from typing import Optional, Dict

class DataExtractor:
    def __init__(self, base_path: Optional[str] = None):
        self.base_path = Path(base_path) if base_path else Path.cwd()
        # Column standardization mapping
        self.column_mapping = {
            'MATERIAL': 'MATERIAL_NBR',
            'MATERIAL_NO': 'MATERIAL_NBR',
            'MATERIAL_NUMBER': 'MATERIAL_NBR',
            'SALES_GROSS': 'GROSS_SALES',
            'SALES_NET': 'NET_SALES',
            'REGION': 'REGION_CODE',
            'REGION_CD': 'REGION_CODE'
        }
        # Configure logging
        self._setup_logging()

    def _setup_logging(self):
        """Configure logging format"""
        logging.basicConfig(
            filename='logs/etl.log',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

    def _read_csv(self, file_path: Path, **kwargs) -> Optional[pd.DataFrame]:
        """Try different encodings becuse of excel export problems"""
        encodings = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252'] 
        
        for enc in encodings:
            try:
                df = pd.read_csv(file_path, encoding=enc, **kwargs)
                logging.info(f"Read CSV file: {file_path} with {enc}")
                return df
            except UnicodeDecodeError:
                continue # try next encoding
            except Exception as e:
                logging.error(f"Error reading CSV {file_path} with {enc}: {e}")
                continue
        
        logging.error(f"Failed to read CSV with any encoding: {file_path}")
        return None

    def _read_excel(self, file_path: Path, **kwargs) -> Optional[pd.DataFrame]:
        """Read Excel file with error handling"""
        try:
            df = pd.read_excel(file_path, **kwargs)
            logging.info(f"Successfully read Excel file: {file_path}")
            return df
        except Exception as e:
            logging.error(f"Error reading Excel {file_path}: {e}")
            return None

    def validate_path(self, file_path: Path) -> bool:
        """Validate file path and format"""
        try:
            if not file_path.exists():
                logging.error(f"File not found: {file_path}")
                return False
            
            if not file_path.is_file():
                logging.error(f"Not a file: {file_path}")
                return False
                
            if file_path.suffix.lower() not in self.supported_formats:
                logging.error(f"Unsupported file format: {file_path.suffix}")
                return False
                
            return True
            
        except Exception as e:
            logging.error(f"Error validating path {file_path}: {e}")
            return False

    def read_file(self, file_path: str) -> Optional[pd.DataFrame]:
        """Read data file based on extension"""
        try:
            path = self.base_path / file_path
            if not path.exists():
                logging.error(f"File not found: {file_path}")
                return None

            if path.suffix.lower() == '.csv':
                df = pd.read_csv(path, encoding='utf-8')
                df = self._standardize_columns(df)
                logging.info(f"Successfully read CSV file: {file_path}, {len(df)} rows")
                return df
                
            elif path.suffix.lower() in ['.xlsx', '.xls']:
                all_sheets = []
                excel_file = pd.ExcelFile(path)
                
                for sheet in excel_file.sheet_names:
                    try:
                        if not sheet.isdigit() or len(sheet) != 4:
                            logging.debug(f"Skipping non-year sheet: {sheet}")
                            continue
                            
                        sheet_df = pd.read_excel(path, sheet_name=sheet)
                        sheet_df = self._standardize_columns(sheet_df)
                        
                        # Add period information
                        sheet_df['PERIOD'] = f"{sheet}.01"
                        if 'YEAR' not in sheet_df.columns:
                            sheet_df['YEAR'] = int(sheet)
                            
                        all_sheets.append(sheet_df)
                        logging.info(f"Read sheet {sheet} from {file_path}: {len(sheet_df)} rows")
                        
                    except Exception as e:
                        logging.error(f"Error reading sheet {sheet} from {file_path}: {e}")
                        continue

                if not all_sheets:
                    logging.error(f"No valid sheets found in {file_path}")
                    return None
                    
                combined_df = pd.concat(all_sheets, ignore_index=True)
                logging.info(f"Successfully combined {len(all_sheets)} sheets from {file_path}, total {len(combined_df)} rows")
                return combined_df
                
            else:
                logging.error(f"Unsupported file format: {path.suffix}")
                return None

        except Exception as e:
            logging.error(f"Error reading file {file_path}: {str(e)}")
            return None

    def read_files(self, file_paths: Dict[str, str], **kwargs) -> Dict[str, pd.DataFrame]:
        """
        Read multiple files and return as dictionary
        
        Args:
            file_paths: Dictionary of {name: path} pairs
            **kwargs: Additional arguments passed to read functions
            
        Returns:
            Dictionary of {name: DataFrame} pairs
        """
        results = {}
        for name, path in file_paths.items():
            df = self.read_file(path, **kwargs)
            if df is not None and not df.empty:
                results[name] = df
            else:
                logging.warning(f"No valid data read for {name} from {path}")
        return results 

    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names"""
        # Strip whitespace and convert to uppercase
        df.columns = df.columns.str.strip().str.upper()
        
        # Apply column mapping
        for old_col, new_col in self.column_mapping.items():
            if old_col in df.columns:
                df = df.rename(columns={old_col: new_col})
                
        # Log column names for debugging
        logging.debug(f"Standardized columns: {df.columns.tolist()}")
        
        return df