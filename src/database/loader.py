
import pandas as pd
import sqlite3
import logging
from pathlib import Path

class DatabaseLoader:
    def __init__(self, db_path: str = "data/sales_forecast.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self):
        """Initialize database schema"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                with open('sql/schema.sql', 'r') as f:
                    conn.executescript(f.read())
                logging.info("Database schema initialized")
        except Exception as e:
            logging.error(f"Database initialization failed: {e}")
            raise

    def load_sales_data(self, df: pd.DataFrame) -> bool:
        """Load sales data into database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Standardize material numbers before loading
                df['material_number'] = df['material_number'].apply(self._standardize_material_number)
                
                # Load dimensions first
                self._load_dimensions(conn, df)
                
                # Prepare fact table data
                fact_sales = df.copy()
                
                # Get dimension IDs with standardized material numbers
                fact_sales['material_id'] = self._get_dimension_id(
                    conn, 
                    'dim_material', 
                    'material_number', 
                    fact_sales['material_number']
                )
                fact_sales['time_id'] = self._get_dimension_id(
                    conn, 
                    'dim_time', 
                    'period', 
                    fact_sales['period']
                )
                
                # Select columns for fact table
                fact_sales = fact_sales[[
                    'material_id',
                    'time_id',
                    'region_code',
                    'gross_sales',
                    'net_sales'
                ]]
                
                # Load fact table
                fact_sales.to_sql('fact_sales', conn, if_exists='append', index=False)
                logging.info(f"Loaded {len(fact_sales)} sales records")
                return True
                
        except Exception as e:
            logging.error(f"Error loading sales data: {e}")
            return False

    def _standardize_material_number(self, value: any) -> str:
        """Standardize material number format"""
        try:
            # Remove any non-numeric characters and decimal points
            clean_value = str(value).replace('.0', '')
            clean_value = ''.join(filter(str.isdigit, clean_value))
            
            # Validate length
            if len(clean_value) > 0:
                if len(clean_value) > 8:
                    clean_value = clean_value[:8]  # Truncate if too long
                return clean_value.zfill(8)  # Pad with leading zeros
            
            logging.warning(f"Invalid material number format: {value}")
            return str(value)
            
        except Exception as e:
            logging.warning(f"Error standardizing material number {value}: {e}")
            return str(value)

    def load_forecast_data(self, df: pd.DataFrame) -> bool:
        """Load forecast data into database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # First standardize material numbers
                df['material_number'] = df['material_number'].apply(self._standardize_material_number)
                
                # Create cross product of materials and years for dimension loading
                unique_materials = df['material_number'].unique()
                unique_years = df['year'].unique()
                
                # Create dimension data using cross product
                material_years = pd.DataFrame([(m, y) for m in unique_materials for y in unique_years],
                                            columns=['material_number', 'year'])
                
                # Load dimensions
                self._load_dimensions(conn, material_years, data_type='forecast')
                
                # Get dimension IDs
                df['material_id'] = self._get_dimension_id(
                    conn, 'dim_material', 'material_number', df['material_number']
                )
                
                # Create period from year for time dimension lookup
                df['period'] = df['year'].astype(str) + '.01'
                df['time_id'] = self._get_dimension_id(
                    conn, 'dim_time', 'period', df['period']
                )

                # Prepare fact table data
                fact_forecast = df[[
                    'material_id',
                    'time_id',
                    'forecast_value'
                ]].copy()
                
                # Validate data before loading
                fact_forecast = fact_forecast.dropna()
                if fact_forecast.empty:
                    logging.error("No valid forecast records to load after validation")
                    return False
                    
                # Log data stats
                logging.info(f"Forecast data statistics:")
                logging.info(f"Total records: {len(fact_forecast)}")
                logging.info(f"Unique materials: {fact_forecast['material_id'].nunique()}")
                logging.info(f"Unique time periods: {fact_forecast['time_id'].nunique()}")
                
                # Load fact table
                fact_forecast.to_sql('fact_forecast', conn, if_exists='append', index=False)
                logging.info(f"Successfully loaded {len(fact_forecast)} forecast records")
                return True

        except Exception as e:
            logging.error(f"Error loading forecast data: {e}")
            logging.debug("DataFrame info:", exc_info=True)
            logging.debug(f"DataFrame columns: {df.columns.tolist()}")
            logging.debug(f"DataFrame shape: {df.shape}")
            return False

    def _load_dimensions(self, conn: sqlite3.Connection, df: pd.DataFrame, data_type: str = 'sales'):
        """Load dimension tables incrementally with better error handling"""
        try:
            # Load materials incrementally with standardization
            materials = df[['material_number']].drop_duplicates().reset_index(drop=True)
            materials['material_number'] = materials['material_number'].apply(self._standardize_material_number)
            
            # Get existing materials with standardized numbers
            existing_materials = pd.read_sql(
                "SELECT material_number FROM dim_material",
                conn
            )
            existing_materials['material_number'] = existing_materials['material_number'].apply(self._standardize_material_number)
            
            # Find new materials
            new_materials = materials[~materials['material_number'].isin(existing_materials['material_number'])]
            if not new_materials.empty:
                logging.info(f"Sample of new materials to be loaded: {new_materials['material_number'].head().tolist()}")
                new_materials.to_sql('dim_material', conn, if_exists='append', index=False)
                logging.info(f"Loaded {len(new_materials)} new materials")
            
            # Handle time periods based on data type
            if data_type == 'sales':
                if 'period' in df.columns and 'year' in df.columns:
                    periods = df[['period', 'year']].drop_duplicates().reset_index(drop=True)
                else:
                    logging.warning("Missing period/year columns in sales data")
                    return
            else:  # forecast
                if 'year' in df.columns:
                    years = pd.Series(df['year'].unique()).sort_values()
                    periods = pd.DataFrame({
                        'year': years,
                        'period': years.astype(str) + '.01'
                    }).reset_index(drop=True)
                else:
                    logging.warning("Missing year column in forecast data")
                    return
            
            # Get existing periods
            existing_periods = pd.read_sql("SELECT period FROM dim_time", conn)
            
            # Find and load new periods
            if not periods.empty:
                new_periods = periods[~periods['period'].isin(existing_periods['period'])]
                if not new_periods.empty:
                    new_periods.to_sql('dim_time', conn, if_exists='append', index=False)
                    logging.info(f"Loaded {len(new_periods)} new time periods")

        except Exception as e:
            logging.error(f"Error loading dimensions: {str(e)}")
            raise

    def _get_dimension_id(self, conn: sqlite3.Connection, table: str, key_column: str, values: pd.Series) -> pd.Series:
        """Get dimension IDs with better error handling"""
        try:
            # Standardize material numbers if getting material IDs
            if table == 'dim_material':
                values = values.apply(self._standardize_material_number)
            
            query = f"SELECT {key_column}, {table.split('_')[1]}_id FROM {table}"
            dim_df = pd.read_sql(query, conn)
            
            # Standardize lookup values if material
            if table == 'dim_material':
                dim_df[key_column] = dim_df[key_column].apply(self._standardize_material_number)
            
            id_map = dict(zip(dim_df[key_column], dim_df[f"{table.split('_')[1]}_id"]))
            
            # Map values and handle missing mappings
            mapped_ids = values.map(id_map)
            if mapped_ids.isna().any():
                missing_values = values[~values.isin(id_map.keys())].unique()
                if table == 'dim_region':
                    logging.error(f"Invalid region codes found: {missing_values}")
                    raise ValueError(f"Invalid region codes: {missing_values}")
                
                # For materials, try to load missing values
                if table == 'dim_material':
                    missing_df = pd.DataFrame({key_column: missing_values})
                    self._load_dimensions(conn, missing_df)
                    # Retry mapping after loading
                    return self._get_dimension_id(conn, table, key_column, values)
                
                raise ValueError(f"Missing {table} mappings for values: {missing_values}")
            
            return mapped_ids
            
        except Exception as e:
            logging.error(f"Error getting dimension IDs for {table}: {e}")
            raise
