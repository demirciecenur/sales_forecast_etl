"""
Test suite for ETL pipeline
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from src.etl.pipeline import ETLPipeline
from src.quality.validator import DataValidator
from src.transform.normalizer import DataNormalizer

@pytest.fixture
def sample_sales_data():
    """Create sample sales data for testing"""
    return pd.DataFrame({
        'PERIOD': ['202301', '202301', '202302'],
        'MATERIAL_NBR': ['M001', 'M002', 'M003'],
        'GROSS_SALES': [1000.0, 2000.0, 1500.0],
        'NET_SALES': [900.0, 1800.0, 1350.0],
        'REGION': ['AMERICAS', 'EMEA', 'ASIA']
    })

@pytest.fixture
def sample_forecast_data():
    """Create sample forecast data for testing"""
    return pd.DataFrame({
        'MATERIAL_NUMBER': ['M001', 'M002', 'M003'],
        'YEAR': [2023, 2023, 2023],
        'FORECAST_VAL': [3000.0, 4000.0, 3500.0]
    })

@pytest.fixture
def sample_config():
    return {
        'base_path': 'tests/data',
        'db_connection': {
            'host': 'localhost',
            'database': 'test_db'
        },
        'validation_rules': {
            'required_columns': ['id', 'name', 'value']
        }
    }

@pytest.fixture
def sample_input_files():
    return {
        'data1': 'sample1.csv',
        'data2': 'sample2.xlsx'
    }

class TestDataNormalizer:
    def test_sales_normalization(self, sample_sales_data):
        normalizer = DataNormalizer()
        result = normalizer.normalize_sales_data(sample_sales_data)
        
        assert 'period' in result.columns
        assert 'material_number' in result.columns
        assert 'gross_sales' in result.columns
        assert 'net_sales' in result.columns
        assert 'region_code' in result.columns
        assert 'year' in result.columns
        
        assert result['gross_sales'].dtype == np.float64
        assert result['net_sales'].dtype == np.float64
        assert all(result['gross_sales'] >= result['net_sales'])

    def test_forecast_normalization(self, sample_forecast_data):
        normalizer = DataNormalizer()
        result = normalizer.normalize_forecast_data(sample_forecast_data)
        
        assert 'material_number' in result.columns
        assert 'year' in result.columns
        assert 'forecast_value' in result.columns
        
        assert result['forecast_value'].dtype == np.float64
        assert result['year'].dtype == np.float64
        assert all(result['forecast_value'] > 0)

class TestDataValidator:
    def test_sales_validation(self, sample_sales_data):
        validator = DataValidator()
        assert validator.validate_sales_data(sample_sales_data)

    def test_forecast_validation(self, sample_forecast_data):
        validator = DataValidator()
        assert validator.validate_forecast_data(sample_forecast_data)

    def test_invalid_sales_data(self):
        validator = DataValidator()
        invalid_data = pd.DataFrame({
            'WRONG_COLUMN': []
        })
        assert not validator.validate_sales_data(invalid_data)

class TestETLPipeline:
    def test_config_reading(self, tmp_path):
        # Create temporary config file
        config_content = """
        AMERICAS_PATH=/path/to/americas.xlsx
        EMEA_PATH=/path/to/emea.xlsx
        ASIA_PATH=/path/to/asia.xlsx
        FORECAST_PATH=/path/to/forecast.xlsx
        """
        config_file = tmp_path / "dev.txt"
        config_file.write_text(config_content)
        
        pipeline = ETLPipeline()
        # Test implementation...

    def test_end_to_end(self, tmp_path, sample_sales_data, sample_forecast_data):
        # Create test files
        sales_file = tmp_path / "sales.xlsx"
        forecast_file = tmp_path / "forecast.xlsx"
        sample_sales_data.to_excel(sales_file, index=False)
        sample_forecast_data.to_excel(forecast_file, index=False)
        
        # Test implementation... 

def test_pipeline_successful_run(sample_config, sample_input_files):
    pipeline = ETLPipeline(sample_config)
    result = pipeline.run(sample_input_files)
    assert result == True

def test_pipeline_with_invalid_files(sample_config):
    pipeline = ETLPipeline(sample_config)
    result = pipeline.run({'invalid': 'nonexistent.csv'})
    assert result == False

def test_pipeline_with_invalid_data(sample_config, sample_input_files):
    # Test with data that fails validation
    pipeline = ETLPipeline(sample_config)
    result = pipeline.run({'bad_data': 'invalid_format.csv'})
    assert result == False 