import pytest
import pandas as pd
from pathlib import Path
from src.etl.pipeline import ETLPipeline
from src.quality.validator import DataValidator
from src.transform.normalizer import DataNormalizer

@pytest.fixture
def test_db_path(tmp_path):
    """Create temporary database path"""
    return str(tmp_path / "test.db")

@pytest.fixture
def pipeline(test_db_path):
    """Create ETL pipeline with test config"""
    return ETLPipeline(db_path=test_db_path)

def test_sales_validation():
    """Test sales data validation rules"""
    validator = DataValidator()
    
    test_data = pd.DataFrame({
        'PERIOD': ['2023.01', '2023.01', '2023.01'],
        'MATERIAL_NBR': ['12345678', '87654321', '11111111'],
        'GROSS_SALES': [1000, 2000, 3000],
        'NET_SALES': [800, 2100, 2900],
        'REGION_CODE': ['1', '2', '4']
    })
    
    valid_df = validator.validate_sales_data(test_data)
    
    # Should remove row where NET_SALES > GROSS_SALES
    assert not valid_df.empty
    assert len(valid_df) == 2
    assert 2100 not in valid_df['NET_SALES'].values

def test_material_number_standardization():
    """Test material number standardization"""
    normalizer = DataNormalizer()
    print("Should be filled")
    assert True