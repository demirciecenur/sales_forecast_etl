"""
Main entry point for the ETL pipeline
"""
import logging
import yaml
import argparse
import os
from src.etl.pipeline import ETLPipeline

DEFAULT_CONFIG_PATH = 'config/dev.yaml'

def load_config(config_path: str = DEFAULT_CONFIG_PATH) -> dict:
    """
    Load configuration from YAML file
    """
    try:          
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logging.error(f"Error loading config file: {e}")
        raise

def main():

    try:
        # Load configuration
        config = load_config(DEFAULT_CONFIG_PATH)
        
        # Initialize and run pipeline
        pipeline = ETLPipeline(config=config)
        
        # Get input files from config
        input_files = config.get('input_files', {})
        
        # Run pipeline
        success = pipeline.run(input_files)
        
        if success:
            logging.info("ETL pipeline completed successfully")
            return 0
        else:
            logging.error("ETL pipeline failed")
            return 1
            
    except Exception as e:
        logging.error(f"ETL pipeline failed: {str(e)}")
        return 1

if __name__ == "__main__":
    exit(main()) 