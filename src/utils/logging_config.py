"""
Enhanced logging configuration
"""
import logging
from pathlib import Path
from typing import Optional

def setup_logging(log_dir: Optional[str] = None, level: int = logging.INFO):
    """Configure logging with file and console handlers"""
    if log_dir:
        log_path = Path(log_dir)
        log_path.mkdir(parents=True, exist_ok=True)
        log_file = log_path / "etl.log"
    else:
        log_file = Path("logs/etl.log")
        log_file.parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    ) 