"""
Batch ETL Pipeline Orchestrator
Runs the complete Extract-Transform-Load pipeline
"""

import logging
import argparse
from datetime import datetime
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from spark_extract import SparkExtractor
from spark_transform import SparkTransformer
from spark_load import SparkLoader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BatchETLPipeline:
    """Orchestrates the complete batch ETL pipeline"""
    
    def __init__(self, test_mode=False):
        """
        Initialize pipeline
        
        Args:
            test_mode: If True, runs in test mode with limited data
        """
        self.test_mode = test_mode
        self.extractor = SparkExtractor()
        self.transformer = SparkTransformer()
        self.loader = SparkLoader()
        
        logger.info(f"Batch ETL Pipeline initialized (test_mode={test_mode})")
    
    def run(self):
        """
        Execute the complete ETL pipeline
        
        Returns:
            Boolean indicating success
        """
        start_time = datetime.now()
        logger.info("="*80)
        logger.info("BATCH ETL PIPELINE STARTED")
        logger.info(f"Start Time: {start_time}")
        logger.info("="*80)
        
        try:
            # EXTRACT PHASE
            logger.info("\n" + "="*80)
            logger.info("PHASE 1: EXTRACT")
            logger.info("="*80)
            
            data = self.extractor.extract_all_sources()
            
            if data['sensor_readings'] is None:
                logger.error("Failed to extract sensor readings. Aborting pipeline.")
                return False
            
            # TRANSFORM PHASE
            logger.info("\n" + "="*80)
            logger.info("PHASE 2: TRANSFORM")
            logger.info("="*80)
            
            transformed_df = self.transformer.transform_all(data)
            
            # In test mode, limit data
            if self.test_mode:
                logger.info("Test mode: Limiting to 1000 rows")
                transformed_df = transformed_df.limit(1000)
            
            # LOAD PHASE
            logger.info("\n" + "="*80)
            logger.info("PHASE 3: LOAD")
            logger.info("="*80)
            
            success = self.loader.load_all(transformed_df, data)
            
            # SUMMARY
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info("\n" + "="*80)
            logger.info("BATCH ETL PIPELINE COMPLETED")
            logger.info(f"End Time: {end_time}")
            logger.info(f"Duration: {duration:.2f} seconds")
            logger.info(f"Status: {'SUCCESS' if success else 'FAILED'}")
            logger.info("="*80)
            
            return success
        
        except Exception as e:
            logger.error(f"Pipeline failed with error: {e}", exc_info=True)
            return False
        
        finally:
            # Cleanup
            self.extractor.stop()
    
    def validate_pipeline(self):
        """
        Validate pipeline configuration and connections
        
        Returns:
            Boolean indicating if validation passed
        """
        logger.info("Validating pipeline configuration...")
        
        # TODO: Add validation checks
        # - Check database connections
        # - Verify Spark cluster is available
        # - Check required tables exist
        
        logger.info("Validation completed")
        return True


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Run Batch ETL Pipeline')
    parser.add_argument('--test-mode', action='store_true', 
                       help='Run in test mode with limited data')
    parser.add_argument('--validate-only', action='store_true',
                       help='Only validate configuration without running pipeline')
    
    args = parser.parse_args()
    
    pipeline = BatchETLPipeline(test_mode=args.test_mode)
    
    if args.validate_only:
        success = pipeline.validate_pipeline()
    else:
        success = pipeline.run()
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
