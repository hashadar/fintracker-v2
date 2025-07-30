"""
Coinbase Pipeline Orchestrator - Main ETL Pipeline Controller
Coordinates the execution of all data processing steps following the ETL strategy.
"""

import sys
import os
import argparse
from datetime import datetime
from typing import Dict, List, Optional

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def run_data_extraction() -> Dict:
    """
    Run the data extraction step.
    Returns: Extraction results and status.
    """
    print("\n" + "=" * 60)
    print("STEP 1: DATA EXTRACTION")
    print("=" * 60)

    try:
        from data_extractor import main as run_extraction

        result = run_extraction()
        print(f"✓ Data extraction completed successfully")
        return result
    except Exception as e:
        print(f"✗ Data extraction failed: {e}")
        raise


def run_data_transformation() -> Dict:
    """
    Run the data transformation step.
    Returns: Transformation results and status.
    """
    print("\n" + "=" * 60)
    print("STEP 2: DATA TRANSFORMATION")
    print("=" * 60)

    try:
        from data_transformer import main as run_transformation

        result = run_transformation()
        print(f"✓ Data transformation completed successfully")
        return result
    except Exception as e:
        print(f"✗ Data transformation failed: {e}")
        raise


def run_analytics_calculation() -> Dict:
    """
    Run the analytics calculation step.
    Returns: Analytics results and status.
    """
    print("\n" + "=" * 60)
    print("STEP 3: ANALYTICS CALCULATION")
    print("=" * 60)

    try:
        from analytics_calculator import main as run_analytics

        result = run_analytics()
        print(f"✓ Analytics calculation completed successfully")
        return result
    except Exception as e:
        print(f"✗ Analytics calculation failed: {e}")
        raise


def run_full_pipeline() -> Dict:
    """
    Run the complete ETL pipeline end-to-end.
    Returns: Complete pipeline results and status.
    """
    print("\n" + "=" * 80)
    print("COINBASE ETL PIPELINE - FULL EXECUTION")
    print("=" * 80)
    print(f"Pipeline started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    pipeline_results = {
        "pipeline_start": datetime.now().isoformat(),
        "steps_completed": [],
        "steps_failed": [],
        "overall_status": "running",
    }

    try:
        # Step 1: Data Extraction
        print("\n🔄 Starting Step 1: Data Extraction...")
        extraction_result = run_data_extraction()
        pipeline_results["steps_completed"].append(
            {
                "step": "data_extraction",
                "status": "success",
                "result": extraction_result,
            }
        )
        print(f"✓ Step 1 completed at: {datetime.now().strftime('%H:%M:%S')}")

        # Step 2: Data Transformation
        print("\n🔄 Starting Step 2: Data Transformation...")
        transformation_result = run_data_transformation()
        pipeline_results["steps_completed"].append(
            {
                "step": "data_transformation",
                "status": "success",
                "result": transformation_result,
            }
        )
        print(f"✓ Step 2 completed at: {datetime.now().strftime('%H:%M:%S')}")

        # Step 3: Analytics Calculation
        print("\n🔄 Starting Step 3: Analytics Calculation...")
        analytics_result = run_analytics_calculation()
        pipeline_results["steps_completed"].append(
            {
                "step": "analytics_calculation",
                "status": "success",
                "result": analytics_result,
            }
        )
        print(f"✓ Step 3 completed at: {datetime.now().strftime('%H:%M:%S')}")

        # Pipeline completed successfully
        pipeline_results["pipeline_end"] = datetime.now().isoformat()
        pipeline_results["overall_status"] = "success"

        print("\n" + "=" * 80)
        print("🎉 COINBASE ETL PIPELINE COMPLETED SUCCESSFULLY!")
        print("=" * 80)
        print(f"Pipeline completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # Summary
        print("\n📊 PIPELINE SUMMARY:")
        print(f"  • Data Extraction: {extraction_result['records_processed']}")
        print(f"  • Data Transformation: {transformation_result['records_processed']}")
        print(f"  • Analytics Calculation: {analytics_result['records_processed']}")

        return pipeline_results

    except Exception as e:
        pipeline_results["pipeline_end"] = datetime.now().isoformat()
        pipeline_results["overall_status"] = "failed"
        pipeline_results["error"] = str(e)

        print("\n" + "=" * 80)
        print("❌ COINBASE ETL PIPELINE FAILED!")
        print("=" * 80)
        print(f"Pipeline failed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Error: {e}")

        # Show completed steps
        if pipeline_results["steps_completed"]:
            print(f"\nCompleted steps: {len(pipeline_results['steps_completed'])}")
            for step in pipeline_results["steps_completed"]:
                print(f"  ✓ {step['step']}")

        raise


def validate_prerequisites() -> bool:
    """
    Validate that all prerequisites are met before running the pipeline.
    Returns: True if all prerequisites are met, False otherwise.
    """
    print("🔍 Validating prerequisites...")

    try:
        # Check if configuration exists
        from configuration.secrets import (
            COINBASE_API_KEY,
            COINBASE_API_SECRET,
            ENVIRONMENT,
            AWS_ACCESS_KEY_ID,
            AWS_SECRET_ACCESS_KEY,
            AWS_REGION,
            S3_BUCKET_NAME,
        )

        print("✓ Configuration loaded successfully")

        # Check if required modules can be imported
        import coinbase.rest
        import boto3
        import pandas as pd
        import numpy as np

        print("✓ Required Python packages available")

        # Test S3 connection
        from aws.connect_to_s3 import S3Helper

        s3_helper = S3Helper()
        print("✓ S3 connection successful")

        # Test Coinbase API connection (basic test)
        from coinbase.rest import RESTClient

        client = RESTClient(api_key=COINBASE_API_KEY, api_secret=COINBASE_API_SECRET)
        # Try to get portfolios (this will fail if credentials are invalid)
        portfolios_response = client.get_portfolios()
        print("✓ Coinbase API connection successful")

        print("✅ All prerequisites validated successfully!")
        return True

    except ImportError as e:
        print(f"❌ Configuration error: {e}")
        print(
            "Please ensure configuration/secrets.py exists with all required credentials."
        )
        return False
    except Exception as e:
        print(f"❌ Prerequisite validation failed: {e}")
        return False


def show_pipeline_status() -> None:
    """
    Show the current status of the pipeline and data freshness.
    """
    print("\n" + "=" * 60)
    print("PIPELINE STATUS CHECK")
    print("=" * 60)

    try:
        from configuration.secrets import ENVIRONMENT
        from aws.connect_to_s3 import S3Helper

        s3_helper = S3Helper()

        # Check latest files in each folder
        folders_to_check = [
            "raw/positions/latest",
            "raw/transactions/latest",
            "raw/prices/latest",
            "raw/market/latest",
            "processed/current",
            "processed/historical",
            "analytics",
        ]

        for folder in folders_to_check:
            prefix = f"{ENVIRONMENT}/coinbase/{folder}/"
            files = s3_helper.list_files(prefix=prefix)

            if files:
                latest_file = max(files, key=lambda x: x.split("/")[-1])
                print(f"✓ {folder}: {latest_file.split('/')[-1]}")
            else:
                print(f"✗ {folder}: No files found")

    except Exception as e:
        print(f"❌ Error checking pipeline status: {e}")


def main():
    """
    Main pipeline orchestrator with command-line interface.
    """
    parser = argparse.ArgumentParser(description="Coinbase ETL Pipeline Orchestrator")
    parser.add_argument(
        "--step",
        choices=["extract", "transform", "analytics", "full", "status", "validate"],
        default="full",
        help="Which step(s) to run (default: full pipeline)",
    )
    parser.add_argument(
        "--skip-validation", action="store_true", help="Skip prerequisite validation"
    )

    args = parser.parse_args()

    print("🚀 Coinbase ETL Pipeline Orchestrator")
    print(f"Command: {args.step}")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        # Validate prerequisites unless skipped
        if not args.skip_validation and args.step != "validate":
            if not validate_prerequisites():
                print("❌ Prerequisites validation failed. Exiting.")
                sys.exit(1)

        # Run requested step(s)
        if args.step == "validate":
            validate_prerequisites()

        elif args.step == "status":
            show_pipeline_status()

        elif args.step == "extract":
            run_data_extraction()

        elif args.step == "transform":
            run_data_transformation()

        elif args.step == "analytics":
            run_analytics_calculation()

        elif args.step == "full":
            run_full_pipeline()

        print(f"\n✅ Pipeline orchestrator completed successfully!")

    except KeyboardInterrupt:
        print("\n⚠️ Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Pipeline orchestrator failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
