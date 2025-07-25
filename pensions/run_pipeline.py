# pensions/run_pipeline.py
import subprocess
import sys
import os


def run_script(script_path):
    """Executes a Python script and checks for errors."""
    print(f"--- Running {os.path.basename(script_path)} ---")
    try:
        result = subprocess.run(
            [sys.executable, script_path], check=True, text=True, capture_output=True
        )
        print(result.stdout)
        if result.stderr:
            print("--- Stderr ---")
            print(result.stderr)
        print(f"--- Finished {os.path.basename(script_path)} ---")
    except subprocess.CalledProcessError as e:
        print(f"!!! ERROR while running {script_path} !!!")
        print(e.stdout)
        print(e.stderr)
        sys.exit(1)


def main():
    """Runs the full Pensions data processing pipeline."""
    print("Starting Pensions Data Pipeline...")

    pipeline_dir = os.path.dirname(os.path.abspath(__file__))

    raw_script = os.path.join(pipeline_dir, "raw", "create_pensions_raw_tables.py")
    cleansing_script = os.path.join(
        pipeline_dir, "cleansed", "create_pensions_cleansed_tables.py"
    )
    staging_script = os.path.join(
        pipeline_dir, "staging", "create_pensions_staging_tables.py"
    )

    # Run the scripts in the correct order
    run_script(raw_script)
    run_script(cleansing_script)
    run_script(staging_script)

    print("Pensions Data Pipeline completed successfully!")


if __name__ == "__main__":
    main()
