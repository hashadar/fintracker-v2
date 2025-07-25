# wise/run_pipeline.py
import subprocess
import sys
import os


def run_script(script_path):
    """Executes a Python script using the same interpreter and checks for errors."""
    print(f"--- Running {os.path.basename(script_path)} ---")
    try:
        # Ensure the script is run with the same python executable
        # This is important for virtual environments
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
    """Runs the full Wise data processing pipeline."""
    print("Starting Wise Data Pipeline...")

    # Get the directory of the current script to reliably locate other scripts
    pipeline_dir = os.path.dirname(os.path.abspath(__file__))

    cleansing_script = os.path.join(
        pipeline_dir, "cleansed", "create_wise_cleansed_tables.py"
    )
    staging_script = os.path.join(
        pipeline_dir, "staging", "create_wise_staging_tables.py"
    )

    run_script(cleansing_script)
    run_script(staging_script)

    print("Wise Data Pipeline completed successfully!")


if __name__ == "__main__":
    main()
