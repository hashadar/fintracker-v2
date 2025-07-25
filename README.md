# FinTracker V2

A personal finance tracking system that processes and visualizes banking and pension data through automated ETL pipelines and interactive dashboards.

## Features

### Data Processing Pipelines
- **Wise Banking Pipeline**: Processes CSV statements to track account balance and transaction patterns
- **Pensions Pipeline**: Pulls data from Google Sheets to analyze pension performance and investment growth

### Interactive Dashboards
- **Multi-page Streamlit application** with dedicated views for each data source
- **Banking Dashboard**: Balance trends, transaction analysis, and cash flow calendar heatmap
- **Pensions Dashboard**: Performance tracking, gain/loss analysis, and investment growth visualization

### Data Storage & Management
- **AWS S3 integration** with organized folder structure (`raw`, `cleansed`, `staging`)
- **Environment-based configurations** for development and production
- **Automated data processing** with timestamped file versioning

## Quick Start

### Setup
1. Install dependencies: `pip install -r requirements.txt`
2. Configure `configuration/secrets.py` (use `configuration/secrets_template.py` as reference)
3. Set up AWS S3 bucket and Google Sheets API access

### Running Pipelines
```bash
# Process Wise banking data
python wise/run_pipeline.py

# Process pension data
python pensions/run_pipeline.py
```

### View Dashboards
```bash
streamlit run streamlit/Home.py
```

## Project Structure

```
fintracker-v2/
├── aws/                    # S3 connection utilities
├── gcp/                    # Google Sheets API utilities
├── wise/                   # Wise banking data pipeline
│   ├── cleansed/          # Data cleaning scripts
│   ├── staging/           # Dashboard-ready data generation
│   └── run_pipeline.py    # Pipeline orchestration
├── pensions/              # Pension data pipeline
│   ├── raw/               # Google Sheets data extraction
│   ├── cleansed/          # Data filtering and standardization
│   ├── staging/           # Performance analysis tables
│   └── run_pipeline.py    # Pipeline orchestration
├── streamlit/             # Interactive dashboards
│   ├── Home.py           # Landing page
│   └── pages/            # Individual dashboard pages
└── configuration/         # Secrets and environment config
```

## Data Flow

1. **Raw Data**: CSV files (Wise) and Google Sheets (Pensions)
2. **Cleansed**: Standardized, filtered data with consistent schemas
3. **Staging**: Dashboard-ready tables optimized for visualization
4. **Dashboards**: Interactive Streamlit visualizations 