# Sports Analytics Data Warehouse ETL Pipeline

A comprehensive ETL (Extract, Transform, Load) pipeline for sports analytics data using Apache Airflow and PostgreSQL. This project processes GPS tracking data and Wyscout football analytics data into a dimensional data warehouse model.

## 📊 Project Overview

This ETL pipeline processes multiple sports data sources:
- **GPS Match Data** - Player tracking data from matches
- **GPS Training Data** - Player tracking data from training sessions  
- **Wyscout Player Data** - Professional football player statistics (outfield & goalkeeper)
- **Wyscout Match Data** - Match information and statistics

The pipeline transforms this raw data into a star schema data warehouse with dimension and fact tables optimized for analytics.

## 🏗️ Data Warehouse Schema

### Dimension Tables
- `dim_date` - Date dimension with year, month, day, week, quarter
- `dim_team` - Team information with standardized names
- `dim_player` - Player information with standardized names
- `dim_competition` - Competition/league information
- `dim_match` - Match details and metadata

### Fact Tables
- `fact_player_gps` - GPS tracking metrics for players
- `fact_player_wyscout` - Wyscout player performance statistics
- `fact_wyscout_match` - Match-level statistics and results

## 🛠️ Technology Stack

- **Python 3.7+** - Core programming language
- **Apache Airflow** - Workflow orchestration
- **PostgreSQL** - Data warehouse database
- **pandas** - Data manipulation and analysis
- **SQLAlchemy** - Database ORM and connection management
- **RapidFuzz** - Fuzzy string matching for data cleaning
- **python-dotenv** - Environment variable management

## 📋 Prerequisites

- Python 3.7 or higher
- PostgreSQL database server
- Git (for version control)

## 🚀 Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/Mazelt-Ne9es/Data-Warehouse.git
   cd Data-Warehouse
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up PostgreSQL database**
   - Install PostgreSQL if not already installed
   - Create a database named `sports_analytics` (or your preferred name)
   - Create a user with appropriate permissions

4. **Configure environment variables**
   ```bash
   cp .env.example .env
   ```
   
   Edit `.env` file with your PostgreSQL credentials:
   ```env
   DB_HOST=localhost
   DB_PORT=5432
   DB_NAME=sports_analytics
   DB_USER=your_username
   DB_PASSWORD=your_password
   ```

## 📁 Required Data Files

Place the following CSV files in the project directory:
- `matchs_gps.csv` - GPS data from matches
- `training_gps.csv` - GPS data from training
- `wyscout_players_outfield.csv` - Outfield player statistics
- `wyscout_players_goalkeeper.csv` - Goalkeeper statistics
- `wyscout_matchs.csv` - Match information

## 🎯 Usage

### Standalone Execution (Recommended for Development)

Run the ETL pipeline directly:
```bash
python TL.py
```

This will:
1. Extract data from CSV files
2. Transform and clean the data
3. Load into PostgreSQL database
4. Create all dimension and fact tables

### Apache Airflow Execution

1. **Initialize Airflow** (if not already done)
   ```bash
   airflow db init
   airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com
   ```

2. **Copy the DAG file**
   ```bash
   cp TL.py ~/airflow/dags/
   ```

3. **Start Airflow services**
   ```bash
   # Terminal 1 - Start scheduler
   airflow scheduler
   
   # Terminal 2 - Start web server
   airflow webserver --port 8080
   ```

4. **Access Airflow UI**
   - Open http://localhost:8080
   - Enable the `sports_analytics_etl_postgresql` DAG
   - Trigger the DAG manually or wait for scheduled execution

## 🔧 Key Features

### Data Quality & Cleaning
- **Fuzzy String Matching** - Handles variations in team and player names
- **Date Standardization** - Converts all dates to consistent format
- **Null Value Handling** - Graceful handling of missing data
- **Duplicate Detection** - Removes duplicate records

### Data Transformation
- **Dimensional Modeling** - Star schema optimized for analytics
- **ID Mapping** - Foreign key relationships between tables
- **Data Type Conversion** - Proper data types for efficient storage
- **Session Categorization** - Distinguishes between match and training data

### Scalability & Maintenance
- **Modular Design** - Separate functions for extract, transform, load
- **Environment Configuration** - Database settings via environment variables
- **Error Handling** - Robust error handling and logging
- **Airflow Integration** - Production-ready workflow orchestration

## 📊 Data Processing Flow

```
CSV Files → Extract → Transform → Load → PostgreSQL
    ↓           ↓          ↓        ↓         ↓
Raw Data → DataFrames → Clean Data → SQL → Data Warehouse
```

1. **Extract**: Read CSV files into pandas DataFrames
2. **Transform**: 
   - Clean and standardize data
   - Create dimension tables
   - Apply fuzzy matching for name resolution
   - Generate fact tables with proper foreign keys
3. **Load**: Insert processed data into PostgreSQL tables

## 🗂️ Project Structure

```
DataWarehouse/
├── TL.py                    # Main ETL pipeline script
├── requirements.txt         # Python dependencies
├── .env.example            # Environment variables template
├── .gitignore              # Git ignore patterns
├── README.md               # This file
├── matchs_gps.csv          # GPS match data (not tracked)
├── training_gps.csv        # GPS training data (not tracked)
├── wyscout_players_outfield.csv  # Outfield player data (not tracked)
├── wyscout_players_goalkeeper.csv # Goalkeeper data (not tracked)
└── wyscout_matchs.csv      # Match data (not tracked)
```

## 🔒 Security

- Environment variables for database credentials
- `.env` file excluded from version control
- No hardcoded passwords or sensitive information

## 📈 Performance Considerations

- **Batch Processing** - Efficient bulk data operations
- **Index Creation** - Database indexes on key columns (recommended)
- **Memory Management** - Efficient pandas operations
- **Connection Pooling** - SQLAlchemy connection management

## 📧 Support

For questions or issues, please open an issue on GitHub or contact the maintainer.

---

**Note**: Make sure all CSV data files are present in the project directory before running the ETL pipeline.
