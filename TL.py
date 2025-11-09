from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine
import pandas as pd
from rapidfuzz import process, fuzz
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# PostgreSQL DB configuration
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# Construct PostgreSQL connection string
POSTGRES_DB_URL = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

def extract_data():
    """Extract data from CSV files and return as dictionary"""
    data = {
        'gps_match': pd.read_csv('matchs_gps.csv'),
        'gps_training': pd.read_csv('training_gps.csv'),
        'wyscout_outfield': pd.read_csv('wyscout_players_outfield.csv'),
        'wyscout_goalkeeper': pd.read_csv('wyscout_players_goalkeeper.csv'),
        'wyscout_matches': pd.read_csv('wyscout_matchs.csv')
    }
    return data

def extract(**kwargs):
    """Airflow extract task wrapper"""
    data = extract_data()
    for key, value in data.items():
        kwargs['ti'].xcom_push(key=key, value=value)

def transform_data(gps_match, gps_training, wyscout_outfield, wyscout_goalkeeper, wyscout_matches):
    """Transform data and return processed tables"""
    
    gps_match['session_type'] = 'match'
    gps_training['session_type'] = 'training'
    fact_player_gps = pd.concat([gps_match, gps_training], ignore_index=True)

    for df in [fact_player_gps, wyscout_matches, wyscout_outfield, wyscout_goalkeeper]:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')

    date_min = min(fact_player_gps['date'].min(), wyscout_matches['date'].min())
    date_max = max(fact_player_gps['date'].max(), wyscout_matches['date'].max())
    date_range = pd.date_range(start=date_min, end=date_max)
    dim_date = pd.DataFrame({
        'date': date_range, 
        'date_key': date_range.strftime('%Y%m%d').astype(int),
        'year': date_range.year,
        'month': date_range.month,
        'day': date_range.day,
        'week': date_range.isocalendar().week,
        'quarter': date_range.quarter
    })

    teams_gps = fact_player_gps['team_name'].dropna().unique()
    teams_wyscout = pd.concat([wyscout_outfield['team_name'], wyscout_goalkeeper['team_name']]).dropna().unique()
    all_teams = list(set(list(teams_gps) + list(teams_wyscout)))
    dim_team = pd.DataFrame({'team_name_std': all_teams})
    dim_team['team_id'] = range(1, len(dim_team) + 1)

    players_gps = fact_player_gps['name'].dropna().unique()
    players_wyscout = pd.concat([wyscout_outfield['player'], wyscout_goalkeeper['player']]).dropna().unique()
    all_players = list(set(list(players_gps) + list(players_wyscout)))
    dim_player = pd.DataFrame({'player_name_std': all_players})
    dim_player['player_id'] = range(1, len(dim_player) + 1)

    dim_competition = pd.DataFrame(wyscout_matches['competition'].dropna().unique(), columns=['competition'])
    dim_competition['competition_id'] = range(1, len(dim_competition) + 1)

    def fuzzy_match(name, choices, threshold=85):
        match, score, _ = process.extractOne(name, choices, scorer=fuzz.token_set_ratio)
        return match if score >= threshold else None

    def map_name_to_id(name, dim_df, name_col, id_col):
        if pd.isna(name):
            return None
        matched = fuzzy_match(name, dim_df[name_col].tolist())
        if matched:
            return dim_df.loc[dim_df[name_col] == matched, id_col].values[0]
        return None

    fact_player_gps['team_id'] = fact_player_gps['team_name'].apply(lambda x: map_name_to_id(x, dim_team, 'team_name_std', 'team_id'))
    fact_player_gps['player_id'] = fact_player_gps['name'].apply(lambda x: map_name_to_id(x, dim_player, 'player_name_std', 'player_id'))
    wyscout_outfield['team_id'] = wyscout_outfield['team_name'].apply(lambda x: map_name_to_id(x, dim_team, 'team_name_std', 'team_id'))
    wyscout_goalkeeper['team_id'] = wyscout_goalkeeper['team_name'].apply(lambda x: map_name_to_id(x, dim_team, 'team_name_std', 'team_id'))
    wyscout_outfield['player_id'] = wyscout_outfield['player'].apply(lambda x: map_name_to_id(x, dim_player, 'player_name_std', 'player_id'))
    wyscout_goalkeeper['player_id'] = wyscout_goalkeeper['player'].apply(lambda x: map_name_to_id(x, dim_player, 'player_name_std', 'player_id'))
    wyscout_matches['team_id'] = wyscout_matches['team_name'].apply(lambda x: map_name_to_id(x, dim_team, 'team_name_std', 'team_id'))
    wyscout_matches['competition_id'] = wyscout_matches['competition'].apply(lambda x: map_name_to_id(x, dim_competition, 'competition', 'competition_id'))

    dim_match = wyscout_matches[['match', 'date', 'team_id', 'competition_id']].drop_duplicates()
    dim_match = dim_match.rename(columns={'match': 'match_id'})
    dim_match['date_key'] = dim_match['date'].dt.strftime('%Y%m%d').astype(int)

    wyscout_goalkeeper['player_type'] = 'goalkeeper'
    wyscout_outfield['player_type'] = 'outfield'
    fact_player_wyscout = pd.concat([wyscout_goalkeeper, wyscout_outfield], ignore_index=True)

    return {
        'dim_date': dim_date,
        'dim_team': dim_team,
        'dim_player': dim_player,
        'dim_competition': dim_competition,
        'dim_match': dim_match,
        'fact_player_gps': fact_player_gps,
        'fact_player_wyscout': fact_player_wyscout,
        'fact_wyscout_match': wyscout_matches
    }

def transform(**kwargs):
    """Airflow transform task wrapper"""
    ti = kwargs['ti']
    gps_match = ti.xcom_pull(key='gps_match')
    gps_training = ti.xcom_pull(key='gps_training')
    wyscout_outfield = ti.xcom_pull(key='wyscout_outfield')
    wyscout_goalkeeper = ti.xcom_pull(key='wyscout_goalkeeper')
    wyscout_matches = ti.xcom_pull(key='wyscout_matches')

    transformed_data = transform_data(gps_match, gps_training, wyscout_outfield, wyscout_goalkeeper, wyscout_matches)
    
    for key, value in transformed_data.items():
        ti.xcom_push(key=key, value=value)

def load_data_to_db(transformed_data):
    """Load transformed data to PostgreSQL database"""
    engine = create_engine(POSTGRES_DB_URL)
    
    # Load dimension tables first
    transformed_data['dim_date'].to_sql('dim_date', engine, if_exists='replace', index=False)
    transformed_data['dim_team'].to_sql('dim_team', engine, if_exists='replace', index=False)
    transformed_data['dim_player'].to_sql('dim_player', engine, if_exists='replace', index=False)
    transformed_data['dim_competition'].to_sql('dim_competition', engine, if_exists='replace', index=False)
    transformed_data['dim_match'].to_sql('dim_match', engine, if_exists='replace', index=False)
    
    # Load fact tables
    transformed_data['fact_player_gps'].to_sql('fact_player_gps', engine, if_exists='replace', index=False)
    transformed_data['fact_player_wyscout'].to_sql('fact_player_wyscout', engine, if_exists='replace', index=False)
    transformed_data['fact_wyscout_match'].to_sql('fact_wyscout_match', engine, if_exists='replace', index=False)
    
    print("Data successfully loaded to PostgreSQL database")

def load(**kwargs):
    """Airflow load task wrapper"""
    ti = kwargs['ti']
    
    # Pull all transformed data from XCom
    transformed_data = {
        'dim_date': ti.xcom_pull(key='dim_date'),
        'dim_team': ti.xcom_pull(key='dim_team'),
        'dim_player': ti.xcom_pull(key='dim_player'),
        'dim_competition': ti.xcom_pull(key='dim_competition'),
        'dim_match': ti.xcom_pull(key='dim_match'),
        'fact_player_gps': ti.xcom_pull(key='fact_player_gps'),
        'fact_player_wyscout': ti.xcom_pull(key='fact_player_wyscout'),
        'fact_wyscout_match': ti.xcom_pull(key='fact_wyscout_match')
    }
    
    load_data_to_db(transformed_data)

default_args = {
    'owner': 'user',
    'start_date': datetime(2025, 11, 9),
    'retries': 1,
}

with DAG('sports_analytics_etl_postgresql', default_args=default_args, schedule='@daily', catchup=False) as dag:
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load
    )

    extract_task >> transform_task >> load_task

def run_etl_standalone():
    """Run ETL process without Airflow for testing and development"""
    print("Starting ETL process...")
    
    # 1. Extract
    print("1. Extracting data...")
    raw_data = extract_data()
    
    # 2. Transform
    print("2. Transforming data...")
    transformed_data = transform_data(**raw_data)
    
    # 3. Load
    print("3. Loading data...")
    load_data_to_db(transformed_data)
    
    print("ETL process completed! Data loaded to PostgreSQL database")

# For standalone execution (without Airflow scheduler)
if __name__ == "__main__":
    run_etl_standalone()
