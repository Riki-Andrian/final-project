import pandas as pd  # ← WAJIB
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

def load_to_postgres(file_path, table_name, conn_str, date_column, execution_date, next_execution_date, full_load=False):
    try:
        df = pd.read_csv(file_path, parse_dates=[date_column], dayfirst=True)
        df[date_column] = pd.to_datetime(df[date_column], utc=True)

        if isinstance(execution_date, str):
            execution_date = pd.to_datetime(execution_date)
        if isinstance(next_execution_date, str):
            next_execution_date = pd.to_datetime(next_execution_date)

        if full_load:
            filtered_df = df
        else:
            mask = (df[date_column] >= execution_date) & (df[date_column] < next_execution_date)
            filtered_df = df.loc[mask]

        if filtered_df.empty:
            print(f"No data to load for '{table_name}'")
            return

        engine = create_engine(conn_str)
        filtered_df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)

    except FileNotFoundError:
        print(f"File not found: {file_path}")
    except SQLAlchemyError as db_err:
        print(f"DB error: {db_err}")
    except Exception as e:
        print(f"Unexpected error: {e}")
