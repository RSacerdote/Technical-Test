from thefuzz import fuzz
from thefuzz import process
import pandas as pd
from snowflake.connector import connect, ProgrammingError
from snowflake.connector.pandas_tools import write_pandas
import names
from dotenv import load_dotenv
import os
import seaborn

def load_data(csv_path = './data') -> tuple[pd.DataFrame, pd.DataFrame, dict[str, pd.DataFrame]]:
    """
    Load transaction, customer and name data.
    
    Args:
        csv_path (str, optional): Path to the stored data. Defaults to './data'

    Returns:
        tuple: A tuple containing the transactions dataframe, the customers dataframe and the reference names dictionary.
    """

    # Reading data from csv
    transactions = pd.read_csv(f'{csv_path}/transactions.csv')
    customers = pd.read_csv(f'{csv_path}/customers.csv')

    # Getting most common names from the US Census (public data)
    # It is fetched through the module "names" and stored in the package data
    reference_names = {}
    for key, path in names.FILES.items():
        column_names = ['name', 'pct', 'sum_pct', 'position']

        df = pd.read_csv(
            path,
            sep=r'\s+',
            names=column_names, 
            nrows=1000,
        )
        reference_names[key] = df
    
    return transactions, customers, reference_names

def get_best_score(name: str, choices: list[str]) -> int:
    """
    Find the best score for a match between the name and the choices.

    Args:
        name (str): The name to be matched against the choices.
        choices (list[str]): A list with the possible choices to match the name to.

    Returns:
        int: The biggest score.
    """
    best, score = process.extractOne(name, choices, scorer=fuzz.token_set_ratio)
    return score

def extract_best(name: str, choices: list[str], threshold: int = 75) -> str | None:
    """
    Wrapper for the extractOne function which allows it to be called from apply.
    
    Args:
        name (str): The name to be matched against the choices.
        choices (list[str]): A list with the possible choices to match the name to.
        threshold (int, optional): The threshold to filter the results by (inclusive).

    Returns:
        Optional[str]: The best match, if there's one with score bigger than the threshold.
    """
    result = process.extractOne(name, choices, scorer=fuzz.token_set_ratio, score_cutoff=threshold)
    if result is not None:
        return result[0]
    return None

def select_best_full_name(row: pd.Series) -> str:
    """
    Find the most probable full name using the scores columns.

    Args:
        row (pd.Series): A pandas Series with the columns customer_name_1, customer_name_2, score_1, score_2, score_last_1, score_last_2.

    Returns:
        str: The most probable full name.
    """
    if row['score_1'] > row['score_2']:
        best_first = row['customer_name_1'].split(' ')[0]
    elif row['score_2'] > row['score_1']:
        best_first = row['customer_name_2'].split(' ')[0]
    else:
        best_first = row['customer_name_1'].split(' ')[0]

    if row['score_last_1'] > row['score_last_2']:
        best_last = row['customer_name_1'].split(' ')[1]
    elif row['score_last_2'] > row['score_last_1']:
        best_last = row['customer_name_2'].split(' ')[1]
    else:
        best_last = row['customer_name_1'].split(' ')[1]

    return f"{best_first} {best_last}"

def connect_to_snowflake(verbose: bool =True):
    """
    Uses environment variables to connect to Snowflake.
    
    Args:
        verbose (bool, optional): Set to true to display more information about the connection atempt. Defaults to True.

    Returns:
        Optional[SnowflakeConnection]: The connection to the snowflake database.
    """
    load_dotenv()

    SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
    SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
    SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
    SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
    SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
    SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')

    try:
        conn = connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        if verbose:
            print("Connection to Snowflake established successfully.")
        return conn
    except ProgrammingError as e:
        if verbose:
            print(f"Error connecting to Snowflake: {e}")  
    return None  

def create_table(conn, table_name: str, verbose: str = False) -> bool:
    """
    Creates a table in the connected Snowflake database.

    Args:
        conn: The Snowflake connection object.
        table_name (str): The name of the table to create.
        verbose (bool, optional): Set to true to display more information about the execution. Defaults to True.

    Returns:
        bool: True if the table was created successfully, False otherwise.
    """

    try:
        with conn.cursor() as cursor:
            create_table_query = f"""
            CREATE OR REPLACE TABLE {table_name} (
                transaction_id INTEGER,
                amount INTEGER,
                transaction_date DATE,
                customer_id INTEGER,
                email STRING,
                customer_name STRING
            )
            """
            cursor.execute(create_table_query)
            if verbose:
                print(f"Table '{table_name}' created successfully.")
            return True
    except Exception as e:
        if verbose:
            print(f"Failed to create table '{table_name}': {e}")
        return False
    
def main(csv_path: str = './data', table_name: str = 'CUSTOMER_TRANSACTIONS', verbose: bool = True) -> None:
    """
    Loads the csv data, matches it and loads to a Snowflake database.

    Args:
        csv_path (str, optional): Path to the stored data. Defaults to './data'.
        table_name (str, optional): Name to the new table (uppercase). Defaults to 'CUSTOMER_TRANSACTIONS'.
        verbose (bool, optional): Set to true to display more information about the execution. Defaults to True.
    """

    table_name = table_name.upper()

    # Loading data from csv and module files
    transactions, customers, reference_names = load_data(csv_path)

    # Matching the customer_names from both dataframes using fuzzy matching
    customers_names = customers['customer_name'].tolist()
    transactions['external_name'] = transactions['customer_name'].apply(lambda name: extract_best(name, customers_names))

    # Merging the two dataframes
    merged_df = pd.merge(
        transactions,
        customers,
        left_on='external_name',
        right_on='customer_name',
        how='left',
        suffixes=('_1', '_2')
    )

    # Turning any None (in this case it would be a transaction without a matching customer name) into " " so that it can be parsed by the next step
    merged_df = merged_df.fillna(' ')

    # Removing duplicate column
    merged_df.drop(columns=['external_name'], inplace=True)

    # Scoring the names based on the reference list to get the most probable full name
    merged_df['score_1'] = merged_df['customer_name_1'].apply(lambda name: get_best_score(name.split(' ')[0], reference_names['first:female']['name'].to_list()+reference_names['first:male']['name'].to_list()))
    merged_df['score_2'] = merged_df['customer_name_2'].apply(lambda name: get_best_score(name.split(' ')[0], reference_names['first:female']['name'].to_list()+reference_names['first:male']['name'].to_list()))
    merged_df['score_last_1'] = merged_df['customer_name_1'].apply(lambda name: get_best_score(name.split(' ')[1], reference_names['last']['name'].to_list()))
    merged_df['score_last_2'] = merged_df['customer_name_2'].apply(lambda name: get_best_score(name.split(' ')[1], reference_names['last']['name'].to_list()))
    merged_df['customer_name'] = merged_df.apply(lambda row: select_best_full_name(row), axis=1)
    merged_df.drop(columns=['customer_name_1', 'customer_name_2', 'score_1', 'score_2', 'score_last_1', 'score_last_2'], inplace=True)

    # Preparing the column names to allow it to be uploaded to snowflake
    merged_df.columns = [str.upper(column_name) for column_name in merged_df.columns]

    # Connecting to snowflake using the environment variables
    conn = connect_to_snowflake(verbose)

    # Creating a new table (or replacing an existing one)
    create_table(conn, table_name, verbose)

    # Uploading data to the new table on snowflake
    success, nchunks, nrows, _ = write_pandas(conn, merged_df, table_name)
    if success:
        if verbose:
            print(f"Successfully uploaded {nrows} rows to {table_name} in {nchunks} chunks.")
    else:
        if verbose:
            print("Failed to upload DataFrame to Snowflake.")

    # Closing the connection
    conn.close()
    if verbose:
        print("Connection to Snowflake closed successfully.")

if __name__ == '__main__':
    main()