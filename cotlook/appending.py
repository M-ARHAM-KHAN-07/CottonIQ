# appending.py
import os
import pandas as pd
import numpy as np
import psycopg2
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def save_to_excel(growth_df, indices_df, config):
    """Save DataFrames to Excel files with new schema structure including basis columns"""
    try:
        output_folder = config['output_folder']
        os.makedirs(output_folder, exist_ok=True)

        growth_file = os.path.join(output_folder, 'cotlook_growth.xlsx')
        indices_file = os.path.join(output_folder, 'cotlook_indices.xlsx')

        # Save cotlook_growth table with new basis columns
        if not growth_df.empty:
            if 'Spot_basis' not in growth_df.columns:
                growth_df['Spot_basis'] = np.nan
            if 'forward_basis' not in growth_df.columns:
                growth_df['forward_basis'] = np.nan

            growth_df.to_excel(growth_file, index=False)
            logger.info(f"cotlook_growth data saved to: {growth_file}")
        else:
            pd.DataFrame(columns=[
                'Date', 'Marketing_Year', 'Growth',
                'Spot_Price', 'Spot_Change', 'Spot_Shpt',
                'forward_Price', 'forward_Change', 'forward_Shpt',
                'Spot_basis', 'forward_basis'
            ]).to_excel(growth_file, index=False)
            logger.warning("No cotlook_growth data found, created empty template file")

        # Save cotlook_indices table
        if not indices_df.empty:
            indices_df.to_excel(indices_file, index=False)
            logger.info(f"cotlook_indices data saved to: {indices_file}")
        else:
            pd.DataFrame(columns=[
                'Date', 'Marketing_Year', 'Index_Name',
                'Value', 'Change', 'Unit'
            ]).to_excel(indices_file, index=False)
            logger.warning("No cotlook_indices data found, created empty template file")

        return growth_file, indices_file

    except Exception as e:
        logger.error(f"Error saving to Excel: {e}")
        raise


def insert_to_postgresql(growth_file, indices_file, config):
    """Insert data from Excel files to PostgreSQL database with new schema including basis columns"""
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=config["db_name"],
            user=config["db_user"],
            password=config["db_password"],
            host=config["db_host"],
            port=config.get("db_port", 5432)
        )
        conn.autocommit = True
        schema_name = config.get("db_schema", "public")

        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
            logger.info(f"Ensured schema '{schema_name}' exists")

        # Insert cotlook_growth data
        if os.path.exists(growth_file):
            df_growth = pd.read_excel(growth_file)
            if not df_growth.empty:
                if 'Spot_basis' not in df_growth.columns:
                    df_growth['Spot_basis'] = np.nan
                if 'forward_basis' not in df_growth.columns:
                    df_growth['forward_basis'] = np.nan

                with conn.cursor() as cur:
                    insert_query = f"""
                        INSERT INTO {schema_name}.cotlook_growth
                        (Date, Marketing_Year, Growth,
                         Spot_price, Spot_change, Spot_shpt,
                         forward_price, forward_change, forward_shpt,
                         Spot_basis, forward_basis)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """
                    for _, row in df_growth.iterrows():
                        cur.execute(insert_query, tuple(row.values))
                logger.info(f"Inserted {len(df_growth)} records into {schema_name}.cotlook_growth")
            else:
                logger.warning("cotlook_growth file is empty. No records inserted.")

        # Insert cotlook_indices data
        if os.path.exists(indices_file):
            df_indices = pd.read_excel(indices_file)
            if not df_indices.empty:
                with conn.cursor() as cur:
                    insert_query = f"""
                        INSERT INTO {schema_name}.cotlook_indices
                        ("Date", "Marketing_Year", "Index_Name",
                         "Value", "Change", "Unit")
                        VALUES (%s,%s,%s,%s,%s,%s)
                    """
                    for _, row in df_indices.iterrows():
                        cur.execute(insert_query, tuple(row.values))
                logger.info(f"Inserted {len(df_indices)} records into {schema_name}.cotlook_indices")
            else:
                logger.warning("cotlook_indices file is empty. No records inserted.")

        logger.info("All data successfully inserted into PostgreSQL")

    except Exception as e:
        logger.error(f"Error inserting to PostgreSQL: {e}")
        raise

    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed")
