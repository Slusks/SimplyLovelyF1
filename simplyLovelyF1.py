import os
import fastf1
import pandas as pd
import time
import shutil
import numpy as np


def get_driver_mapping(session):
    """
    Create a mapping between driver numbers and their full names.
    
    Parameters:
    -----------
    session : fastf1.Session
        The F1 session object
    
    Returns:
    --------
    dict
        Dictionary mapping driver numbers to full names
    """
    driver_mapping = {}
    try:
        # Get driver info from the session
        for _, driver in session.drivers.iterrows():
            driver_number = driver['DriverNumber']
            driver_name = driver['FullName']
            driver_mapping[driver_number] = driver_name
    except Exception as e:
        print(f"⚠️ Could not get driver mapping: {e}")
    
    return driver_mapping

def get_driver_team_mapping(session):
    """
    Create a mapping between driver numbers and their full names with team names.
    
    Parameters:
    -----------
    session : fastf1.Session
        The F1 session object
    
    Returns:
    --------
    dict
        Dictionary mapping driver numbers to (driver_name, team_name) tuples
    """
    driver_team_map = {}
    try:
        # Get driver info from the session
        for _, driver in session.drivers.iterrows():
            driver_number = driver['DriverNumber']
            driver_name = driver['FullName']
            team_name = driver['TeamName']
            if pd.notna(driver_name) and pd.notna(team_name):
                driver_team_map[driver_number] = (driver_name, team_name)
                print(f"Mapped driver {driver_number}: {driver_name} ({team_name})")
    except Exception as e:
        print(f"⚠️ Could not get driver mapping: {e}")
    
    return driver_team_map

def F1_API_Data_Collection(years=None, sessions_to_collect=None, cache_path=None):
    """
    Collect F1 session data for specified years and sessions.
    
    Parameters:
    -----------
    years : list, optional
        List of years to collect data for. Defaults to [2024].
    sessions_to_collect : list, optional
        List of sessions to collect (e.g., ['FP1', 'FP2', 'FP3', 'Race']).
        Defaults to ['Race'].
    cache_path : str, optional
        Path to store cached data. Defaults to './dataFileDesktop'.
    
    Returns:
    --------
    tuple
        (successful_races, failed_races, skipped_races, output_file)
    """
    # Set default parameters
    if years is None:
        years = [2024]
    if sessions_to_collect is None:
        sessions_to_collect = ['Race']
    if cache_path is None:
        cache_path = r'./dataFileDesktop'

    # Dictionary of F1 races organized by year
    f1_races_by_year = {
        2020: [
            'Austrian Grand Prix',
            'Styrian Grand Prix',
            'Hungarian Grand Prix',
            'British Grand Prix',
            '70th Anniversary Grand Prix',
            'Spanish Grand Prix',
            'Belgian Grand Prix',
            'Italian Grand Prix',
            'Tuscan Grand Prix',
            'Russian Grand Prix',
            'Eifel Grand Prix',
            'Portuguese Grand Prix',
            'Emilia Romagna Grand Prix',
            'Turkish Grand Prix',
            'Bahrain Grand Prix',
            'Sakhir Grand Prix',
            'Abu Dhabi Grand Prix'
        ],
        2021: [
            'Bahrain Grand Prix',
            'Emilia Romagna Grand Prix',
            'Portuguese Grand Prix',
            'Spanish Grand Prix',
            'Monaco Grand Prix',
            'Azerbaijan Grand Prix',
            'French Grand Prix',
            'Styrian Grand Prix',
            'Austrian Grand Prix',
            'British Grand Prix',
            'Hungarian Grand Prix',
            'Belgian Grand Prix',
            'Dutch Grand Prix',
            'Italian Grand Prix',
            'Russian Grand Prix',
            'Turkish Grand Prix',
            'United States Grand Prix',
            'Mexico City Grand Prix',
            'São Paulo Grand Prix',
            'Qatar Grand Prix',
            'Saudi Arabian Grand Prix',
            'Abu Dhabi Grand Prix'
        ],
        2022: [
            'Bahrain Grand Prix',
            'Saudi Arabian Grand Prix',
            'Australian Grand Prix',
            'Emilia Romagna Grand Prix',
            'Miami Grand Prix',
            'Spanish Grand Prix',
            'Monaco Grand Prix',
            'Azerbaijan Grand Prix',
            'Canadian Grand Prix',
            'British Grand Prix',
            'Austrian Grand Prix',
            'French Grand Prix',
            'Hungarian Grand Prix',
            'Belgian Grand Prix',
            'Dutch Grand Prix',
            'Italian Grand Prix',
            'Singapore Grand Prix',
            'Japanese Grand Prix',
            'United States Grand Prix',
            'Mexico City Grand Prix',
            'São Paulo Grand Prix',
            'Abu Dhabi Grand Prix'
        ],
        2023: [
            'Bahrain Grand Prix',
            'Saudi Arabian Grand Prix',
            'Australian Grand Prix',
            'Azerbaijan Grand Prix',
            'Miami Grand Prix',
            'Emilia Romagna Grand Prix',
            'Monaco Grand Prix',
            'Spanish Grand Prix',
            'Canadian Grand Prix',
            'Austrian Grand Prix',
            'British Grand Prix',
            'Hungarian Grand Prix',
            'Belgian Grand Prix',
            'Dutch Grand Prix',
            'Italian Grand Prix',
            'Singapore Grand Prix',
            'Japanese Grand Prix',
            'Qatar Grand Prix',
            'United States Grand Prix',
            'Mexico City Grand Prix',
            'São Paulo Grand Prix',
            'Las Vegas Grand Prix',
            'Abu Dhabi Grand Prix'
        ],
        2024: [
            'Bahrain Grand Prix',
            'Saudi Arabian Grand Prix',
            'Australian Grand Prix',
            'Japanese Grand Prix',
            'Chinese Grand Prix',
            'Miami Grand Prix',
            'Emilia Romagna Grand Prix',
            'Monaco Grand Prix',
            'Canadian Grand Prix',
            'Spanish Grand Prix',
            'Austrian Grand Prix',
            'British Grand Prix',
            'Hungarian Grand Prix',
            'Belgian Grand Prix',
            'Dutch Grand Prix',
            'Italian Grand Prix',
            'Azerbaijan Grand Prix',
            'Singapore Grand Prix',
            'United States Grand Prix',
            'Mexico City Grand Prix',
            'São Paulo Grand Prix',
            'Las Vegas Grand Prix',
            'Qatar Grand Prix',
            'Abu Dhabi Grand Prix'
        ],
        2025: [
        'Australian Grand Prix',
        'Chinese Grand Prix',
        'Japanese Grand Prix',
        'Bahrain Grand Prix',
        'Saudi Arabian Grand Prix',
        #'Miami Grand Prix',
        #'Emilia Romagna Grand Prix',
        #'Monaco Grand Prix',
        #'Spanish Grand Prix',
        #'Canadian Grand Prix',
        #'Austrian Grand Prix',
        #'British Grand Prix',
        #'Belgian Grand Prix',
        #'Hungarian Grand Prix',
        #'Dutch Grand Prix',
        #'Italian Grand Prix',
        #'Azerbaijan Grand Prix',
        #'Singapore Grand Prix',
        #'United States Grand Prix',
        #'Mexico City Grand Prix',
        #'São Paulo Grand Prix',
        #'Las Vegas Grand Prix',
        #'Qatar Grand Prix',
        #'Abu Dhabi Grand Prix'
    ]
    }

    # Set up cache path
    if not os.path.exists(cache_path):
        os.makedirs(cache_path)

    fastf1.Cache.enable_cache(cache_path)

    all_laps = []
    successful_races = set()
    failed_races = set()
    consecutive_failures = 0
    skipped_races = []

    for year in years:
        try:
            schedule = fastf1.get_event_schedule(year, include_testing=False)
            for _, event in schedule.iterrows():
                location = event['EventName']
                # Check if the race exists in the specific year's list
                if year not in f1_races_by_year or location not in f1_races_by_year[year]:
                    print(f"⚠️ Skipping {location} as it's not in the {year} race list")
                    continue
                
                race_success = False
                for session_type in sessions_to_collect:
                    try:
                        session = fastf1.get_session(year, location, session_type)
                        session.load()
                        laps = session.laps
                        if laps.empty:
                            print(f"No laps for {year} {location} {session_type}")
                            continue

                        # Extract only necessary columns
                        subset = laps[['Driver', 'Team', 'LapNumber', 'LapTime']].copy()
                        subset['Year'] = year
                        subset['Track'] = location
                        subset['Session'] = session_type

                        # For race sessions, add the final position
                        if session_type == 'Race':
                            try:
                                results = session.results
                                if not results.empty:
                                    print(f"\nProcessing race results for {year} {location}")
                                    
                                    # Debug: Print raw results data
                                    print("\nRaw results data:")
                                    print(results[['Abbreviation', 'Position', 'TeamName']])
                                    
                                    # Create a simple mapping of driver abbreviation to position
                                    position_map = dict(zip(results['Abbreviation'], results['Position']))
                                    print("\nPosition mapping:")
                                    for abbrev, pos in position_map.items():
                                        print(f"{abbrev} -> Position {pos}")
                                    
                                    # Add position to the subset
                                    subset['Position'] = subset['Driver'].map(position_map)
                                    
                                    # Debug: Print sample of mapped data
                                    print("\nSample of mapped data:")
                                    print(subset[['Driver', 'Team', 'Position']].head())
                                    
                            except Exception as e:
                                print(f"⚠️ Could not get race results for {year} {location}: {e}")
                                print(f"Error details: {str(e)}")
                                import traceback
                                print(traceback.format_exc())
                                subset['Position'] = pd.NA

                        # Convert lap times to seconds
                        subset['LapTime'] = convert_lap_times_column(subset, 'LapTime')

                        all_laps.append(subset)
                        print(f"✅ Added: {year} {location} {session_type} - {len(subset)} laps")
                        race_success = True
                        consecutive_failures = 0  # Reset consecutive failures on success

                    except Exception as e:
                        print(f"⚠️ Skipped {year} {location} {session_type}: {e}")
                        skipped_races.append(f"{year} - {location} - {session_type}: {str(e)}")
                    print("Sleeping for 3 seconds between API calls...")
                    time.sleep(3)
                
                if race_success:
                    successful_races.add(f"{year} - {location}")
                    consecutive_failures = 0  # Reset consecutive failures on success
                else:
                    failed_races.add(f"{year} - {location}")
                    consecutive_failures += 1
                    
                # Check for 3 consecutive failures
                if consecutive_failures >= 3:
                    print("\n❌ Stopping program due to 3 consecutive failures")
                    print("\n=== Skipped Races Summary ===")
                    for race in skipped_races[-3:]:  # Show last 3 skipped races
                        print(race)
                    break  # Exit the race loop
            
            if consecutive_failures >= 3:
                break  # Exit the year loop
                    
        except Exception as e:
            print(f"⚠️ Skipped year {year}: {e}")
            print("Sleeping for 3 seconds before next year...")
            time.sleep(3)

    # Concatenate all collected lap data
    if all_laps:
        df = pd.concat(all_laps, ignore_index=True)
        
        # Create separate DataFrames for each session type
        session_dfs = {}
        for session_type in ['FP1', 'FP2', 'FP3', 'Race']:
            session_df = df[df['Session'] == session_type].copy()
            if not session_df.empty:
                # Rename LapTime column to include session type
                session_df = session_df.rename(columns={'LapTime': f'{session_type} Lap Time'})
                
                # Define columns to keep based on session type
                if session_type == 'Race':
                    columns_to_keep = ['Year', 'Track', 'Team', 'Driver', 'LapNumber', 
                                     'Race Lap Time', 'Position', 'Session']
                else:
                    columns_to_keep = ['Year', 'Track', 'Team', 'Driver', 'LapNumber', 
                                     f'{session_type} Lap Time', 'Session']
                
                # Ensure all required columns exist
                for col in columns_to_keep:
                    if col not in session_df.columns:
                        session_df[col] = pd.NA
                
                # Keep only the required columns
                session_df = session_df[columns_to_keep]
                session_dfs[session_type] = session_df
        
        if not session_dfs:
            print("\n❌ No valid session data found.")
            return successful_races, failed_races, skipped_races, None
        
        # Determine output filename based on session type
        if 'Race' in sessions_to_collect:
            output_file = 'f1_race_laps.csv'
            # Save race data
            if 'Race' in session_dfs:
                session_dfs['Race'].to_csv(output_file, index=False)
                print(f"\n✅ Race data saved to {output_file}")
                print("\nSample of saved race data:")
                print(session_dfs['Race'].head())
        else:
            output_file = 'f1_practice_laps.csv'
            # Save practice data
            practice_dfs = [df for session_type, df in session_dfs.items() if session_type != 'Race']
            if practice_dfs:
                pd.concat(practice_dfs, ignore_index=True).to_csv(output_file, index=False)
                print(f"\n✅ Practice data saved to {output_file}")
    else:
        print("\n❌ No lap data was collected.")
        output_file = None

    # Print summary of successful and failed races
    print("\n=== Data Collection Summary ===")
    print(f"Successful races: {len(successful_races)}")
    print(f"Failed races: {len(failed_races)}")

    return successful_races, failed_races, skipped_races, output_file

def combine_f1_lap_data(dataframes):
    """
    Combine multiple F1 session DataFrames into a single DataFrame with separate columns for each session type.
    
    Parameters:
    -----------
    dataframes : list of pandas.DataFrame
        List of DataFrames containing F1 lap data from different sessions
    
    Returns:
    --------
    pandas.DataFrame
        Combined DataFrame with separate columns for each session type
    """
    if not dataframes:
        print("❌ No dataframes provided")
        return None
    
    # Create a dictionary to store DataFrames by session type
    session_dfs = {}
    
    # Process each input DataFrame
    for df in dataframes:
        # Ensure all required columns exist
        required_columns = ['Year', 'Track', 'Team', 'Driver', 'LapNumber', 'LapTime', 'Session']
        for col in required_columns:
            if col not in df.columns:
                df[col] = pd.NA
        
        # Get the session type from the first row (assuming all rows have the same session type)
        session_type = df['Session'].iloc[0] if not df.empty else None
        
        if session_type:
            # Rename LapTime column to include session type
            df = df.rename(columns={'LapTime': f'{session_type} Lap Time'})
            
            # Store the DataFrame in the dictionary
            session_dfs[session_type] = df
    
    if not session_dfs:
        print("❌ No valid session data found")
        return None
    
    # Get all unique combinations of Year, Track, Team, Driver, and LapNumber
    all_combinations = pd.concat([
        df[['Year', 'Track', 'Team', 'Driver', 'LapNumber']]
        for df in session_dfs.values()
    ]).drop_duplicates()
    
    # Start with the base combinations
    merged_df = all_combinations.copy()
    
    # Merge with each session DataFrame
    for session_type, df in session_dfs.items():
        # Select only the columns we need for this merge
        merge_columns = ['Year', 'Track', 'Team', 'Driver', 'LapNumber', f'{session_type} Lap Time']
        if session_type == 'Race':
            merge_columns.append('Position')
        
        # Ensure all columns exist
        for col in merge_columns:
            if col not in df.columns:
                df[col] = pd.NA
        
        # Merge with the main DataFrame
        merged_df = merged_df.merge(
            df[merge_columns],
            on=['Year', 'Track', 'Team', 'Driver', 'LapNumber'],
            how='left'
        )
    
    # Define the final column order
    final_columns = [
        'Year', 'Track', 'Team', 'Driver', 'LapNumber',
        'FP1 Lap Time', 'FP2 Lap Time', 'FP3 Lap Time', 'Race Lap Time',
        'Position'
    ]
    
    # Ensure all columns exist
    for col in final_columns:
        if col not in merged_df.columns:
            merged_df[col] = pd.NA
    
    # Reorder columns
    merged_df = merged_df[final_columns]
    
    return merged_df

def standardize_team_names(df):
    """
    Standardize team names in the DataFrame according to current team names.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame containing F1 data with a 'Team' column
    
    Returns:
    --------
    pandas.DataFrame
        DataFrame with standardized team names
    """
    team_replacements = {
        'Racing Point': 'Aston Martin',
        'Alfa Romeo': 'Kick Sauber',
        'Alfa Romeo Racing': 'Kick Sauber',
        'Alpha Tauri': 'RB',
        'Renault': 'Alpine'
    }
    
    df['Team'] = df['Team'].replace(team_replacements)
    return df

def combine_practice_and_race_times():
    """
    Combine practice and race lap times from existing CSV files into a single table.
    
    Returns:
    --------
    str
        Path to the combined output file, or None if the operation failed
    """
    # Create ConsumedData directory if it doesn't exist
    consumed_data_dir = './ConsumedData'
    if not os.path.exists(consumed_data_dir):
        os.makedirs(consumed_data_dir)
    
    # Define input files
    aggregate_file = './f1_lapData_2020-2025.csv'
    race_file = './f1_race_laps.csv'
    practice_file = './f1_practice_laps.csv'
    
    # Check if required files exist
    if not os.path.exists(race_file):
        print(f"❌ Race data file not found: {race_file}")
        print("Please run data collection for race sessions first.")
        return None
    
    if not os.path.exists(practice_file):
        print(f"❌ Practice data file not found: {practice_file}")
        print("Please run data collection for practice sessions first.")
        return None
    
    print("\n=== Starting Data Combination ===")
    print(f"Found race data file: {race_file}")
    print(f"Found practice data file: {practice_file}")
    
    try:
        # Read and combine data from available files
        dataframes = []
        
        # Read race file
        print("\nReading race data...")
        race_df = pd.read_csv(race_file)
        race_df = standardize_team_names(race_df)
        dataframes.append(race_df)
        print(f"Read {len(race_df)} rows from race data")
        
        # Read practice file
        print("\nReading practice data...")
        practice_df = pd.read_csv(practice_file)
        practice_df = standardize_team_names(practice_df)
        dataframes.append(practice_df)
        print(f"Read {len(practice_df)} rows from practice data")
        
        # Move input files to ConsumedData directory
        print("\nMoving input files to ConsumedData directory...")
        shutil.move(race_file, os.path.join(consumed_data_dir, os.path.basename(race_file)))
        shutil.move(practice_file, os.path.join(consumed_data_dir, os.path.basename(practice_file)))
        
        # Combine the data
        print("\nCombining data...")
        merged_df = pd.concat(dataframes, ignore_index=True)
        print(f"Combined data contains {len(merged_df)} rows")
        
        # Convert lap times to seconds if they're not already
        print("\nConverting lap times to seconds...")
        lap_time_columns = [col for col in merged_df.columns if 'Lap Time' in col]
        for col in lap_time_columns:
            if merged_df[col].dtype == 'object':  # Only convert if they're strings
                merged_df[col] = convert_lap_times_column(merged_df, col)
        
        # Group by Year, Track, Team, Driver, and LapNumber
        print("\nGrouping data by Year, Track, Team, Driver, and LapNumber...")
        grouped_df = merged_df.groupby(['Year', 'Track', 'Team', 'Driver', 'LapNumber']).agg({
            'FP1 Lap Time': 'first',
            'FP2 Lap Time': 'first',
            'FP3 Lap Time': 'first',
            'Race Lap Time': 'first',
            'Position': 'first'
        }).reset_index()
        
        # Define the final column order
        final_columns = [
            'Year', 'Track', 'Team', 'Driver', 'LapNumber',
            'FP1 Lap Time', 'FP2 Lap Time', 'FP3 Lap Time', 'Race Lap Time',
            'Position'
        ]
        
        # Ensure all columns exist
        for col in final_columns:
            if col not in grouped_df.columns:
                grouped_df[col] = pd.NA
        
        # Reorder columns
        grouped_df = grouped_df[final_columns]
        
        # Save the combined data
        print(f"\nSaving combined data to {aggregate_file}...")
        grouped_df.to_csv(aggregate_file, index=False)
        print(f"✅ Combined data saved to {aggregate_file}")
        
        # Print summary
        print("\n=== Data Summary ===")
        print(f"Total rows: {len(grouped_df)}")
        print(f"Years covered: {sorted(grouped_df['Year'].unique())}")
        print(f"Tracks covered: {len(grouped_df['Track'].unique())}")
        print(f"Drivers covered: {len(grouped_df['Driver'].unique())}")
        
        return aggregate_file
        
    except Exception as e:
        print(f"❌ Error combining data: {str(e)}")
        import traceback
        print("\nFull error traceback:")
        print(traceback.format_exc())
        return None

def convert_lap_time_to_seconds(lap_time):
    """
    Convert FastF1 lap time format to seconds.
    
    Args:
        lap_time: A pandas Series or single value containing lap times in FastF1 format
        
    Returns:
        A pandas Series or single value containing lap times in seconds
    """
    if pd.isna(lap_time):
        return np.nan
    
    # If input is a string, convert to timedelta
    if isinstance(lap_time, str):
        try:
            td = pd.Timedelta(lap_time)
            return td.total_seconds()
        except:
            return np.nan
    
    # If input is already a timedelta
    if isinstance(lap_time, pd.Timedelta):
        return lap_time.total_seconds()
    
    return np.nan

def convert_lap_times_column(df, column_name):
    """
    Convert a column of lap times in a DataFrame to seconds.
    
    Args:
        df: pandas DataFrame containing lap times
        column_name: Name of the column containing lap times
        
    Returns:
        pandas Series with converted lap times in seconds
    """
    return df[column_name].apply(convert_lap_time_to_seconds) 


def aggregate_median_lap_times(input_file='f1_lapData_2020-2025.csv', debug=False):
    """
    Aggregate median lap times for each session (FP1, FP2, FP3, Race) per year, track, team, and driver.
    
    Parameters:
    -----------
    input_file : str, optional
        Path to the input CSV file containing lap data. Defaults to 'f1_lapData_2020-2025.csv'.
    debug : bool, optional
        Whether to print debug information. Defaults to False.
    
    Returns:
    --------
    pandas.DataFrame
        DataFrame containing aggregated median lap times
    """
    try:
        # Read the input file
        if debug:
            print(f"\nReading data from {input_file}...")
        df = pd.read_csv(input_file)
        if debug:
            print(f"Initial data shape: {df.shape}")
            print("\nSample of input data:")
            print(df.head())
        
        # Convert lap time columns to seconds if needed
        time_columns = ['FP1 Lap Time', 'FP2 Lap Time', 'FP3 Lap Time', 'Race Lap Time']
        for col in time_columns:
            if col in df.columns and df[col].dtype == 'object':
                df[col] = convert_lap_times_column(df, col)
        
        # Group by Year, Track, Team, and Driver, then calculate mean for each session
        if debug:
            print("\nGrouping data...")
        
        # Group and calculate mean for lap times
        grouped = df.groupby(['Year', 'Track', 'Team', 'Driver'])[time_columns].mean().reset_index()
        
        # For race sessions, keep the position
        if 'Position' in df.columns:
            position_df = df.groupby(['Year', 'Track', 'Team', 'Driver'])['Position'].first().reset_index()
            grouped = pd.merge(grouped, position_df, on=['Year', 'Track', 'Team', 'Driver'], how='left')
        
        if debug:
            print(f"\nGrouped data shape: {grouped.shape}")
            print("\nSample of grouped data:")
            print(grouped.head())
        
        # Round the lap times to 3 decimal places
        for col in time_columns:
            if col in grouped.columns:
                grouped[col] = grouped[col].round(3)
        
        # Sort by Year, Track, Team, and Driver
        grouped = grouped.sort_values(['Year', 'Track', 'Team', 'Driver'])
        
        if debug:
            print("\nFinal data shape:", grouped.shape)
            print("\nSample of final data:")
            print(grouped.head())
        
        return grouped
        
    except Exception as e:
        print(f"Error processing data: {str(e)}")
        import traceback
        print("\nFull error traceback:")
        print(traceback.format_exc())
        return None

def combine_lap_data_files(race_file='f1_race_laps.csv', practice_file='f1_practice_laps.csv', output_file='combined_laptimes.csv'):
    """
    Combine race and practice lap data files into a single file.
    
    Parameters:
    -----------
    race_file : str, optional
        Path to the race laps CSV file. Defaults to 'f1_race_laps.csv'.
    practice_file : str, optional
        Path to the practice laps CSV file. Defaults to 'f1_practice_laps.csv'.
    output_file : str, optional
        Path to save the combined output file. Defaults to 'combined_laptimes.csv'.
    
    Returns:
    --------
    str
        Path to the combined output file, or None if the operation failed
    """
    try:
        # Check if input files exist
        if not os.path.exists(race_file):
            print(f"❌ Race file not found: {race_file}")
            return None
        if not os.path.exists(practice_file):
            print(f"❌ Practice file not found: {practice_file}")
            return None

        # Read the input files
        print(f"\nReading data from {race_file} and {practice_file}...")
        race_df = pd.read_csv(race_file)
        practice_df = pd.read_csv(practice_file)

        # Standardize team names in both DataFrames
        race_df = standardize_team_names(race_df)
        practice_df = standardize_team_names(practice_df)

        # Convert lap times to seconds if they're not already
        lap_time_columns = [col for col in race_df.columns if 'Lap Time' in col]
        for col in lap_time_columns:
            if race_df[col].dtype == 'object':
                race_df[col] = convert_lap_times_column(race_df, col)
            if practice_df[col].dtype == 'object':
                practice_df[col] = convert_lap_times_column(practice_df, col)

        # Define the merge columns
        merge_columns = ['Year', 'Track', 'Driver', 'LapNumber']

        # Merge the DataFrames
        print("\nCombining data...")
        combined_df = pd.merge(
            race_df,
            practice_df,
            on=merge_columns,
            how='outer',
            suffixes=('_race', '_practice')
        )

        # Clean up duplicate columns
        for col in combined_df.columns:
            if col.endswith('_race') and col.replace('_race', '_practice') in combined_df.columns:
                # Keep the non-NA value from either column
                race_col = col
                practice_col = col.replace('_race', '_practice')
                combined_df[col.replace('_race', '')] = combined_df[race_col].combine_first(combined_df[practice_col])
                combined_df = combined_df.drop([race_col, practice_col], axis=1)

        # Define the final column order
        final_columns = [
            'Year', 'Track', 'Team', 'Driver', 'LapNumber',
            'FP1 Lap Time', 'FP2 Lap Time', 'FP3 Lap Time', 'Race Lap Time',
            'Position'
        ]

        # Ensure all columns exist
        for col in final_columns:
            if col not in combined_df.columns:
                combined_df[col] = pd.NA

        # Reorder columns
        combined_df = combined_df[final_columns]

        # Save the combined data
        combined_df.to_csv(output_file, index=False)
        print(f"\n✅ Combined data saved to {output_file}")
        
        # Print summary
        print("\n=== Data Summary ===")
        print(f"Total rows: {len(combined_df)}")
        print(f"Years covered: {combined_df['Year'].unique()}")
        print(f"Tracks covered: {len(combined_df['Track'].unique())}")
        print(f"Drivers covered: {len(combined_df['Driver'].unique())}")
        
        return output_file

    except Exception as e:
        print(f"❌ Error combining lap data files: {str(e)}")
        import traceback
        print("\nFull error traceback:")
        print(traceback.format_exc())
        return None

########### RUN PROGRAM ###########   
if __name__ == "__main__":
    # Ask if user wants to run data collection
    print("\n=== F1 Data Collection Setup ===")
    print("Would you like to collect new F1 data?")
    print("1. Yes")
    print("2. No")
    
    collect_choice = input("Enter your choice (1-2): ").strip()
    
    if collect_choice == "1":
        # Prompt for session type
        print("\nChoose session type to collect:")
        print("1. Race sessions")
        print("2. Practice sessions (FP1, FP2, FP3)")
        print("3. Use default (Race)")
        
        session_choice = input("Enter your choice (1-3): ").strip()
        
        if session_choice == "1":
            sessions_to_collect = ['Race']
        elif session_choice == "2":
            sessions_to_collect = ['FP1', 'FP2', 'FP3']
        else:
            sessions_to_collect = ['Race']  # Default
        
        # Set default years to 2020-2025
        years = list(range(2020, 2026))
        
        # Run data collection
        print(f"\nStarting data collection for years {years} with sessions: {sessions_to_collect}")
        successful, failed, skipped, output_file = F1_API_Data_Collection(
            years=years,
            sessions_to_collect=sessions_to_collect
        )
        
        # Print detailed results
        if successful:
            print("\n=== Successful Races ===")
            for race in sorted(successful):
                print(f"✅ {race}")
        
        if failed:
            print("\n=== Failed Races ===")
            for race in sorted(failed):
                print(f"❌ {race}")
        
        if skipped:
            print("\n=== Skipped Races Details ===")
            for race in skipped:
                print(f"⚠️ {race}")
    
    # Ask if user wants to combine data
    print("\nWould you like to combine F1 lap data?")
    print("1. Yes")
    print("2. No")
    
    combine_choice = input("Enter your choice (1-2): ").strip()
    
    if combine_choice == "1":
        output_file = combine_practice_and_race_times()

    # Add option to aggregate median lap times
    print("\nWould you like to aggregate median lap times?")
    print("1. Yes")
    print("2. No")
    
    aggregate_choice = input("Enter your choice (1-2): ").strip()
    
    if aggregate_choice == "1":
        # Get the input file path
        input_file = input("Enter the path to the lap data file (default: f1_lapData_2020-2025.csv): ").strip()
        if not input_file:
            input_file = 'f1_lapData_2020-2025.csv'
        
        # Aggregate the data
        result = aggregate_median_lap_times(input_file, debug=True)
        
        if result is not None:
            print("\n=== Results (First 5 rows) ===")
            print(result.head().to_string(index=False))
            
            # Save to CSV
            output_file = 'median_lap_times_by_year.csv'
            result.to_csv(output_file, index=False)
            print(f"\nResults saved to {output_file}")