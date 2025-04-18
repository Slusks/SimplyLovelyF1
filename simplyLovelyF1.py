import os
import fastf1
import pandas as pd
import time

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

                        # Merge with weather data if available
                        if session.weather_data is not None and not session.weather_data.empty:
                            laps = laps.merge(session.weather_data, on='Time', how='left')
                        else:
                            laps['TrackTemperature'] = pd.NA
                            laps['Rainfall'] = pd.NA

                        # Add missing columns if not present
                        if 'TrackTemperature' not in laps.columns:
                            laps['TrackTemperature'] = pd.NA
                        if 'Rainfall' not in laps.columns:
                            laps['Rainfall'] = pd.NA

                        # Extract only necessary columns
                        subset = laps[['Driver', 'Team', 'LapNumber', 'LapTime',
                                     'TrackTemperature', 'Rainfall']].copy()
                        subset['Year'] = year
                        subset['Track'] = location
                        subset['Session'] = session_type

                        all_laps.append(subset)
                        print(f"✅ Added: {year} {location} {session_type} - {len(subset)} laps")
                        race_success = True
                        consecutive_failures = 0  # Reset consecutive failures on success

                    except Exception as e:
                        print(f"⚠️ Skipped {year} {location} {session_type}: {e}")
                        skipped_races.append(f"{year} - {location} - {session_type}: {str(e)}")
                    print("Sleeping for 4 seconds between API calls...")
                    time.sleep(4)
                
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
            print("Sleeping for 4 seconds before next year...")
            time.sleep(4)

    # Concatenate all collected lap data
    if all_laps:
        df = pd.concat(all_laps, ignore_index=True)
        df = df[['Year', 'Track', 'Session', 'Team', 'Driver', 'Rainfall', 'TrackTemperature', 'LapNumber', 'LapTime']]
        
        # Create separate DataFrames for practice and race sessions
        practice_df = df[df['Session'].isin(['FP1', 'FP2', 'FP3'])].copy()
        race_df = df[df['Session'] == 'Race'].copy()
        
        # Rename LapTime columns
        practice_df = practice_df.rename(columns={'LapTime': 'PracticeLapTime'})
        race_df = race_df.rename(columns={'LapTime': 'RaceLapTime'})
        
        # Drop Session column as it's no longer needed
        practice_df = practice_df.drop(columns=['Session'])
        race_df = race_df.drop(columns=['Session'])
        
        # Merge practice and race data
        # First, get unique combinations of Year, Track, Driver, and LapNumber from both DataFrames
        all_combinations = pd.concat([
            practice_df[['Year', 'Track', 'Driver', 'LapNumber']],
            race_df[['Year', 'Track', 'Driver', 'LapNumber']]
        ]).drop_duplicates()
        
        # Merge with practice data
        merged_df = all_combinations.merge(
            practice_df,
            on=['Year', 'Track', 'Driver', 'LapNumber'],
            how='left'
        )
        
        # Merge with race data
        merged_df = merged_df.merge(
            race_df,
            on=['Year', 'Track', 'Driver', 'LapNumber'],
            how='left'
        )
        
        # Reorder columns for better readability
        merged_df = merged_df[[
            'Year', 'Track', 'Driver', 'Team', 'LapNumber',
            'PracticeLapTime', 'RaceLapTime',
            'Rainfall', 'TrackTemperature'
        ]]
        
        # Determine output filename
        output_file = f'f1_lap_times_{min(years)}-{max(years)}.csv'
        
        # Check if file exists and append if it does
        if os.path.exists(output_file):
            existing_df = pd.read_csv(output_file)
            combined_df = pd.concat([existing_df, merged_df], ignore_index=True)
            # Remove duplicates based on Year, Track, Driver, and LapNumber
            combined_df = combined_df.drop_duplicates(subset=['Year', 'Track', 'Driver', 'LapNumber'])
            combined_df.to_csv(output_file, index=False)
            print(f"\n✅ Data appended to {output_file}")
        else:
            merged_df.to_csv(output_file, index=False)
            print(f"\n✅ New data saved to {output_file}")
    else:
        print("\n❌ No lap data was collected.")
        output_file = None

    # Print summary of successful and failed races
    print("\n=== Data Collection Summary ===")
    print(f"Successful races: {len(successful_races)}")
    print(f"Failed races: {len(failed_races)}")

    return successful_races, failed_races, skipped_races, output_file

def combine_practice_and_race_times(years=None):
    """
    Combine practice and race lap times from existing CSV files into a single table.
    
    Parameters:
    -----------
    years : list, optional
        List of years to combine data for. If None, will use all available years.
    
    Returns:
    --------
    str
        Path to the combined output file
    """
    # Find all practice and race CSV files
    practice_files = [f for f in os.listdir('.') if f.startswith('f1_practice_laps_') and f.endswith('.csv')]
    race_files = [f for f in os.listdir('.') if f.startswith('f1_race_laps_') and f.endswith('.csv')]
    
    if not practice_files or not race_files:
        print("❌ No practice or race files found. Please run data collection first.")
        return None
    
    # Read and combine all practice files
    practice_dfs = []
    for file in practice_files:
        file_years = file.split('_')[-1].replace('.csv', '').split('-')
        if years is None or any(int(y) in years for y in file_years):
            df = pd.read_csv(file)
            practice_dfs.append(df)
    
    if not practice_dfs:
        print("❌ No practice data found for specified years.")
        return None
    
    practice_df = pd.concat(practice_dfs, ignore_index=True)
    
    # Read and combine all race files
    race_dfs = []
    for file in race_files:
        file_years = file.split('_')[-1].replace('.csv', '').split('-')
        if years is None or any(int(y) in years for y in file_years):
            df = pd.read_csv(file)
            race_dfs.append(df)
    
    if not race_dfs:
        print("❌ No race data found for specified years.")
        return None
    
    race_df = pd.concat(race_dfs, ignore_index=True)
    
    # Rename LapTime columns
    practice_df = practice_df.rename(columns={'LapTime': 'PracticeLapTime'})
    race_df = race_df.rename(columns={'LapTime': 'RaceLapTime'})
    
    # Drop Session column as it's no longer needed
    practice_df = practice_df.drop(columns=['Session'])
    race_df = race_df.drop(columns=['Session'])
    
    # Get unique combinations of Year, Track, Team, Driver,  and LapNumber
    all_combinations = pd.concat([
        practice_df[['Year', 'Track', 'Team', 'Driver', 'LapNumber']],
        race_df[['Year', 'Track', 'Team', 'Driver', 'LapNumber']]
    ]).drop_duplicates()
    
    # Merge with practice data
    merged_df = all_combinations.merge(
        practice_df,
        on=['Year', 'Track', 'Team', 'Driver', 'LapNumber'],
        how='left'
    )
    
    # Merge with race data
    merged_df = merged_df.merge(
        race_df,
        on=['Year', 'Track', 'Team', 'Driver', 'LapNumber'],
        how='left'
    )
    
    # Reorder columns for better readability
    merged_df = merged_df[[
        'Year', 'Track', 'Team', 'Driver', 'LapNumber',
        'PracticeLapTime', 'RaceLapTime',
        'Rainfall', 'TrackTemperature'
    ]]
    
    # Determine output filename
    if years:
        year_range = f"{min(years)}-{max(years)}"
    else:
        year_range = "all"
    output_file = f'f1_combined_lap_times_{year_range}.csv'
    
    # Save the combined data
    merged_df.to_csv(output_file, index=False)
    print(f"\n✅ Combined data saved to {output_file}")
    
    return output_file

########### RUN PROGRAM ###########   
if __name__ == "__main__":
    # Prompt for session type
    print("\n=== F1 Data Collection Setup ===")
    print("Choose session type to collect:")
    print("1. Race sessions")
    print("2. Practice sessions (FP1, FP2, FP3)")
    print("3. Use default (Race)")
    print("4. Combine existing practice and race data")
    
    session_choice = input("Enter your choice (1-4): ").strip()
    
    if session_choice == "4":
        # Prompt for years to combine
        print("\nChoose years to combine:")
        print("1. 2024 (Current season)")
        print("2. 2025 (Upcoming season)")
        print("3. Both 2024 and 2025")
        print("4. All available years")
        print("5. Custom year(s)")
        
        year_choice = input("Enter your choice (1-5): ").strip()
        
        if year_choice == "1":
            years = [2024]
        elif year_choice == "2":
            years = [2025]
        elif year_choice == "3":
            years = [2024, 2025]
        elif year_choice == "4":
            years = None  # Will use all available years
        elif year_choice == "5":
            custom_years = input("Enter years separated by commas (e.g., 2024,2025): ").strip()
            years = [int(year.strip()) for year in custom_years.split(',')]
        else:
            years = [2024]  # Default
            
        output_file = combine_practice_and_race_times(years)
    else:
        # Original data collection code
        if session_choice == "1":
            sessions_to_collect = ['Race']
        elif session_choice == "2":
            sessions_to_collect = ['FP1', 'FP2', 'FP3']
        else:
            sessions_to_collect = ['Race']  # Default
        
        # Prompt for year selection
        print("\nChoose years to collect:")
        print("1. 2024 (Current season)")
        print("2. 2025 (Upcoming season)")
        print("3. Both 2024 and 2025")
        print("4. Custom year(s)")
        print("5. Use default (2024)")
        
        year_choice = input("Enter your choice (1-5): ").strip()
        
        if year_choice == "1":
            years = [2024]
        elif year_choice == "2":
            years = [2025]
        elif year_choice == "3":
            years = [2024, 2025]
        elif year_choice == "4":
            custom_years = input("Enter years separated by commas (e.g., 2024,2025): ").strip()
            years = [int(year.strip()) for year in custom_years.split(',')]
        else:
            years = [2024]  # Default
        
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