import os
import fastf1
import pandas as pd
import time

# Set up cache path
cache_path = r'./dataFileDesktop'
if not os.path.exists(cache_path):
    os.makedirs(cache_path)

fastf1.Cache.enable_cache(cache_path)

# Configuration
years = [2020]
years =  [2021, 2022, 2023, 2024]
# Choose which sessions to collect by uncommenting the desired line
#sessions_to_collect = ['FP1', 'FP2', 'FP3']  # For practice sessions
sessions_to_collect = ['Race']  # For race sessions

# Comprehensive list of all F1 races from 2020-2024
f1_races = [
    # 2020 Races
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
    'Abu Dhabi Grand Prix',
    
    # 2021 Races
    # 'Bahrain Grand Prix',
    # 'Emilia Romagna Grand Prix',
    # 'Portuguese Grand Prix',
    # 'Spanish Grand Prix',
    # 'Monaco Grand Prix',
    # 'Azerbaijan Grand Prix',
    # 'French Grand Prix',
    # 'Styrian Grand Prix',
    # 'Austrian Grand Prix',
    # 'British Grand Prix',
    # 'Hungarian Grand Prix',
    # 'Belgian Grand Prix',
    # 'Dutch Grand Prix',
    # 'Italian Grand Prix',
    # 'Russian Grand Prix',
    # 'Turkish Grand Prix',
    # 'United States Grand Prix',
    # 'Mexico City Grand Prix',
    # 'São Paulo Grand Prix',
    #'Qatar Grand Prix',
    'Saudi Arabian Grand Prix',
    'Abu Dhabi Grand Prix',
    
    # 2022 Races
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
    'Abu Dhabi Grand Prix',
    
    # 2023 Races
    'Bahrain Grand Prix',
    'Saudi Arabian Grand Prix',
    'Australian Grand Prix',
    'Azerbaijan Grand Prix',
    'Miami Grand Prix',
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
    'Abu Dhabi Grand Prix',
    
    # 2024 Races
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
]

all_laps = []
successful_races = set()
failed_races = set()

for year in years:
    try:
        schedule = fastf1.get_event_schedule(year, include_testing=False)
        for _, event in schedule.iterrows():
            location = event['EventName']
            if location not in f1_races:
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

                except Exception as e:
                    print(f"⚠️ Skipped {year} {location} {session_type}: {e}")
                print("Sleeping for 4 seconds between API calls...")
                time.sleep(4)
            
            if race_success:
                successful_races.add(location)
            else:
                failed_races.add(location)
                
    except Exception as e:
        print(f"⚠️ Skipped year {year}: {e}")
        print("Sleeping for 4 seconds before next year...")
        time.sleep(4)

# Concatenate all collected lap data
if all_laps:
    df = pd.concat(all_laps, ignore_index=True)
    df = df[['Year', 'Track', 'Session', 'Team', 'Driver', 'Rainfall', 'TrackTemperature', 'LapNumber', 'LapTime']]
    
    # Determine output filename based on session type
    session_type = sessions_to_collect[0].lower()
    if len(sessions_to_collect) > 1:
        session_type = 'practice'
    output_file = f'f1_{session_type}_laps_{years[0]}.csv'
    
    # Check if file exists and append if it does
    if os.path.exists(output_file):
        existing_df = pd.read_csv(output_file)
        combined_df = pd.concat([existing_df, df], ignore_index=True)
        combined_df = combined_df.drop_duplicates()
        combined_df.to_csv(output_file, index=False)
        print(f"\n✅ Data appended to {output_file}")
    else:
        df.to_csv(output_file, index=False)
        print(f"\n✅ New data saved to {output_file}")
else:
    print("\n❌ No lap data was collected.")

# Print summary of successful and failed races
print("\n=== Data Collection Summary ===")
print(f"Successful races: {len(successful_races)}")
print(f"Failed races: {len(failed_races)}")
if failed_races:
    print("Failed races:", ", ".join(failed_races))