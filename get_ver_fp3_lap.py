import fastf1



# Load the Singapore 2024 FP3 session
session = fastf1.get_session(2024, 'Singapore', 'FP3')
session.load()

# Get VER's lap 2 time
ver_lap = session.laps.pick_driver('VER').pick_lap(2)
print(f"VER's FP3 Lap 2 time in Singapore 2024: {ver_lap['LapTime']}") 