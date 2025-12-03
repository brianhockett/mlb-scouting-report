import pybaseball as pb
import pandas as pd

def preprocess_statcast(data: pd.DataFrame):
    df = data.copy()

    # Descriptions that are considered swings
    swing = ['hit_into_play', 'foul', 'swinging_strike', 'swinging_strike_blocked', 
             'foul_tip', 'foul_bunt', 'missed_bunt', 'bunt_foul_tip']

    # Descriptions that are considered whiffs
    whiff = ['swinging_strike', 'swinging_strike_blocked', 'foul_tip']

    # Columns for swing and whiff
    df['swing'] = df['description'].isin(swing)
    df['whiff'] = df['description'].isin(whiff)

    # Columns for whether the pitch was in or out of the zone (1-9 are strikes, 10+ are balls)
    df['in_zone'] = df['zone'] < 10
    df['out_zone'] = df['zone'] > 10

    # Column for whether a swing was a chase
    df['chase'] = df['swing'] & ~df['in_zone']

    # Convert break values to inches
    df['pfx_z'] = df['pfx_z'] * 12
    df['pfx_x'] = df['pfx_x'] * 12

    return df

# Define the date ranges by month
months = [
    ('2025-03-30', '2025-03-31'),
    ('2025-04-01', '2025-04-30'),
    ('2025-05-01', '2025-05-31'),
    ('2025-06-01', '2025-06-30'),
    ('2025-07-01', '2025-07-31'),
    ('2025-08-01', '2025-08-31'),
    ('2025-09-01', '2025-09-30')
]

all_data = []

for start, end in months:
    print(f"Downloading Statcast data from {start} to {end}")
    chunk = pb.statcast(start, end)
    all_data.append(chunk)

# Concatenate all chunks into one DataFrame
statcast_data_2025 = pd.concat(all_data, ignore_index=True)

# Apply preprocessing
statcast_data_2025 = preprocess_statcast(statcast_data_2025)

# Save to Parquet
statcast_data_2025.to_parquet('./statcast_data_2025.parquet', index=False)
print("Saved processed 2025 Statcast data to ./statcast_data_2025.parquet")
