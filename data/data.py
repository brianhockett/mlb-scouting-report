import pybaseball as pb
import pandas as pd

# Define the date ranges by month
months = [
    ('2025-03-30', '2025-03-31'),
    ('2025-04-01', '2025-04-30'),
    ('2025-05-01', '2025-05-31'),
    ('2025-06-01', '2025-06-30'),
    ('2025-07-01', '2025-07-31'),
    ('2025-08-01', '2025-08-31'),
    ('2025-09-01', '2025-09-30'),
    ('2025-10-01', '2025-10-01')
]

all_data = []

for start, end in months:
    print(f"Downloading Statcast data from {start} to {end}")
    chunk = pb.statcast(start, end)
    all_data.append(chunk)

# Concatenate all chunks into one DataFrame
statcast_data_2025 = pd.concat(all_data, ignore_index=True)

# Save to Parquet
statcast_data_2025.to_parquet('./statcast_data_2025.parquet', index=False)
print("Saved full 2025 Statcast data to data/statcast_data_2025.parquet")
