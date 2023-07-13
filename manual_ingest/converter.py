import pandas as pd
from dbc_reader import DbcReader

path = '/home/chic/repos/data_sus_tik/fct-unesp-datasus/landing/SIHSUS/RDRO_2107.dbc'

# Read the rows from the .dbc file using DbcReader
rows = [row for row in DbcReader(path)]

# Create a DataFrame from the rows
df = pd.DataFrame(rows)

# Print the DataFrame
print(df)
