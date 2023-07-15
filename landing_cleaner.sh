#!/bin/bash

# Specify the directory path
directory="/home/chic/repos/data_sus_tik/fct-unesp-datasus/landing/SIHSUS"

# Navigate to the directory
cd "$directory" || exit

# Remove files with the ".dbc" extension
find . -type f -name "*.dbc" -delete

echo "Files with the '.dbc' extension have been removed."