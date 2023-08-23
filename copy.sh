#!/bin/bash

# Function to check if a date is valid
function isValidDate() {
    date -d "$1" >/dev/null 2>&1
}

startDate=$1
endDate=$2
baseLocation=$3
projectLocation=$4
filePattern=$5
targetLocation=$6

# Check if start and end dates are valid
if ! isValidDate "$startDate"; then
    echo "Error: Invalid start date"
    exit 1
fi

if ! isValidDate "$endDate"; then
    echo "Error: Invalid end date"
    exit 1
fi

# Check if base location is a valid directory
if [ ! -d "$baseLocation" ]; then
    echo "Error: Invalid base location"
    exit 1
fi

# Check if target location is a valid directory
if [ ! -d "$targetLocation" ]; then
    echo "Error: Invalid target location"
    exit 1
fi

# Convert dates to timestamps
startTimestamp=$(date -d "$startDate" +%s)
endTimestamp=$(date -d "$endDate" +%s)

# Set a flag to indicate if a file was found and copied
fileCopied=0

# Loop through each day between start and end dates (inclusive)
for ((currentTimestamp=startTimestamp; currentTimestamp<=endTimestamp; currentTimestamp+=86400)); do
    # Convert timestamp to date in yyyymmdd format
    currentDate=$(date -d "@$currentTimestamp" +%Y%m%d)
    # Check if file with defined pattern exists in the directory
    files=("$baseLocation/$currentDate/$projectLocation/$filePattern")
    if [ ${#files[@]} -gt 0 ]; then
        for file in "${files[@]}"; do
            cp "$file" "$targetLocation"
            echo "File $file copied to $targetLocation"
        done
        fileCopied=1
        break
    fi
done

if [ $fileCopied -eq 1 ]; then
    echo "File with pattern $filePattern found and copied to target location"
    exit 0
else
    echo "No file with pattern $filePattern found in any directory"
    exit 1
fi
