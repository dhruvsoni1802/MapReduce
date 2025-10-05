#!/bin/bash

# Script to merge mr-out-1 through mr-out-9 into mr-out-final
# Maintains sorted order by combining and sorting all entries

echo "Merging mr-out files 1-9 into mr-out-final..."

# Create the output directory if it doesn't exist
mkdir -p mapreducefinal

# Combine all mr-out files (1-9) and sort them
# The sort command will handle the alphabetical ordering
cat mapreducefinal/mr-out-{1..9} | sort > mapreducefinal/mr-out-final

# Count the total number of unique words
total_words=$(wc -l < mapreducefinal/mr-out-final)

echo "Merge completed successfully!"
echo "Total unique words in mr-out-final: $total_words"
echo "Output file: mapreducefinal/mr-out-final"

# Show first 10 lines as a preview
echo ""
echo "First 10 lines of merged output:"
head -10 mapreducefinal/mr-out-final
