#!/bin/bash

# This script executes the YouTube collector cron jobs in the correct sequence
# with a 5-minute interval between each step.

# --- Configuration ---
# The directory where the cron scripts are located (assuming the script is run from project root)
CRON_SCRIPT_DIR="."
# Sleep duration in seconds (5 minutes)
SLEEP_DURATION=300

# --- Execution Sequence ---
echo "\n🚀 Starting YouTube Collector Cron Sequence..."

# 1. Fetch URLs
echo "\n[1/4] Running cron__fetch_urls.py..."
~/.local/bin/poetry run python "${CRON_SCRIPT_DIR}/cron__fetch_urls.py"
EXIT_CODE=$?
if [ $EXIT_CODE -ne 0 ]; then
    echo "⚠️ cron__fetch_urls.py failed (Exit Code: $EXIT_CODE). Continuing to next step..."
fi

# Wait 5 minutes
echo "\n⏳ Waiting ${SLEEP_DURATION} seconds (5 minutes) before the next step..."
sleep $SLEEP_DURATION

# 2. Submit Video Download Job
echo "\n[2/4] Running cron__kaggle_job_yt_video_download.py..."
~/.local/bin/poetry run python "${CRON_SCRIPT_DIR}/cron__kaggle_job_yt_video_download.py"
EXIT_CODE=$?
if [ $EXIT_CODE -ne 0 ]; then
    echo "⚠️ cron__kaggle_job_yt_video_download.py failed (Exit Code: $EXIT_CODE). Continuing to next step..."
fi

# Wait 5 minutes
echo "\n⏳ Waiting ${SLEEP_DURATION} seconds (5 minutes) before the next step..."
sleep $SLEEP_DURATION

# 3. Submit Captions Job
echo "\n[3/4] Running cron__kaggle_job_yt_captions.py..."
~/.local/bin/poetry run python "${CRON_SCRIPT_DIR}/cron__kaggle_job_yt_captions.py"
EXIT_CODE=$?
if [ $EXIT_CODE -ne 0 ]; then
    echo "⚠️ cron__kaggle_job_yt_captions.py failed (Exit Code: $EXIT_CODE). Continuing to next step..."
fi

# Wait 5 minutes
echo "\n⏳ Waiting ${SLEEP_DURATION} seconds (5 minutes) before the next step..."
sleep $SLEEP_DURATION

# 4. Submit Info Collection Job
echo "\n[4/4] Running cron__kaggle_job_yt_info_collection.py..."
~/.local/bin/poetry run python "${CRON_SCRIPT_DIR}/cron__kaggle_job_yt_info_collection.py"
EXIT_CODE=$?
if [ $EXIT_CODE -ne 0 ]; then
    echo "⚠️ cron__kaggle_job_yt_info_collection.py failed (Exit Code: $EXIT_CODE). Sequence finished."
fi

echo "\n✅ Cron sequence finished."