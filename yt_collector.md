# YouTube Collector — Project Configuration Summary

A lightweight utility for efficiently fetching the latest YouTube video URLs and basic metadata from a channel. It avoids redundant processing by maintaining a queue of only *new* videos for downstream tasks (e.g., detail extraction, download, or analysis).

---

## 1. Central Configuration (`omniconf`)

Configuration is managed through the [`omniconf`](omniconf_py) system, which loads all project-level TOML settings into a unified config object.

### **File:** [`omniconf.py`][omniconf_py]

**Purpose:** Initializes and registers the global configuration (`config`), including references to TOML files in the [`settings_file`][settings_folder] directory.

**Usage Example:**

```python
from omniconf import config, logger

# use the imported logger to log messages everywhere

# Access settings directly from the unified config object, 
# as all TOML files are loaded into the default environment.
LOOKBACK_DAYS = config.yt_collector.lookback_days
```

All configuration files reside in the [`settings_file`][settings_folder] folder.

---

## 2. YouTube Collector Configuration

### **[Settings Folder][settings_folder]**:

Holds all TOML configuration files (e.g., `settings.toml`, `yt_collector.toml`).

**Environment Prefix:** `YT_COLLECTOR_`

---

## 3. Core Scripts

### **[Minimal URL Fetcher][yt_collector_fetch_urls]**

Uses `yt-dlp`’s `extract_flat` mode to quickly list recent videos (newest first).
Ensures:

1. Only the latest `MAX_NEW_URLS` videos are processed.
2. Only new, unseen videos are added to the queue.

---

### **[Local Storage Utilities][yt_collector_local_storage_utils]**

Handles all queue operations — saving new video metadata and preventing duplicates through consistent local storage management.


---

## 4. Cron Jobs (Scheduled Automation)

These scripts are designed to be run periodically via a **cron scheduler** for continuous operation.

*[URL Fetching from Channels][cron_fetch_urls]*
Purpose: Runs the minimal URL fetcher across all configured channels and adds any new video URLs to the queue. Includes a random time delay (jitter) between channel processes. Writes new video metadata files to the [yt_collector_toml].`remaining_video_url_queue`.

*[Kaggle Caption Job Cron][cron_kaggle_caption_job]*
Purpose: Scans the [yt_collector_toml].`remaining_video_url_queue` for new videos, dynamically generates a Python script with video IDs, and submits it as a dedicated Kaggle Kernel job to retrieve automatic captions. Moves processed video files to the [yt_collector_toml].`processing_caption_video_url_queue`.

*[Kaggle Video Info Fetcher Cron][cron_kaggle_info_collection_job]*
Purpose: Scans the [yt_collector_toml].`processing_caption_video_url_queue` (and checks for corresponding completed caption files in video_automatic_captions), dynamically generates a Python script with video IDs, and submits it as a dedicated Kaggle Kernel job for video info collection. Moves the files to the `processing_info_collection_video_url_queue`

[omniconf_py]: omniconf.py
[settings_folder]: settings_file
[yt_collector_fetch_urls]: yt_collector/fetch_urls.py
[yt_collector_local_storage_utils]: yt_collector/local_storage_utils.py
[yt_collector_toml]: settings_file/yt_collector.toml
[cron_fetch_urls]: cron__fetch_urls.py
[cron_kaggle_caption_job]: cron__kaggle_job_yt_captions.py
[cron_kaggle_info_collection_job]: cron__kaggle_job_yt_info_collection.py