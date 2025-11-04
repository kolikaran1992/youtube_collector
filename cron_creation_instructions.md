# 🤖 YouTube Collector Agent Development Guide

This guide contains the essential configuration and conceptual links for developing cron scripts within the YouTube Collector project.

---

## ⚙️ Core Configuration

The central source for all dynamic variables, queue paths, and limits is:
* **[Config File]:** [settings file][settings_toml]

**Key Configuration Variables for Cron Scripts:**
| Variable Name                             | Description                                                                       | Used In                                                                                      |
| :---------------------------------------- | :-------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------- |
| `channels_to_monitor`                     | List of channels for standard **video** (caption-only) flow.                      | `cron__fetch_urls.py`                                                                        |
| `channels_to_monitor_plus_video_download` | List of channels for **Shorts/Video Download** flow.                              | `cron__fetch_urls.py`                                                                        |
| `yt_caption_fetching_queue_dir`           | **Source Queue** for caption jobs; **Destination Queue** for video download jobs. | All cron scripts                                                                             |
| `yt_video_download_queue_dir`             | **Source Queue** for video download jobs.                                         | `cron__fetch_urls.py`, `cron__kaggle_job_yt_video_download.py`                               |
| `MAX_NEW_URLS_TO_FETCH`                   | Max videos/shorts to fetch per channel in one run.                                | `cron__fetch_urls.py`                                                                        |
| `max_videos_to_download`                  | Max videos to process per video download Kaggle job.                              | `cron__kaggle_job_yt_video_download.py` (via `KAGGLE_JOB_YT_VIDEO_DOWNLOAD_URLS_TO_PROCESS`) |

---

## 📦 Job Templates

Kaggle job scripts are dynamically generated from these templates. New cron scripts should use or copy the appropriate template and substitution logic (`{{KEY}}` placeholders).
* **[Caption Job Template]:** `yt_collector/kaggle_job_caption.txt`
* **[Video Download Job Template]:** `yt_collector/kaggle_job_video_download.txt`

---

## 🔄 Collector Queue Flow & Keys

Cron scripts utilize the **`yt_collector.job_queue.JobQueue`** class to manage sequential processing. Each Kaggle job script must:
1.  Pop jobs from its **SOURCE\_QUEUE**.
2.  Add its specific tracking metadata under a unique key.
3.  Push jobs to the next step's **DESTINATION\_QUEUE**.

| Processing Stage     | Source Queue Variable           | Destination Queue Variable                                                              | Job Data Key (for tracking)       |
| :------------------- | :------------------------------ | :-------------------------------------------------------------------------------------- | :-------------------------------- |
| **Fetch URLs**       | N/A (Fetches from YT)           | `yt_video_download_queue_dir` or `yt_caption_fetching_queue_dir`                        | N/A                               |
| **Video Download**   | `yt_video_download_queue_dir`   | `YT_VIDEO_DOWNLOAD_DESTINATION_QUEUE_DIR` (resolves to `yt_caption_fetching_queue_dir`) | `'kaggle_job_yt_video_download'`  |
| **Caption Fetching** | `yt_caption_fetching_queue_dir` | `yt_info_fetching_queue_dir`                                                            | `'kaggle_job_yt_captions'`        |
| **Info Collection**  | `yt_info_fetching_queue_dir`    | `resting_queue_dir`                                                                     | `'kaggle_job_yt_info_collection'` |

[settings_toml]: settings_file/yt_collector.toml