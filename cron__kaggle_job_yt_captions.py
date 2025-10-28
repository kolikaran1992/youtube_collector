import os
import json
import uuid
from pathlib import Path
import traceback # Added for error reporting
# Assuming crongle and omniconf are available in the environment
from crongle import KernelLauncher
from omniconf import config
from slack_messenger import send_slack_message as send_slack_message_base # Added for job notifications

# --- CONFIGURATION RETRIEVAL AND JOB CONSTANTS ---
# Assuming 'ytcc' environment provides all necessary configuration keys
yt_config = config

# Define the paths and parameters for this specific job
TEMPLATE_FILE = yt_config.KAGGLE_JOB_YT_CAPTION_TEMPLATE_FILE
# Output script location is placed in a sub-directory of BASE_OUTPUT_DIR
OUTPUT_SCRIPT_FILE = Path('yt_collector', "yt_caption_job_executable.py").as_posix()
JOB_NAME = "yt-caption-collector" # A descriptive name for this job type
BASE_OUTPUT_DIR = yt_config.video_automatic_captions

# --- HELPER FUNCTIONS ---
def send_slack_message_wrapper(message):
    send_slack_message_base(message=message, header=f'*YT-COLLECTOR-JOB: `{Path(__file__).name}`*')

def substitute_placeholders(template_content: str, config_data: dict) -> str:
    """
    Replaces {{KEY}} placeholders in the template with values from the config_data.
    Values are substituted as their raw string representation.
    """
    content = template_content
    for key, value in config_data.items():
        placeholder = "{{" + key + "}}"
        # For lists (like video_ids_list), the calling code must ensure the value
        # is a properly formatted JSON string so it pastes correctly into the Python template.
        content = content.replace(placeholder, str(value))
    return content

# New function to get file paths and IDs
def get_queue_files_and_video_ids() -> tuple[list[Path], list[str]]:
    """
    Scans the remaining video URL queue directory for *.json files, extracts their stems
    as video IDs, and returns both the file paths and the IDs.
    """
    video_ids = []
    file_paths = []
    # We use the path specified in config.remaining_video_url_queue
    queue_path = Path(yt_config.remaining_video_url_queue).resolve()
    print(f"Scanning queue directory for video IDs: {queue_path}")

    if not queue_path.is_dir():
        print(f"Queue directory not found: {queue_path}. Returning empty list.")
        return file_paths, video_ids

    # rglob finds all files matching the pattern recursively
    for file_path in queue_path.rglob('*.json'):
        # The file stem (filename without extension) is assumed to be the video ID
        video_ids.append(file_path.stem)
        file_paths.append(file_path)

    print(f"Found {len(video_ids)} videos to process.")
    return file_paths[:config.KAGGLE_JOB_MAXIMUM_URLS_TO_PROCESS], video_ids[:config.KAGGLE_JOB_MAXIMUM_URLS_TO_PROCESS]


def move_files_to_processing(file_paths: list[Path]):
    """
    Moves the list of files from the remaining queue to the processing queue,
    maintaining their relative path structure.
    """
    processing_queue_root = Path(yt_config.processing_caption_video_url_queue).resolve()
    remaining_queue_root = Path(yt_config.remaining_video_url_queue).resolve()
    
    # Ensure the destination directory exists
    processing_queue_root.mkdir(parents=True, exist_ok=True)
    
    print(f"Moving {len(file_paths)} files to processing queue: {processing_queue_root}")

    for file_path in file_paths:
        try:
            # Calculate the relative path from the remaining queue root
            relative_path = file_path.relative_to(remaining_queue_root)
            # Calculate the new path in the processing queue root
            destination_path = processing_queue_root / relative_path
            
            # Ensure parent directories in the destination exist
            destination_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Perform the move (rename)
            file_path.rename(destination_path)
        except Exception as e:
            print(f"Error moving file {file_path}: {e}")
    
    print("Move operation completed.")


# --- MAIN EXECUTION ---
def run_job():
    """Retrieves job parameters, generates the executable script, and submits the job to Kaggle."""
    print("🎬 Starting dynamic caption job generation and submission...")

    # 1. Prepare dynamic substitution data
    file_paths, video_ids = get_queue_files_and_video_ids()

    if not video_ids:
        abort_message = "🛑 No new video IDs found in the queue. Job submission aborted."
        print(abort_message)
        send_slack_message_wrapper(message=f"🛑 `Job Aborted`: {abort_message}")
        return None

    # Construct the dictionary for placeholder substitution
    job_substitution_data = {
        # The quota for minutes, substituted directly
        "minutes_to_use": yt_config.KAGGLE_JOB_MINUTES_QOUTA,
        # The list of video IDs, must be dumped to a JSON string for Python substitution in the template
        "video_ids_list": json.dumps(video_ids),
    }

    print(f"✅ Loaded job parameters: minutes_to_use={job_substitution_data['minutes_to_use']}")

    # 2. Load Template and Cookies
    try:
        with open(TEMPLATE_FILE, "r") as f:
            template_content = f.read()
    except FileNotFoundError:
        print(f"❌ Error: Template file not found at {TEMPLATE_FILE}.")
        return None

    # 3. Substitute and Generate Final Python Script
    final_script_content = substitute_placeholders(template_content, job_substitution_data)

    # Ensure the output directory for the script exists
    output_dir = os.path.dirname(OUTPUT_SCRIPT_FILE)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    with open(OUTPUT_SCRIPT_FILE, "w") as f:
        f.write(final_script_content)

    print(
        f"✅ Generated executable script: {OUTPUT_SCRIPT_FILE}. Variables substituted."
    )

    # 4. Programmatic Crongle Submission
    print("🚀 Submitting job using Crongle KernelLauncher...")

    # Generate a unique ID for this specific run
    run_uuid = str(uuid.uuid4())[:8]
    final_kernel_name = f"crongle-job-{JOB_NAME}-{run_uuid}"

    # Define local output folder (where results will download)
    output_path = Path(BASE_OUTPUT_DIR).joinpath(final_kernel_name).resolve()
    output_path.mkdir(exist_ok=True, parents=True)
    output_folder_posix = output_path.as_posix()

    # Kernel options (using reasonable defaults)
    kernel_type = "cpu"
    polling_freq = 15

    nb_options = {
        "title": final_kernel_name,
        "is_private": True,
        "enable_gpu": kernel_type == "gpu",
        "enable_internet": True,
    }

    def just_print(**kwargs):
        sep = '-'*10
        print_sep = lambda x: print(f'{sep}param-list: {x}{sep}')
        print_sep('start')
        for key, val in kwargs.items():
            print(f'{key}: {val}')
        print_sep('end')
    
    try:
        job_id = KernelLauncher().submit_job(
        # just_print(
            kernel_name=final_kernel_name,
            script_path=OUTPUT_SCRIPT_FILE,
            output_folder=output_folder_posix,
            # Polling/Timeout settings (using reasonable defaults)
            timeout=3600 * 1, # 1 hours
            interval_amount=polling_freq,
            interval_unit="minute",
            kernel_kwargs=nb_options,
            # Optional: Slack channel ID for notifications (checking for attribute existence)
            slack_channel_id=config.crongle.slack.channel_id if hasattr(config, 'crongle') and hasattr(config.crongle, 'slack') else None,
            # Optional: Slack bot token for notifications (checking for attribute existence)
            slack_bot_token=config.slack.bot_token if hasattr(config, 'slack') else None,
        )

        success_message = (
            f"🎉 Job Submitted Successfully!\n"
            f"*Kernel Name*: `{final_kernel_name}`\n"
            f"*Videos to Process*: {len(video_ids)}\n"
            f"*Output Dir*: `{output_folder_posix}`\n"
            f"*Kernel Link*: `https://www.kaggle.com/code/kolikaran/{final_kernel_name}`"
        )
        print(success_message)
        send_slack_message_wrapper(message=success_message)
        
        # 5. Move files to processing queue
        move_files_to_processing(file_paths)

    except Exception as e:
        tb = traceback.format_exc()
        error_message = (
            f"❌ *Kaggle Submission Failed*\n"
            f"*Error*: {e}\n"
            f"*Attempted Kernel Name*: `{final_kernel_name}`\n"
            f"*Traceback*:\n```\n{tb}\n```"
        )
        print(f"❌ Crongle Submission Failed: {e}")
        send_slack_message_wrapper(message=error_message)
        return None

    return job_id


if __name__ == "__main__":
    run_job()
