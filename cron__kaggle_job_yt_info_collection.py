import os
import json
import uuid
from pathlib import Path
import traceback
from crongle import KernelLauncher
from omniconf import config
from slack_messenger import send_slack_message as send_slack_message_base

# --- CONFIGURATION RETRIEVAL AND JOB CONSTANTS ---
yt_config = config
# Define the paths and parameters for this specific job
TEMPLATE_FILE = yt_config.KAGGLE_JOB_YT_INFO_COLLECTION_TEMPLATE_FILE # NEW TEMPLATE
# Output script location is placed in a sub-directory of BASE_OUTPUT_DIR
OUTPUT_SCRIPT_FILE = Path('yt_collector', "yt_info_collection_job_executable.py").as_posix() # NEW SCRIPT NAME
JOB_NAME = "yt-info-collector" # NEW JOB NAME
BASE_OUTPUT_DIR = yt_config.video_detailed_info # Target output dir for info

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

# MODIFIED function to get file paths and IDs (Intersection Logic)
def get_queue_files_and_video_ids() -> tuple[list[Path], list[str]]:
    """
    Scans the processing queue for *.json files AND checks for corresponding caption files.
    Returns the intersection of video IDs.
    """
    queue_path = Path(yt_config.processing_caption_video_url_queue).resolve() # Files from previous step
    captions_path = Path(yt_config.video_automatic_captions).resolve() # Completed caption files
    print(f"Scanning processing queue for video IDs: {queue_path}")
    
    if not queue_path.is_dir():
        print(f"Queue directory not found: {queue_path}. Returning empty list.")
        return [], []

    # 1. Collect all video IDs from the current processing queue
    queue_stems = set()
    queue_file_paths = {}
    for file_path in queue_path.rglob('*.json'):
        stem = file_path.stem
        queue_stems.add(stem)
        queue_file_paths[stem] = file_path

    print(f"Found {len(queue_stems)} videos in the caption processing queue.")

    # 2. Collect all video IDs that have a successful English caption file
    caption_stems = set()
    for file_path in captions_path.rglob('*.en.json3'): # Looking for .en.json3 extension
        caption_stems.add(file_path.stem.split('.')[0]) # Stem is the ID before .en.json3

    print(f"Found {len(caption_stems)} videos with English captions.")

    # 3. Calculate the intersection (videos that need info and have captions)
    intersection_stems = queue_stems.intersection(caption_stems)
    print(f"Found {len(intersection_stems)} videos in the intersection to process.")

    # 4. Apply the new configuration limit
    MAX_TO_PROCESS = yt_config.KAGGLE_JOB_INFO_COLLECTION_URLS_TO_PROCESS
    limited_stems = list(intersection_stems)[:MAX_TO_PROCESS]

    # 5. Get the corresponding file paths for the limited list
    video_ids = limited_stems
    file_paths = [queue_file_paths[stem] for stem in limited_stems]

    print(f"Selected {len(video_ids)} videos for this job (Max: {MAX_TO_PROCESS}).")
    return file_paths, video_ids

def move_files_to_next_stage(file_paths: list[Path]):
    """
    Moves the list of files from the caption processing queue to the info collection processing queue.
    """
    source_queue_root = Path(yt_config.processing_caption_video_url_queue).resolve()
    destination_queue_root = Path(yt_config.processing_info_collection_video_url_queue).resolve() # NEW DESTINATION
    
    # Ensure the destination directory exists
    destination_queue_root.mkdir(parents=True, exist_ok=True)
    
    print(f"Moving {len(file_paths)} files from caption processing to info collection queue: {destination_queue_root}")

    for file_path in file_paths:
        try:
            # Calculate the relative path from the source queue root
            relative_path = file_path.relative_to(source_queue_root)
            # Calculate the new path in the destination queue root
            destination_path = destination_queue_root / relative_path
            
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
    print("🎬 Starting dynamic video info collection job generation and submission...")

    # 1. Prepare dynamic substitution data
    file_paths, video_ids = get_queue_files_and_video_ids()

    if not video_ids:
        abort_message = "🛑 No new video IDs found in the intersection queue. Job submission aborted."
        print(abort_message)
        send_slack_message_wrapper(message=f"🛑 `Job Aborted`: {abort_message}")
        return None

    # Construct the dictionary for placeholder substitution
    job_substitution_data = {
        # The quota for minutes, substituted directly
        "minutes_to_use": yt_config.KAGGLE_JOB_MINUTES_QOUTA, # Using same minutes quota
        # The list of video IDs, must be dumped to a JSON string for Python substitution in the template
        "video_ids_list": json.dumps(video_ids),
    }

    print(f"✅ Loaded job parameters: minutes_to_use={job_substitution_data['minutes_to_use']}")

    # 2. Load Template
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
        # job_id = 'add'
        # just_print(
            kernel_name=final_kernel_name,
            script_path=OUTPUT_SCRIPT_FILE,
            output_folder=output_folder_posix,
            # Polling/Timeout settings (using reasonable defaults)
            timeout=3600 * 1, # 1 hours
            interval_amount=polling_freq,
            interval_unit="minute",
            kernel_kwargs=nb_options,
            # Optional: Slack channel ID for notifications
            slack_channel_id=config.crongle.slack.channel_id if hasattr(config, 'crongle') and hasattr(config.crongle, 'slack') else None,
            # Optional: Slack bot token for notifications
            slack_bot_token=config.slack.bot_token if hasattr(config, 'slack') else None,
        )

        success_message = (
            f"🎉 Info Collection Job Submitted Successfully!\n"
            f"*Kernel Name*: `{final_kernel_name}`\n"
            f"*Videos to Process*: {len(video_ids)}\n"
            f"*Output Dir*: `{output_folder_posix}`\n"
            f"*Kernel Link*: `https://www.kaggle.com/code/kolikaran/{final_kernel_name}`"
        )
        print(success_message)
        send_slack_message_wrapper(message=success_message)
        
        # 5. Move files to the next processing queue
        move_files_to_next_stage(file_paths) # CHANGED FUNCTION NAME

    except Exception as e:
        tb = traceback.format_exc()
        error_message = (
            f"❌ *Kaggle Submission Failed for Info Collection*\n"
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