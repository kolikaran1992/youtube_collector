import json
import uuid
import sys
from pathlib import Path
import traceback
from crongle import KernelLauncher
from omniconf import config, logger
from slack_messenger import send_slack_message as send_slack_message_base
from yt_collector.job_queue import JobQueue # New import for job queue logic

# --- CONFIGURATION RETRIEVAL AND JOB CONSTANTS ---
# Assuming 'ytcc' environment provides all necessary configuration keys
yt_config = config

# Define the paths and parameters for this specific job
TEMPLATE_FILE = yt_config.KAGGLE_JOB_YT_INFO_COLLECTION_TEMPLATE_FILE
OUTPUT_SCRIPT_FILE = Path('yt_collector', 'yt_info_collection_job_executable.py').as_posix()
JOB_NAME = 'yt-info-collector'
BASE_OUTPUT_DIR = yt_config.kaggle_output_dir_yt_info # Target output dir for info

# Initialize the Job Queues
try:
    # Source queue for this script: jobs that need info fetched
    SOURCE_QUEUE = JobQueue(yt_config.yt_info_fetching_queue_dir)
    # Destination queue for this script: resting queue
    DESTINATION_QUEUE = JobQueue(yt_config.resting_queue_dir)
    MAX_URLS_TO_PROCESS = yt_config.KAGGLE_JOB_INFO_COLLECTION_URLS_TO_PROCESS
except AttributeError as e:
    logger.exception(f'Error: Required configuration value not found in config: {e}')
    sys.exit(1)


# --- HELPER FUNCTIONS ---
def send_slack_message_wrapper(message):
    send_slack_message_base(message=message, header=f'*YT-COLLECTOR-{uuid.uuid1().hex[:8]}: `{Path(__file__).name}`*')

def substitute_placeholders(template_content: str, config_data: dict) -> str:
    '''
    Replaces {{KEY}} placeholders in the template with values from the config_data.
    '''
    content = template_content
    for key, value in config_data.items():
        placeholder = '{{' + key + '}}'
        # For lists (like video_ids_list), the calling code must ensure the value
        # is a properly formatted JSON string so it pastes correctly into the Python template.
        content = content.replace(placeholder, str(value))
    return content


# --- MAIN EXECUTION ---
def run_job():
    '''
    Pops jobs from the info collection queue, generates the executable script, 
    submits the job to Kaggle, and pushes the jobs to the resting queue.
    '''
    logger.info('🎬 Starting dynamic info collection job generation and submission...')
    
    video_ids = []
    video_job_data = {} # To hold the original job data for pushing to the next queue

    # 1. Ingest jobs from INFO_COLLECTION_QUEUE (Source)
    logger.info(f'Scanning queue directory for up to {MAX_URLS_TO_PROCESS} video IDs...')
    for _ in range(MAX_URLS_TO_PROCESS):
        job = SOURCE_QUEUE.pop()
        if job is None:
            break
        video_id, job_data = job
        video_ids.append(video_id)
        video_job_data[video_id] = job_data
    
    videos_to_process_count = len(video_ids)

    if videos_to_process_count == 0:
        abort_message = '🛑 No new video IDs found in the queue. Job submission aborted.'
        logger.info(abort_message)
        send_slack_message_wrapper(message=f'🛑 `Job Aborted`: {abort_message}')
        return None

    # Construct the dictionary for placeholder substitution
    job_substitution_data = {
        'minutes_to_use': yt_config.KAGGLE_JOB_MINUTES_QOUTA,
        # The list of video IDs, must be dumped to a JSON string for Python substitution in the template
        'video_ids_list': json.dumps(video_ids),
    }

    logger.info(f'✅ Loaded job parameters: Processing {videos_to_process_count} videos.')

    # 2. Load Template and Cookies
    try:
        with open(TEMPLATE_FILE, 'r') as f:
            template_content = f.read()
    except FileNotFoundError:
        error_message = f'❌ Error: Template file not found at {TEMPLATE_FILE}.'
        logger.exception(error_message)
        send_slack_message_wrapper(message=error_message)
        # Note: If template is missing, the jobs are lost from the queue. This is accepted risk.
        return None

    # 3. Substitute and Generate Final Python Script
    final_script_content = substitute_placeholders(template_content, job_substitution_data)

    # Ensure the output directory for the script exists
    Path(OUTPUT_SCRIPT_FILE).parent.mkdir(parents=True, exist_ok=True)

    with open(OUTPUT_SCRIPT_FILE, 'w') as f:
        f.write(final_script_content)

    logger.info(
        f'✅ Generated executable script: {OUTPUT_SCRIPT_FILE}. Variables substituted.'
    )

    # 4. Programmatic Crongle Submission (Post-processing logic)
    logger.info('🚀 Submitting job using Crongle KernelLauncher...')

    run_uuid = str(uuid.uuid4())[:8]
    final_kernel_name = f'crongle-job-{JOB_NAME}-{run_uuid}'

    output_path = Path(BASE_OUTPUT_DIR).joinpath(final_kernel_name).resolve()
    output_path.mkdir(exist_ok=True, parents=True)
    output_folder_posix = output_path.as_posix()

    kernel_type = 'cpu'
    polling_freq = 15

    nb_options = {
        'title': final_kernel_name,
        'is_private': True,
        'enable_gpu': kernel_type == 'gpu',
        'enable_internet': True,
    }

    try:
        job_id = KernelLauncher().submit_job(
            kernel_name=final_kernel_name,
            script_path=OUTPUT_SCRIPT_FILE,
            output_folder=output_folder_posix,
            timeout=3600 * 1, # 1 hours
            interval_amount=polling_freq,
            interval_unit='minute',
            kernel_kwargs=nb_options,
            slack_channel_id=config.crongle.slack.channel_id if hasattr(config, 'crongle') and hasattr(config.crongle, 'slack') else None,
            slack_bot_token=config.slack.bot_token if hasattr(config, 'slack') else None,
        )

        success_message = (f'🎉 Job Submitted Successfully!\n'
            f'*Kernel Name*: `{final_kernel_name}`\n'
            f'*Videos to Process*: {videos_to_process_count}\n'
            f'*Output Dir*: `{output_folder_posix}`\n'
            f'*Kernel Link*: `https://www.kaggle.com/code/kolikaran/{final_kernel_name}`'
        )
        logger.info(success_message)
        send_slack_message_wrapper(message=success_message)
        
        # 5. Push files to the next queue (Destination: resting_queue_dir)
        processed_count = 0
        kaggle_job_data_to_add = {
            'kernel_name': final_kernel_name,
            'kaggle_kernel_link': f'https://www.kaggle.com/code/kolikaran/{final_kernel_name}',
            'output_dir': output_folder_posix,
            'video_count': videos_to_process_count,
        }
        for video_id, job_data in video_job_data.items():
            # Add the Kaggle job data under a specific key
            job_data['kaggle_job_yt_info_collection'] = kaggle_job_data_to_add # Updated key
            DESTINATION_QUEUE.push(video_id, job_data)
            processed_count += 1
        logger.info(f'✅ Successfully pushed {processed_count} jobs to the resting queue ({DESTINATION_QUEUE.queue_path.name}).')

    except Exception as e:
        tb = traceback.format_exc()
        error_message = (
            f'❌ *Kaggle Submission Failed*\n'
            f'*Error*: {e}\n'
            f'*Attempted Kernel Name*: `{final_kernel_name}`\n'
            f'*Traceback*:\n```\n{tb}\n```'
        )
        logger.exception(f'❌ Crongle Submission Failed: {e}')
        send_slack_message_wrapper(message=error_message)
        
        # --- START: Submit Back Logic (Requeuing) ---
        requeued_count = 0
        for video_id, job_data in video_job_data.items():
            # Remove the job tracking data added previously (in case of re-queue to source)
            if 'kaggle_job_yt_info_collection' in job_data: # Updated key
                del job_data['kaggle_job_yt_info_collection']
                
            SOURCE_QUEUE.push(video_id, job_data)
            requeued_count += 1
        
        requeue_message = f"↩️ Successfully requeued {requeued_count} jobs to the INFO_COLLECTION_QUEUE after submission failure."
        logger.info(requeue_message)
        send_slack_message_wrapper(message=requeue_message)
        # --- END: Submit Back Logic ---
        
        return None        

    return job_id


if __name__ == '__main__':
    run_job()