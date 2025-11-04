import json
from pathlib import Path
from typing import Dict, List, Tuple, Optional

from omniconf import config, logger
from yt_collector.job_queue import JobQueue
from meter_call import LLMFallbackCaller
from xml_parsing import SimpleXMLParser
from slack_messenger import send_slack_message


# --- Configuration ---
PROVIDERS = [{"model": "groq/openai/gpt-oss-120b", "api_key": config.llm_api_key.groq2}]
SYSTEM_PROMPT = """
You are an assistant that analyzes educational programming transcripts and extracts structured insights.

Your task:
Given a transcript, output a concise XML summary capturing:

- topics: The main idea or concept being taught. If there are multiple topics, repeat the topic block.
- problem_it_solves: The core problem or pain point the topic addresses.
- how_it_works: A short description of the mechanism, pattern, technique, or workflow.
- when_to_use: Cases where this pattern is helpful.
- when_not_to_use: Cases where it introduces drawbacks or should be avoided.

Formatting rules:

- Output ONLY XML.
- Keep explanations concise but do not remove important ideas or steps.
- If multiple topics exist, create multiple <topic_block> entries like:

<topic_block>
   <topic>...</topic>
   ...
</topic_block>

- Do not invent information; only use what appears in the transcript.

✅ Example of the required output format
<topic_block>
  <topic>name of the topic</topic>
  <problem_it_solves>Repeatedly passing the same parameters (user_id, db_session, logger, api_key) across many functions.</problem_it_solves>
  <how_it_works>Group shared runtime data into a single context object and pass that object instead of many parameters.</how_it_works>
  <when_to_use>High-level functionality, shared config, dependency injection, reusable runtime settings, testing/mocking.</when_to_use>
  <when_not_to_use>Low-level utility functions; when it increases coupling; when it becomes a “god object”.</when_not_to_use>
</topic_block>

"""

LLM = LLMFallbackCaller(providers=PROVIDERS)
INPUT_QUEUE = JobQueue(config.arjan_codes_queue)
OUTPUT_QUEUE = JobQueue(config.resting_queue_dir)


# --- XML & Slack Formatting Helpers ---


def convert_to_dict(xml: str) -> List[Dict]:
    """Convert the XML summary to a list of topic dictionaries."""
    topics = []
    blocks = SimpleXMLParser.extract_all_tags(xml, "topic_block")

    for block in blocks:
        data = {
            key: SimpleXMLParser.extract_tag_content(block, key)
            for key in [
                "topic",
                "problem_it_solves",
                "how_it_works",
                "when_to_use",
                "when_not_to_use",
            ]
        }
        data["examples"] = SimpleXMLParser.extract_all_tags(block, "example")
        topics.append(data)

    return topics


def format_topics_for_slack(topics: List[Dict]) -> str:
    """Return a Slack-friendly formatted string of extracted topics."""
    formatted = []
    for t in topics:
        msg = (
            f"*{t['topic']}*"
            f"\n>`Problem it solves`: {t['problem_it_solves']}"
            f"\n>`How it works`: {t['how_it_works']}"
            f"\n>`When to use`: {t['when_to_use']}"
            f"\n>`When *not* to use`: {t['when_not_to_use']}"
        )

        if t["examples"]:
            msg += "\n`Examples`:"
            for ex in t["examples"]:
                msg += f"\n>   • {ex}"
        formatted.append(msg)

    return "\n\n".join(formatted)


# --- Caption Parsing ---


def get_captions(caption_file: Path) -> str:
    """Flatten YouTube caption json3 -> raw text."""
    try:
        events = json.loads(caption_file.read_text("utf-8")).get("events", [])
    except Exception:
        logger.error(f"Caption file corrupt: {caption_file}")
        return ""

    text = []
    for e in events:
        for seg in e.get("segs", []):
            text.append(seg.get("utf8", ""))

    return "".join(text)


# --- Job Handling ---


def run_llm_analysis(video_id: str, title: str, captions: str) -> Optional[dict]:
    """Call LLM with transcript and return raw model data if successful."""
    logger.info(f"LLM analysis for {video_id}")

    try:
        result = LLM.call(
            [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": captions},
            ]
        )
        return result.model_dump()
    except Exception as e:
        logger.error(f"LLM failed for {video_id}: {e}")
        return None


def process_job(job: dict, caption_path: Path) -> None:
    video_id = job.get("video_id")
    title = job.get("title", "Untitled")
    captions = get_captions(caption_path)

    raw_output = run_llm_analysis(video_id, title, captions)
    if not raw_output:
        return

    # Extract and send results
    xml = raw_output["choices"][0]["message"]["content"]
    msg = format_topics_for_slack(convert_to_dict(xml))

    send_slack_message(
        msg,
        header=f"Arjan Codes Video Analysis: {title}",
        slack_channel_id=config.slack_channel_id_for_arjan_codes,
    )

    # Persist results
    job_data = INPUT_QUEUE.remove_from_queue(video_id)
    job_data["llm_analysis"] = raw_output
    OUTPUT_QUEUE.push(video_id, job_data)


def get_next_job() -> Tuple[Optional[dict], Optional[Path]]:
    """Return next job with a valid caption file."""
    for file in sorted(
        INPUT_QUEUE.queue_path.glob("*"), key=lambda p: p.stat().st_ctime
    ):
        if not file.is_file():
            continue

        job = json.loads(file.read_text("utf-8"))
        video_id = job.get("video_id")
        caption_info = job.get("kaggle_job_yt_captions", {})
        caption_path = Path(caption_info.get("output_dir", "")).joinpath(
            f"{video_id}.en.json3"
        )

        if caption_path.exists():
            return job, caption_path

        logger.warning(f"Missing transcript for {video_id} — skipped '{caption_path}'.")

    return None, None


# --- Main Entry ---


def main():
    logger.info("Starting LLM analysis cron job.")

    job, caption = get_next_job()
    if not job:
        logger.info("No pending jobs.")
        return

    process_job(job, caption)
    logger.info("Finished LLM analysis run.")


if __name__ == "__main__":
    main()
