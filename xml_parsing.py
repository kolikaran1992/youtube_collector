import re
from typing import List, Optional


class SimpleXMLParser:
    """Simple regex-based XML parser with customizable patterns"""

    @staticmethod
    def extract_with_pattern(text: str, pattern: str, group: int = 1) -> Optional[str]:
        """Extract content using a custom regex pattern"""
        match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
        return match.group(group).strip() if match else None

    @staticmethod
    def extract_all_with_pattern(text: str, pattern: str, group: int = 1) -> List[str]:
        """Extract all matches using a custom regex pattern"""
        matches = re.findall(pattern, text, re.DOTALL | re.IGNORECASE)
        return [
            match.strip() if isinstance(match, str) else match[group - 1].strip()
            for match in matches
        ]

    @staticmethod
    def extract_tag_content(text: str, tag: str) -> Optional[str]:
        """Extract content from a specific XML tag"""
        pattern = rf"<{tag}>\s*(.*?)\s*</{tag}>"
        return SimpleXMLParser.extract_with_pattern(text, pattern)

    @staticmethod
    def extract_all_tags(text: str, tag: str) -> List[str]:
        """Extract all instances of a specific XML tag"""
        pattern = rf"<{tag}>\s*(.*?)\s*</{tag}>"
        return SimpleXMLParser.extract_all_with_pattern(text, pattern)
