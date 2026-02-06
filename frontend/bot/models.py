from dataclasses import dataclass
from enum import Enum
from typing import List, Optional

class ResponseMode(Enum):
    DIRECT = "direct"
    LLM = "llm"
    FALLBACK = "fallback"

@dataclass
class SearchResult:
    id: int
    content: str
    category: str
    faculty: str
    score: float
    keywords: List[str]
    description: Optional[str] = None # Puede ser None si no existe en la DB
