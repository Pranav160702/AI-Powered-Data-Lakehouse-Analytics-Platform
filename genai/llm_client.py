"""LLM provider abstractions for the analytics assistant."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from groq import Groq

from config.settings import get_settings


class LLMClient(Protocol):
    """Protocol implemented by analytics LLM providers."""

    def complete(self, prompt: str) -> str:
        """Return a text completion for a prompt."""


@dataclass
class GroqLLMClient:
    """Groq-backed LLM client."""

    api_key: str
    model: str

    @classmethod
    def from_settings(cls) -> "GroqLLMClient":
        """Create a Groq client from environment settings."""

        settings = get_settings()
        if not settings.groq_api_key:
            raise ValueError("GROQ_API_KEY is required for the GenAI analytics assistant.")
        return cls(api_key=settings.groq_api_key, model=settings.groq_model)

    def complete(self, prompt: str) -> str:
        """Call Groq chat completions and return the response text."""

        client = Groq(api_key=self.api_key)
        response = client.chat.completions.create(
            model=self.model,
            messages=[
                {
                    "role": "system",
                    "content": "You generate safe PostgreSQL SELECT queries for analytics.",
                },
                {"role": "user", "content": prompt},
            ],
            temperature=0.0,
            response_format={"type": "json_object"},
        )
        return response.choices[0].message.content or "{}"
