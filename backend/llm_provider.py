"""
LLM Provider abstraction layer for DataIQ agents.

Supported providers:
  - anthropic  → Claude (claude-sonnet-4-6, claude-haiku-4-5, claude-opus-4-6)
  - openai     → GPT  (gpt-4o, gpt-4o-mini)
  - groq       → Llama / Mixtral via Groq Cloud  [FREE tier available]
  - gemini     → Google Gemini                   [FREE tier available]
  - ollama     → Local models via Ollama         [completely FREE, runs offline]

Free options:
  Groq:   sign up at console.groq.com — free tier gives ~14,400 req/day on Llama 3.
  Gemini: get a key at aistudio.google.com — free tier with gemini-1.5-flash.
  Ollama: install from ollama.com, run `ollama pull llama3.2` — no key needed,
          runs entirely on your machine. Set API key field to "local" or leave blank.

Usage:
    from llm_provider import get_provider
    provider = get_provider(db)          # reads config from DB
    text = await provider.complete(system="...", user="...")
"""

import json
import logging
from abc import ABC, abstractmethod
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

# ── Models ─────────────────────────────────────────────────────────────────────

PROVIDER_MODELS = {
    "anthropic": [
        {"id": "claude-sonnet-4-6",        "label": "Claude Sonnet 4 (recommended)"},
        {"id": "claude-haiku-4-5-20251001", "label": "Claude Haiku 4 (fast / low cost)"},
        {"id": "claude-opus-4-6",           "label": "Claude Opus 4 (most capable)"},
    ],
    "openai": [
        {"id": "gpt-4o",      "label": "GPT-4o (recommended)"},
        {"id": "gpt-4o-mini", "label": "GPT-4o Mini (fast / low cost)"},
    ],
    "groq": [
        {"id": "llama-3.3-70b-versatile",  "label": "Llama 3.3 70B — free tier ✓"},
        {"id": "llama-3.1-8b-instant",     "label": "Llama 3.1 8B Instant — free tier ✓ (fastest)"},
        {"id": "mixtral-8x7b-32768",       "label": "Mixtral 8x7B — free tier ✓"},
    ],
    "gemini": [
        {"id": "gemini-1.5-flash",   "label": "Gemini 1.5 Flash — free tier ✓ (recommended)"},
        {"id": "gemini-1.5-pro",     "label": "Gemini 1.5 Pro — free tier ✓ (more capable)"},
        {"id": "gemini-2.0-flash",   "label": "Gemini 2.0 Flash — free tier ✓ (latest)"},
    ],
    "ollama": [
        {"id": "llama3.2",   "label": "Llama 3.2 — local, no key needed ✓"},
        {"id": "llama3.1",   "label": "Llama 3.1 — local, no key needed ✓"},
        {"id": "mistral",    "label": "Mistral 7B — local, no key needed ✓"},
        {"id": "phi3",       "label": "Phi-3 Mini — local, fastest ✓"},
        {"id": "gemma2",     "label": "Gemma 2 — local, no key needed ✓"},
    ],
}

PROVIDER_LABELS = {
    "anthropic": "Claude (Anthropic)",
    "openai":    "OpenAI",
    "groq":      "Groq — Free tier",
    "gemini":    "Google Gemini — Free tier",
    "ollama":    "Ollama — Local / Free",
}


# ── Abstract base ──────────────────────────────────────────────────────────────

class LLMProvider(ABC):
    @abstractmethod
    async def complete(self, system: str, user: str, max_tokens: int = 1024) -> str:
        """Send a prompt and return the text response."""

    @abstractmethod
    async def test(self) -> dict:
        """Test connectivity. Returns {"ok": bool, "message": str}."""

    @property
    @abstractmethod
    def provider_name(self) -> str: ...

    @property
    @abstractmethod
    def model_name(self) -> str: ...


# ── Anthropic (Claude) ─────────────────────────────────────────────────────────

class AnthropicProvider(LLMProvider):
    API_URL = "https://api.anthropic.com/v1/messages"

    def __init__(self, api_key: str, model: str = "claude-sonnet-4-6"):
        self._api_key = api_key
        self._model   = model

    @property
    def provider_name(self): return "anthropic"

    @property
    def model_name(self): return self._model

    async def complete(self, system: str, user: str, max_tokens: int = 1024) -> str:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(
                self.API_URL,
                headers={
                    "x-api-key":         self._api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type":      "application/json",
                },
                json={
                    "model":      self._model,
                    "max_tokens": max_tokens,
                    "system":     system,
                    "messages":   [{"role": "user", "content": user}],
                },
            )
            resp.raise_for_status()
            return resp.json()["content"][0]["text"]

    async def test(self) -> dict:
        try:
            text = await self.complete(
                system="You are a helpful assistant.",
                user="Reply with exactly: DataIQ connection OK",
                max_tokens=20,
            )
            return {"ok": True, "message": text.strip()}
        except httpx.HTTPStatusError as e:
            return {"ok": False, "message": f"HTTP {e.response.status_code}: {e.response.text[:200]}"}
        except Exception as e:
            return {"ok": False, "message": str(e)}


# ── OpenAI ─────────────────────────────────────────────────────────────────────

class OpenAIProvider(LLMProvider):
    API_URL = "https://api.openai.com/v1/chat/completions"

    def __init__(self, api_key: str, model: str = "gpt-4o"):
        self._api_key = api_key
        self._model   = model

    @property
    def provider_name(self): return "openai"

    @property
    def model_name(self): return self._model

    async def complete(self, system: str, user: str, max_tokens: int = 1024) -> str:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(
                self.API_URL,
                headers={
                    "Authorization": f"Bearer {self._api_key}",
                    "Content-Type":  "application/json",
                },
                json={
                    "model":      self._model,
                    "max_tokens": max_tokens,
                    "messages": [
                        {"role": "system",  "content": system},
                        {"role": "user",    "content": user},
                    ],
                },
            )
            resp.raise_for_status()
            return resp.json()["choices"][0]["message"]["content"]

    async def test(self) -> dict:
        try:
            text = await self.complete(
                system="You are a helpful assistant.",
                user="Reply with exactly: DataIQ connection OK",
                max_tokens=20,
            )
            return {"ok": True, "message": text.strip()}
        except httpx.HTTPStatusError as e:
            return {"ok": False, "message": f"HTTP {e.response.status_code}: {e.response.text[:200]}"}
        except Exception as e:
            return {"ok": False, "message": str(e)}


# ── Groq (free tier — Llama/Mixtral via Groq Cloud) ───────────────────────────

class GroqProvider(LLMProvider):
    """OpenAI-compatible API. Free tier: ~14,400 req/day. Sign up: console.groq.com"""
    API_URL = "https://api.groq.com/openai/v1/chat/completions"

    def __init__(self, api_key: str, model: str = "llama-3.3-70b-versatile"):
        self._api_key = api_key
        self._model   = model

    @property
    def provider_name(self): return "groq"

    @property
    def model_name(self): return self._model

    async def complete(self, system: str, user: str, max_tokens: int = 1024) -> str:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(
                self.API_URL,
                headers={"Authorization": f"Bearer {self._api_key}", "Content-Type": "application/json"},
                json={"model": self._model, "max_tokens": max_tokens,
                      "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}]},
            )
            resp.raise_for_status()
            return resp.json()["choices"][0]["message"]["content"]

    async def test(self) -> dict:
        try:
            text = await self.complete(system="You are a helpful assistant.",
                                       user="Reply with exactly: DataIQ connection OK", max_tokens=20)
            return {"ok": True, "message": text.strip()}
        except httpx.HTTPStatusError as e:
            return {"ok": False, "message": f"HTTP {e.response.status_code}: {e.response.text[:200]}"}
        except Exception as e:
            return {"ok": False, "message": str(e)}


# ── Google Gemini (free tier) ──────────────────────────────────────────────────

class GeminiProvider(LLMProvider):
    """Free tier available. Get a key at aistudio.google.com"""
    API_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"

    def __init__(self, api_key: str, model: str = "gemini-1.5-flash"):
        self._api_key = api_key
        self._model   = model

    @property
    def provider_name(self): return "gemini"

    @property
    def model_name(self): return self._model

    async def complete(self, system: str, user: str, max_tokens: int = 1024) -> str:
        url = self.API_URL.format(model=self._model) + f"?key={self._api_key}"
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(url, json={
                "system_instruction": {"parts": [{"text": system}]},
                "contents": [{"parts": [{"text": user}]}],
                "generationConfig": {"maxOutputTokens": max_tokens},
            })
            resp.raise_for_status()
            return resp.json()["candidates"][0]["content"]["parts"][0]["text"]

    async def test(self) -> dict:
        try:
            text = await self.complete(system="You are a helpful assistant.",
                                       user="Reply with exactly: DataIQ connection OK", max_tokens=20)
            return {"ok": True, "message": text.strip()}
        except httpx.HTTPStatusError as e:
            return {"ok": False, "message": f"HTTP {e.response.status_code}: {e.response.text[:200]}"}
        except Exception as e:
            return {"ok": False, "message": str(e)}


# ── Ollama (local, completely free, no API key) ────────────────────────────────

class OllamaProvider(LLMProvider):
    """Runs entirely on your machine. Install: ollama.com — no API key required.
    Default URL: http://localhost:11434. No key needed — set api_key to 'local' or leave blank."""

    def __init__(self, api_key: str = "local", model: str = "llama3.2",
                 base_url: str = "http://localhost:11434"):
        self._model    = model
        self._base_url = base_url.rstrip("/")

    @property
    def provider_name(self): return "ollama"

    @property
    def model_name(self): return self._model

    async def complete(self, system: str, user: str, max_tokens: int = 1024) -> str:
        async with httpx.AsyncClient(timeout=120.0) as client:
            resp = await client.post(
                f"{self._base_url}/api/chat",
                json={"model": self._model, "stream": False, "options": {"num_predict": max_tokens},
                      "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}]},
            )
            resp.raise_for_status()
            return resp.json()["message"]["content"]

    async def test(self) -> dict:
        try:
            # First check if Ollama is running
            async with httpx.AsyncClient(timeout=5.0) as client:
                tags = await client.get(f"{self._base_url}/api/tags")
                available = [m["name"].split(":")[0] for m in tags.json().get("models", [])]
                if self._model not in available and not any(self._model in m for m in available):
                    return {"ok": False, "message": f"Model '{self._model}' not found. Run: ollama pull {self._model}"}
            text = await self.complete(system="You are a helpful assistant.",
                                       user="Reply with exactly: DataIQ connection OK", max_tokens=20)
            return {"ok": True, "message": f"{text.strip()} (local)"}
        except httpx.ConnectError:
            return {"ok": False, "message": "Ollama not running — start it with: ollama serve"}
        except Exception as e:
            return {"ok": False, "message": str(e)}


# ── No-op fallback (no API key configured) ─────────────────────────────────────

class NoOpProvider(LLMProvider):
    @property
    def provider_name(self): return "none"

    @property
    def model_name(self): return "none"

    async def complete(self, system: str, user: str, max_tokens: int = 1024) -> str:
        raise RuntimeError("No LLM configured — add an API key in Settings → AI Configuration")

    async def test(self) -> dict:
        return {"ok": False, "message": "No LLM provider configured"}


# ── Factory ────────────────────────────────────────────────────────────────────

def build_provider(provider: str, model: str, api_key: str) -> LLMProvider:
    if provider == "anthropic":
        return AnthropicProvider(api_key=api_key, model=model)
    elif provider == "openai":
        return OpenAIProvider(api_key=api_key, model=model)
    elif provider == "groq":
        return GroqProvider(api_key=api_key, model=model)
    elif provider == "gemini":
        return GeminiProvider(api_key=api_key, model=model)
    elif provider == "ollama":
        return OllamaProvider(api_key=api_key, model=model)
    raise ValueError(f"Unknown provider: {provider}")


def get_provider(db) -> LLMProvider:
    """
    Load the active LLM config from the database and return the provider.
    Falls back to NoOpProvider if no config exists.
    """
    try:
        from db.models import LLMConfig
        from services.connection_service import _decrypt

        cfg = db.query(LLMConfig).filter(LLMConfig.is_active == True).order_by(
            LLMConfig.updated_at.desc()
        ).first()

        if not cfg:
            return NoOpProvider()

        api_key = _decrypt(cfg.encrypted_api_key)
        # _decrypt returns a dict (connection config format); for LLM keys we store {"key": "..."}
        if isinstance(api_key, dict):
            api_key = api_key.get("key", "")

        return build_provider(cfg.provider, cfg.model, api_key)
    except Exception as e:
        logger.warning(f"Could not load LLM config: {e}")
        return NoOpProvider()
