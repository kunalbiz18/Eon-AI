import os
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Dict, List

from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from sse_starlette.sse import EventSourceResponse

try:
    from agno.agent import Agent
    from agno.models.openai import OpenAIChat
    from agno.tools.duckduckgo import DuckDuckGoTools
except Exception as e:  # pragma: no cover
    raise RuntimeError("Agno dependencies not installed. Ensure 'agno' is in dependencies.") from e


load_dotenv()


class Message(BaseModel):
    role: str
    content: str


class ChatBody(BaseModel):
    messages: List[Message] = Field(default_factory=list)
    stream: bool = True
    metadata: Dict[str, Any] | None = None


class Settings(BaseModel):
    openai_api_key: str | None = os.getenv("OPENAI_API_KEY")
    model: str = os.getenv("AGNO_MODEL", "gpt-4o-mini")


def create_research_agent(settings: Settings) -> Agent:
    return Agent(
        model=OpenAIChat(id=settings.model),
        tools=[DuckDuckGoTools()],
        instructions=[
            "You are Researcher. Search, validate facts, and provide citations.",
        ],
        markdown=True,
    )


def create_writer_agent(settings: Settings) -> Agent:
    return Agent(
        model=OpenAIChat(id=settings.model),
        instructions=[
            "You are Writer. Compose clear, concise answers using inputs.",
        ],
        markdown=True,
    )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    # Delay agent initialization until first request so health works without keys
    app.state.settings = Settings()
    app.state.team = None
    yield


app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _format_history(messages: List[Message]) -> List[str]:
    history: List[str] = []
    for m in messages:
        prefix = "User" if m.role == "user" else ("Assistant" if m.role == "assistant" else m.role)
        history.append(f"{prefix}: {m.content}")
    return history


@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.post("/chat")
async def chat(body: ChatBody, settings: Settings = Depends(lambda: Settings())) -> EventSourceResponse:
    if not settings.openai_api_key:
        raise HTTPException(status_code=400, detail="OPENAI_API_KEY missing")

    # Orchestration: research on user query, then writer composes final
    user_text = "\n".join(_format_history(body.messages))

    async def event_generator() -> AsyncGenerator[dict, None]:
        # Lazy initialize team
        if app.state.team is None:
            researcher = create_research_agent(settings)
            writer = create_writer_agent(settings)
            app.state.team = {"researcher": researcher, "writer": writer}
        # Step 1: Researcher
        yield {"event": "phase", "data": "research"}
        research_stream = app.state.team["researcher"].run(user_text, stream=True)
        research_accum = ""
        for chunk in research_stream:
            text = str(chunk)
            research_accum += text
            yield {"event": "token", "data": text}

        # Step 2: Writer uses research
        yield {"event": "phase", "data": "write"}
        prompt = (
            "Using the research below, write the final answer.\n" 
            "Research:\n" + research_accum + "\n\n" 
            "Now respond succinctly."
        )
        writer_stream = app.state.team["writer"].run(prompt, stream=True)
        for chunk in writer_stream:
            yield {"event": "token", "data": str(chunk)}
        yield {"event": "done", "data": ""}

    return EventSourceResponse(event_generator())


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=True)

