# API

This directory will contain the FastAPI agent service deployed to Azure Container
Apps. Its boundary includes authentication, request validation, UI session
adaptation, streaming responses, and the runtime entry point for the LangGraph
workflow defined under `agent/`.

Reusable graph logic and governed tools belong under `agent/`. Data pipeline
execution does not belong in this package.
