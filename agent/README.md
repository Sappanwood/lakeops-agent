# Agent

This directory will contain the LangGraph workflow, typed tool contracts, state
model, SQL policy, and operation approval transitions. The graph remains
independent of its HTTP transport and is loaded by the FastAPI runtime under
`apps/api/`.
