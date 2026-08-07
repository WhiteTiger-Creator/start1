FROM public.ecr.aws/docker/library/python:3.13-slim-bookworm@sha256:01f42367a0a94ad4bc17111776fd66e3500c1d87c15bbd6055b7371d39c124fb

ENV DEBIAN_FRONTEND=noninteractive

# Least-volatile layers first: fix the working directory before the apt and
# dependency layers so a change to those never invalidates the WORKDIR layer.
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        tmux \
        asciinema \
    && rm -rf /var/lib/apt/lists/*

# /app/data/events.json ships TRUNCATED exactly as the failed rollout left it;
# the pre-rollout snapshot and the replay journal beside it are the recovery
# sources the responder has to merge back into the authoritative stream.
COPY app/ /app/
RUN cp /app/workflow/export_report.py /app/workflow/.export_report.original \
    && chmod a-w /app/workflow/.export_report.original
