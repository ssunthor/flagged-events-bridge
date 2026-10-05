# flagged-events-bridge (HALO-881, minimal slice). See main.py's module
# docstring for scope. Non-root, single-stage -- no build step is needed,
# unlike fingerprint-rules' Dockerfile, since there is no proto codegen here.
FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt \
 && useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin app
COPY main.py ./
USER 10001
EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
