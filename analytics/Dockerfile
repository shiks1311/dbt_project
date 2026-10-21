FROM python:3.12-slim
ENV DBT_PROFILES_DIR=/dbt \
    PYTHONUNBUFFERED=1

WORKDIR /dbt

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENTRYPOINT ["dbt"]
CMD ["build"]
