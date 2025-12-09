# Junior Data Engineer Assignment – Card Transactions ETL

## 1. Overview

This project implements a small ETL workflow for processing daily card transaction CSV files stored in AWS S3.

### Workflow Summary
- A **Lambda function** (Python 3.11, container image) reads CSV files from:  
  `s3://landing-zone-datalake/.../card_transaction_YYYY-MM-DD.csv`
- Each record is **validated using Pydantic**.
- Valid and invalid records are written as **JSONL files** into:  
  `s3://green-zone-datalake/.../card_transaction_YYYY-MM-DD_valid.jsonl`  
  `s3://green-zone-datalake/.../card_transaction_YYYY-MM-DD_invalid.jsonl`
- An **Airflow DAG**:
  - waits for the daily file to appear on S3  
  - invokes the Lambda  
  - exposes logs in the Airflow UI  

---

## 2. Project Structure

```
colibrix_task/
├── airflow_dags/
│   └── card_transaction_lambda_dag.py
├── lambda/
│   ├── app.py
│   ├── models.py
│   └── requirements.txt
├── card_transaction_2025-11-10.csv
├── sample_output/
│   ├── card_transaction_2025-11-10_valid.jsonl
│   └── card_transaction_2025-11-10_invalid.jsonl
└── README.md
```

---

## 3. Validation Rules

Implemented in `lambda/models.py` using **Pydantic**.

Each record must satisfy:

- `id`: non-empty string  
- `organization_id`: non-empty string  
- `amount`: integer ≥ 0  
- `currency`: one of: `EUR`, `USD`, `GBP`  
- `status`: one of: `Success`, `Error`, `Pending`  
- `created_at`: ISO8601 timestamp, must be **UTC**  

Valid rows are written to the `*_valid.jsonl` file.  
Invalid rows (with errors) are written to the `*_invalid.jsonl` file.

---

## 4. Running the ETL Locally (No AWS Required)

### 4.1 Create environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install boto3 pydantic
```

### 4.2 Run local ETL

```bash
python lambda/app.py
```

This reads the CSV file and writes:

```
sample_output/card_transaction_2025-11-10_valid.jsonl
sample_output/card_transaction_2025-11-10_invalid.jsonl
```

These JSONL files are part of the assignment deliverables.

---

## 5. AWS Lambda (Container Image)

The Lambda function is packaged using a Docker container (`lambda/Dockerfile`).

### 5.1 Build the image

```bash
cd lambda
docker build -t card-transaction-lambda .
```

### 5.2 Push to AWS ECR (example)

```bash
aws ecr create-repository --repository-name card-transaction-lambda
docker tag card-transaction-lambda:latest <ACCOUNT>.dkr.ecr.<REGION>.amazonaws.com/card-transaction-lambda:latest
docker push <ACCOUNT>.dkr.ecr.<REGION>.amazonaws.com/card-transaction-lambda:latest
```

### 5.3 Deploy the Lambda

1. Go to AWS Lambda → Create function → Container image  
2. Select the image from ECR  
3. Set handler:  
   ```
   app.handler
   ```
4. Add environment variable:  
   ```
   GREEN_ZONE_BUCKET=green-zone-datalake
   ```
5. IAM permissions required:
   - Read from `landing-zone-datalake`
   - Write to `green-zone-datalake`

---

## 6. Airflow DAG

Stored in: `airflow_dags/card_transaction_lambda_dag.py`

### Behavior
- Runs daily (`@daily`)
- Constructs expected filename using execution date
- Waits for the file using `S3KeySensor`
- Invokes Lambda with `LambdaInvokeFunctionOperator`
- Logs Lambda output in Airflow

### How to use
Place the DAG inside an Airflow `dags/` folder.  
Ensure Airflow has `apache-airflow-providers-amazon` installed.

---

## 7. Ensuring New Files Are Always Processed

Expected format:
```
card_transaction_YYYY-MM-DD.csv
```

### Implemented solution
- DAG runs daily  
- Builds the exact S3 path for that date  
- `S3KeySensor` waits (up to 6 hours) for the file  
- Once present → Lambda is triggered  
- Ensures any daily file is processed automatically

### Alternative (event-driven)
Using S3 Event Notifications → SQS/EventBridge → trigger Airflow.  
(Not required for this assignment but recommended for production.)

---

## 8. Deliverables

### 1. Repository ZIP containing:
- `lambda/`  
- `airflow_dags/`  
- `sample_output/`  
- `README.md`

### 2. Final JSONL files:
- `sample_output/card_transaction_2025-11-10_valid.jsonl`  
- `sample_output/card_transaction_2025-11-10_invalid.jsonl`

---

