FROM public.ecr.aws/lambda/python:3.11

COPY app.py models.py requirements.txt ${LAMBDA_TASK_ROOT}/

RUN pip install --no-cache-dir -r ${LAMBDA_TASK_ROOT}/requirements.txt

CMD ["app.handler"]
