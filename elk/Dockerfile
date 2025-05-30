FROM python:3.10-slim

WORKDIR /elk

COPY . /elk/.
RUN pip install --no-cache-dir -r elk_requirements.txt

CMD ["python", "src/transform.py"]
