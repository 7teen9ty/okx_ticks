FROM python:3.10-slim

WORKDIR /teaklisten

COPY . /teaklisten/.
RUN pip install --no-cache-dir -r teaklist_requirements.txt

CMD ["python", "src/teaklisten.py"]
