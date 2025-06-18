FROM python:3.10-slim

WORKDIR /strategy_rt

COPY . /strategy_rt/.
RUN pip install --no-cache-dir -r strategy_requirements.txt

CMD ["python", "src/strategy_rt.py"]
