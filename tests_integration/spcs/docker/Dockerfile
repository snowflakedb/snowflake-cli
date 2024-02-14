FROM python:3.10-slim-buster
COPY echo_service.py ./
RUN pip install --upgrade pip && \
    pip install flask
CMD ["python3", "echo_service.py"]
