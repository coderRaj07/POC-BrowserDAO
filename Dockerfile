FROM python:3.12-slim

RUN mkdir /sealed && chmod 777 /sealed

WORKDIR /app

COPY . /app

# Install python-gnupg
RUN pip install --no-cache-dir python-gnupg deepdiff beautifulsoup4 pyyaml jinja2

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "-m", "my_proof"]
