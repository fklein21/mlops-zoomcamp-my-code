FROM agrigorev/zoomcamp-model:mlops-3.9.7-slim

WORKDIR /app

RUN pip install -U pip
RUN pip install pipenv

COPY [ "Pipfile", "Pipfile.lock", "./" ]

RUN pipenv install --system --deploy

COPY "starter.py" "./"

CMD [ "python", "starter.py", "2021", "4" ]