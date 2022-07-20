FROM public.ecr.aws/lambda/python:3.9.2022.07.19.14

RUN pip install -U pip
RUN pip install pipenv

COPY [ "Pipfile", "Pipfile.lock", "./" ]

RUN pipenv install --system --deploy

COPY [ "lambda_function.py", "./" ]

CMD [ "lambda_function.lambda_handler" ]