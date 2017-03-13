FROM python:2.7

ADD setup.py /src/
WORKDIR /src
RUN python setup.py develop && pip install nose mock

CMD ["python", "setup.py", "test"]