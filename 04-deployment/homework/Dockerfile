FROM agrigorev/zoomcamp-model:mlops-2024-3.10.13-slim

# do stuff here

# RUN pip install -U pip 

# RUN pip install pipenv


# COPY [ "Pipfile", "Pipfile.lock", "./" ]

# RUN pipenv install --system --deploy


# COPY [ "starter_cli.py", "./" ]

# # ENTRYPOINT [ "executable" ]


WORKDIR /app

COPY predict_duration.py .

RUN pip install pandas pyarrow scikit-learn

ENTRYPOINT ["python", "predict_duration.py"]