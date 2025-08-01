FROM python:3.11.11-alpine3.21 AS venv-image
WORKDIR /usr/src/app

ENV POETRY_VERSION="2.1.1"
RUN apk add --no-cache \
    file \
    make \
    build-base \
    curl \
    gcc \
    git \
    musl-dev \
    libffi-dev \
    python3-dev \
    openssl-dev \
  && curl -sSL https://install.python-poetry.org | python - \
  && ln -s "$HOME/.local/bin/poetry" "/usr/local/bin"

COPY pyproject.toml poetry.lock README.md ./
COPY swpt_stomp/ swpt_stomp/

RUN poetry config virtualenvs.create false --local \
  && python -m venv /opt/venv \
  && source /opt/venv/bin/activate \
  && poetry install --only main --no-interaction


# This is the final app image. Starting from a clean alpine image, it
# copies over the previously created virtual environment.
FROM python:3.11.11-alpine3.21 AS app-image
ARG APP_NAME=swpt_stomp

ENV APP_NAME=$APP_NAME
ENV APP_ROOT_DIR=/usr/src/app
ENV PYTHONPATH="$APP_ROOT_DIR"
ENV PATH="/opt/venv/bin:$PATH"
ENV APP_LOG_LEVEL=warning

RUN apk add --no-cache \
    libffi \
    && addgroup -S "$APP_NAME" \
    && adduser -S -D -h "$APP_ROOT_DIR" "$APP_NAME" "$APP_NAME"

COPY --from=venv-image /opt/venv /opt/venv

WORKDIR /usr/src/app

COPY $APP_NAME/ $APP_NAME/
COPY docker/entrypoint.sh \
     docker/rmq_connect.py \
     pytest.ini \
     ./
RUN python -m compileall -x '^\./(migrations|tests)/' . \
    && rm -f .env \
    && chown -R "$APP_NAME:$APP_NAME" .

USER $APP_NAME:$APP_NAME
ENTRYPOINT ["/usr/src/app/entrypoint.sh"]
CMD ["swpt-server"]
