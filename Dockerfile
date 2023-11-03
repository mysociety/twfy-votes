FROM python:3.10-bullseye

ENV DEBIAN_FRONTEND noninteractive
COPY pyproject.toml poetry.loc[k] /
RUN curl -sSL https://install.python-poetry.org | python - && \
    echo 'export PATH="/root/.local/bin:$PATH"' > ~/.bashrc && \
    export PATH="/root/.local/bin:$PATH"  && \
    poetry config virtualenvs.create false && \
    poetry self add poetry-bumpversion && \
    poetry install && \
    echo "/workspaces/twfy-votes/src/" > /usr/local/lib/python3.10/site-packages/twfy_votes.pth

COPY . /workspaces/twfy-votes
WORKDIR /workspaces/twfy-votes
ENV SERVER_PRODUCTION=true
ENV PORT=8080
ENV PATH="/root/.local/bin:${PATH}"
CMD ["python", "-m", "twfy_votes" ,"run-server", "--live"]