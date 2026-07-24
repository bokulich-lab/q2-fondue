FROM mambaorg/micromamba:2.4.0 AS base

USER root

ARG ENVIRONMENT
ARG PLUGIN_NAME

ENV PLUGIN_NAME=$PLUGIN_NAME \
    ENV_NAME=$PLUGIN_NAME
ENV PATH=/opt/conda/envs/${PLUGIN_NAME}/bin:$PATH \
    LC_ALL=C.UTF-8 LANG=C.UTF-8 \
    MPLBACKEND=agg \
    UNIFRAC_USE_GPU=N \
    HOME=/home/qiime2 \
    XDG_CONFIG_HOME=/home/qiime2

WORKDIR /home/qiime2
COPY environment.yml .

RUN apt-get update && apt-get install -y --no-install-recommends wget procps make \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN micromamba create --yes -n ${PLUGIN_NAME} --file environment.yml \
    && micromamba clean --all --yes \
    && chmod -R a+rwx /opt/conda

COPY . ./plugin
RUN micromamba run -n ${PLUGIN_NAME} pip install ./plugin

ENV CONDA_PREFIX=/opt/conda/envs/${PLUGIN_NAME}/
RUN micromamba run -n ${PLUGIN_NAME} qiime dev refresh-cache
RUN echo 'eval "$(micromamba shell hook --shell bash)"' >> $HOME/.bashrc \
    && echo "micromamba activate ${PLUGIN_NAME}" >> $HOME/.bashrc
RUN echo "source tab-qiime" >> $HOME/.bashrc


FROM base AS test

LABEL quay.expires-after=4w

RUN micromamba run -n ${PLUGIN_NAME} pip install pytest pytest-cov coverage parameterized pytest-xdist
CMD micromamba run -n ${PLUGIN_NAME} make -f ./plugin/Makefile test-cov
RUN chmod -R a+rwx /home/qiime2

FROM base AS prod

# Important: let any UID modify these directories so that
# `docker run -u UID:GID` works
RUN rm -rf ./plugin
RUN chmod -R a+rwx /home/qiime2
