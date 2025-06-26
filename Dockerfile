FROM continuumio/miniconda3:latest AS base

ARG ENVIRONMENT
ARG PLUGIN_NAME

ENV PLUGIN_NAME=$PLUGIN_NAME
ENV PATH=/opt/conda/envs/${PLUGIN_NAME}/bin:$PATH \
    LC_ALL=C.UTF-8 LANG=C.UTF-8 \
    MPLBACKEND=agg \
    UNIFRAC_USE_GPU=N \
    HOME=/home/qiime2 \
    XDG_CONFIG_HOME=/home/qiime2

WORKDIR /home/qiime2
COPY environment.yml .
COPY install-sra-tools.sh .

RUN apt-get update \
    && apt-get install -y --no-install-recommends wget curl procps make \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN conda update -qy conda \
    && conda install -c conda-forge -qy mamba \
    && mamba env create -n ${PLUGIN_NAME} --file environment.yml \
    && mamba run -n ${PLUGIN_NAME} bash install-sra-tools.sh \
    && mamba clean --all --yes \
    && chmod -R a+rwx /opt/conda

RUN mkdir .ncbi
RUN printf '/LIBS/GUID = "%s"\n' `uuidgen` > .ncbi/user-settings.mkfg

COPY . ./plugin
RUN mamba run -n ${PLUGIN_NAME} pip install ./plugin

RUN /bin/bash -c "source activate ${PLUGIN_NAME}"
ENV CONDA_PREFIX=/opt/conda/envs/${PLUGIN_NAME}/
RUN mamba run -n ${PLUGIN_NAME} qiime dev refresh-cache
RUN echo "source activate ${PLUGIN_NAME}" >> $HOME/.bashrc
RUN echo "source tab-qiime" >> $HOME/.bashrc


FROM base AS test

RUN mamba run -n ${PLUGIN_NAME} pip install pytest pytest-cov coverage parameterized pytest-xdist
CMD mamba run -n ${PLUGIN_NAME} make -f ./plugin/Makefile test-cov

FROM base AS prod

# Important: let any UID modify these directories so that
# `docker run -u UID:GID` works
RUN rm -rf ./plugin
RUN chmod -R a+rwx /home/qiime2