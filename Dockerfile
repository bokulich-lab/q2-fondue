FROM mambaorg/micromamba
ARG MAMBA_DOCKERFILE_ACTIVATE=1
RUN micromamba install -y -c https://packages.qiime2.org/qiime2/2023.2/tested/ \
	-c conda-forge -c bioconda -c defaults \
	q2cli q2-fondue
ENV PATH /opt/conda/bin:$PATH

RUN echo $(whoami)
RUN uname=$(whoami)

USER root
RUN apt-get update && apt-get install uuid-runtime
RUN mkdir $HOME/.ncbi
RUN printf '/LIBS/GUID = "%s"\n' `uuidgen` > $HOME/.ncbi/usert-settings.mkfg

USER ${uname}
CMD [ "/bin/bash" ]
