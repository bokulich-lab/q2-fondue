FROM mambaorg/micromamba
ARG MAMBA_DOCKERFILE_ACTIVATE=1
RUN micromamba install -y -c https://packages.qiime2.org/qiime2/2023.7/tested/ \
	-c conda-forge -c bioconda -c defaults \
	q2cli q2-fondue
ENV PATH /opt/conda/bin:$PATH

RUN mkdir $HOME/.ncbi
RUN printf '/LIBS/GUID = "%s"\n' `uuidgen` > $HOME/.ncbi/user-settings.mkfg

CMD [ "/bin/bash" ]
