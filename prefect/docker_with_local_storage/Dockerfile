FROM prefecthq/prefect:0.14.14-python3.7
ENV PYTHONPATH /app:$PYTHONPATH

RUN apt-get update --fix-missing && \
    apt-get install -y wget bzip2 ca-certificates curl git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

ENV CONDA_DIR $HOME/miniconda3

RUN wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-py38_4.8.3-Linux-x86_64.sh -O ~/miniconda.sh && \
    chmod +x ~/miniconda.sh && \
    ~/miniconda.sh -b -p $CONDA_DIR && \
    rm ~/miniconda.sh
ENV PATH ${CONDA_DIR}/bin:$PATH
ADD environment.yml .

RUN conda env create -f environment.yml && \
    rm -rf $CONDA_DIR/pkgs/* && \
    conda init && \
    echo "conda activate myEnv" >> /root/.bashrc
ENV PATH ${CONDA_DIR}/envs/melqart/bin:$PATH

ADD . .

RUN pip install -e .