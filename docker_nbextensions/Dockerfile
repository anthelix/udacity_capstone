FROM        jupyter/all-spark-notebook:latest
CMD echo *********1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111
USER        root

RUN 	    python -m pip install --upgrade pip

RUN         pip install jupyter_contrib_nbextensions \
                && jupyter contrib nbextension install --system \
                && pip install jupyter_nbextensions_configurator \
                && jupyter nbextensions_configurator enable --system \
                && pip install yapf # for code pretty
CMD echo ********************222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222
RUN 	   python -m pip install jupyterthemes
RUN 	   python -m pip install --upgrade jupyterthemes

CMD echo ************************************3333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333

USER        $NB_UID
