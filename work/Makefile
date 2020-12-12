OUTDIR = ../output
INDIR = ../input


install: re
	@sh bootstrap_jupyter.sh


etl:
	@python3 ./notebook/etl.py

re:
	@if [ -d "${OUTDIR}" ]; then \
		rm -r ${OUTDIR}; \
	fi; \

	@if [ -d "${INDIR}" ]; then \
		rm -r ${INDIR}; \
	fi \

	@mkdir ${OUTDIR}
	@mkdir ${INDIR}

clean:
	@if [ -d "${OUTDIR}" ]; then \
		rm -r ${OUTDIR}; \
	fi; \

	@mkdir ${OUTDIR}
	@python3 ./notebook/etl_clean.py



	