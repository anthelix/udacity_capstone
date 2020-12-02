OUTDIR = ../output


install:
	@sh bootstrap_jupyter.sh

etl:
	@python3 ./notebook/etl.py

clean:
	@if [ -d "${OUTDIR}" ]; then \
		rm -r ${OUTDIR}; \
	fi \


	