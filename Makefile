.PHONY: all
all: clean build develop

.PHONY: venv
venv: requirements-dev.txt tox.ini
	tox -e venv
	. venv/bin/activate

.PHONY: frontend-build
frontend-build:
	venv/bin/jlpm build
	venv/bin/jupyter labextension install .
	./node_modules/.bin/babel js/ -d lib/ --verbose
	./node_modules/.bin/flow-copy-source js lib

.PHONY: build
build: venv frontend-build
	ipython profile create --ipython-dir=.ipython
	sed -i '/c.InteractiveShellApp.extensions.append/d' .ipython/profile_default/ipython_config.py
	echo "c.InteractiveShellApp.extensions.append('sparkmonitor.kernelextension')" >>  .ipython/profile_default/ipython_config.py
	venv/bin/pip install -I .
	venv/bin/jupyter labextension enable jupyterlab_sparkmonitor
	venv/bin/jupyter serverextension enable --py sparkmonitor

develop:
	IPYTHONDIR=.ipython venv/bin/jupyter lab --watch

.PHONY: clean
clean:
	rm -rf venv
	rm -rf node_modules
	rm -rf .ipython
	rm -rf lib

.PHONY: lint
lint: venv frontend-build
	tox -e lint
	./node_modules/.bin/eslint js/*.js --fix

itest:
	docker build --tag itsjafer/sparkmonitor:itest .
	docker run --rm -p 8888:8888 -p 4040:4040 -e JUPYTER_ENABLE_LAB=yes itsjafer/sparkmonitor:itest

publish:
	./node_modules/.bin/webpack --mode=production
	npm publish
