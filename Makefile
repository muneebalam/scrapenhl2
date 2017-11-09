PYTHON=python3
MODULE_NAME=`$(PYTHON) -c "import constants as c; print(c.module_name)"`
VERSION=`python setup.py --version`

setup:
	pip install -e .

clean:
	@rm -rf *.egg-info *.pyc __pycache__
	@find . -regex "\(.*__pycache__.*\|*.py[co]\)" -delete

test:
	py.test

