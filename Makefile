.PHONY: help docs
.DEFAULT_GOAL := help

help:
	@fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

lint: ## Run code linters
	black --check --diff .
	isort --check .
	flake8 .

format: ## Run code formatters
	isort .
	black .
	flake8 .

#flake: ## Run code checks
#	flake8 .

lock: ## Compile all requirements files
	pip-compile --no-emit-index-url --no-header --verbose --output-file requirements.ci.txt requirements.ci.in

install: ## Install dev requirements
	pip install -r requirements.ci.txt