# Minimal makefile for Sphinx documentation
#

.PHONY: help install html

SPHINXOPTS    =
SPHINXBUILD   = sphinx-build
SPHINXPROJ    = launchdarkly-server-sdk
SOURCEDIR     = .
BUILDDIR      = build

help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

install:
	pip3 install -r requirements.txt

html: install
	@$(SPHINXBUILD) -M html "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)
