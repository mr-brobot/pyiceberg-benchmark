#!/bin/bash

python -m pip install --user --requirement <(poetry export --without-hashes)

wget https://github.com/sharkdp/hyperfine/releases/download/v1.16.1/hyperfine_1.16.1_amd64.deb
sudo dpkg -i hyperfine_1.16.1_amd64.deb
rm hyperfine_1.16.1_amd64.deb

SITE_PACKAGES=$(python -m pip show aws-glue-sessions | grep Location | awk '{print $2}') && \
  python -m jupyter kernelspec install --user $SITE_PACKAGES/aws_glue_interactive_sessions_kernel/glue_pyspark