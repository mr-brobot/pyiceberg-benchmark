from aws_cdk import App

from benchmark.stack import Benchmark
from benchmark.env import cloudbend_env

app = App()

Benchmark(app, "PyIcebergBenchmark", env=cloudbend_env)

app.synth()
