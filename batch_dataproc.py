#!/usr/bin/env python3

"""
Test submitting a Dataproc job from Batch

Make sure these variables are set if running outside of the analysis runner:
export DATASET=fewgenomes
export DATASET_GCP_PROJECT=fewgenomes
export ACCESS_LEVEL=test
export OUTPUT=test
export HAIL_BUCKET=cpg-fewgenomes-test-tmp/hail
export HAIL_BILLING_PROJECT=$DATASET

And activate the service account:
gcloud auth activate-service-account \
    fewgenomes-test-126@hail-295901.iam.gserviceaccount.com \
    --key-file ~/gcloud-keys/fewgenomes-test-126-hail.json
"""

import os
import hailtop.batch as hb
from analysis_runner import dataproc
from package import utils


def main():  # pylint: disable=missing-function-docstring
    billing_project = os.environ['HAIL_BILLING_PROJECT']
    hail_bucket = 'playground-au/tmp/hail'
    backend = hb.ServiceBackend(
        bucket=hail_bucket.replace('gs://', ''),
        billing_project=billing_project,
    )

    b = hb.Batch(
        'test',
        backend=backend,
    )

    utils.say_hello('batch dataproc.py')

    # Scripts path to pass to dataproc when submitting scripts
    scripts_dir = 'scripts'
    package_dir = 'package'

    dataproc.hail_dataproc_job(
        b,
        f'{scripts_dir}/myscript.py',
        pyfiles=[f'{package_dir}/{fp}' for fp in os.listdir(package_dir)],
        job_name='test myscript',
        max_age='10m',
    )

    b.run(wait=False)


if __name__ == '__main__':
    main()  # pylint: disable=E1120
