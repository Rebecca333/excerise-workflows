'''
input.txt --> echo --> stdout.txt
'''

import logging
from pathlib import Path
from Pegasus.api import *
logging.basicConfig(level=logging.INFO)

with open("input.txt", "w") as f:
    f.write("hello world")

rc = ReplicaCatalog()
rc.add_replica(
    site = "local",
    lfn = "input.txt",
    pfn = Path(__file__).parent.resolve() / "input.txt"
)
rc.write()

tc = TransformationCatalog()
echo = Transformation(
    "echo",
    site = "condorpool",
    pfn = "usr/bin/echo",
    is_stageable = False
)
tc.add_transformations(echo)
tc.write()

wf = Workflow("workflow-1")

input_file = File("input.txt")
output_file = File("stdout.txt")

echo_job = Job(echo)\
    .add_args("hello world")\
    .add_input(input_file)\
    .add_outputs(output_file)
wf.add_jobs(echo_job)

wf.plan(submit=True)\
    .wait()\
    .statistics()
