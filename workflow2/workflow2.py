'''
number.txt --> increment --> result.txt
'''

from Pegasus.api import *
from pathlib import Path
import logging
logging.basicConfig(level=logging.INFO)

rc = ReplicaCatalog()
rc.add_replica(
    site = "local",
    lfn = "number.txt",
    pfn = Path(__file__).parent.resolve() / "number.txt"
)
rc.write()

tc = TransformationCatalog()
increment = Transformation(
    "increment.py",
    site = "local",
    pfn = Path(__file__).parent.resolve() / "bin/increment.py",
    is_stageable = True
)
tc.add_transformations(increment)
tc.write()

wf = Workflow("workflow-2")

input_file = File("number.txt")
output_file = File("result.txt")

increment_job = Job(increment)\
    .add_args("number.txt", "result.txt")\
    .add_input(input_file)\
    .add_outputs(output_file)
wf.add_jobs(increment_job)

wf.plan(submit=True)\
    .wait()\
    .statistics()
