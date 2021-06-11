'''
                                             --> odd_nums.txt --> count --> odd_count.txt
nums1.txt, nums2.txt, nums3.txt --> separate                                                --> tar --> result.tar.gz
                                             --> even_nums.txt --> count --> even_count.txt
'''

from Pegasus.api import *
from pathlib import Path
import logging
logging.basicConfig(level=logging.INFO)

rc = ReplicaCatalog()
rc.add_replica(
    site = "local",
    lfn = "nums1.txt",
    pfn = Path(__file__).parent.resolve() / "nums1.txt"
)
rc.add_replica(
    site = "local",
    lfn = "nums2.txt",
    pfn = Path(__file__).parent.resolve() / "nums2.txt"
)
rc.add_replica(
    site = "local",
    lfn = "nums3.txt",
    pfn = Path(__file__).parent.resolve() / "nums3.txt"
)
rc.write()

tc = TransformationCatalog()
separate = Transformation(
    "separate.py",
    site = "local",
    pfn = Path(__file__).parent.resolve() / "bin/separate.py",
    is_stageable = True
)
tc.add_transformations(separate)
count = Transformation(
    "count.py",
    site = "local",
    pfn = Path(__file__).parent.resolve() / "bin/count.py",
    is_stageable = True
)
tc.add_transformations(count)
tar = Transformation(
    "tar",
    site = "condorpool",
    pfn = "usr/bin/tar",
    is_stageable = False
)
tc.add_transformations(tar)
tc.write()

wf = Workflow("workflow-3")

input_one = File("nums1.txt")
input_two = File("nums2.txt")
input_three = File("nums3.txt")
output_odd = File("odd_nums.txt")
output_even = File("even_nums.txt")
output_odd_count = File("odd_count.txt")
output_even_count = File("even_count.txt")
final_output = File("result.tar.gz")

separate_job = Job(separate)\
    .add_input(input_one, input_two, input_three)\
    .add_outputs(output_odd, output_even)
wf.add_jobs(separate_job)
count_odd_job = Job(count)\
    .add_input(output_odd)\
    .add_outputs(output_odd_count)
wf.add_jobs(count_odd_job)
count_even_job = Job(count)\
    .add_input(output_even)\
    .add_outputs(output_even_count)
wf.add_jobs(count_even_job)
tar_job = Job(tar)\
    .add_args("-czvf", "result.tar.gz", "odd_count.txt", "even_count.txt")\
    .add_input(output_odd_count, output_even_count)\
    .add_outputs(final_output)
wf.add_jobs(tar_job)

wf.plan(submit=True)\
    .wait()\
    .statistics()
