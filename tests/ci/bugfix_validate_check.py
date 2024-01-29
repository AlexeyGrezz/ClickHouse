#!/usr/bin/env python3

from pathlib import Path
import subprocess
import sys
from typing import List, Sequence, Tuple
import csv
import logging
from pathlib import Path
from typing import List, Tuple

from report import ERROR, FAILURE, SUCCESS, FAIL, OK, TestResult, TestResults, JobReport
from env_helper import TEMP_PATH
from stopwatch import Stopwatch
from ci_config import JobNames


def post_commit_status_from_file(file_path: Path) -> List[str]:
    with open(file_path, "r", encoding="utf-8") as f:
        res = list(csv.reader(f, delimiter="\t"))
    if len(res) < 1:
        raise Exception(f'Can\'t read from "{file_path}"')
    if len(res[0]) != 3:
        raise Exception(f'Can\'t read from "{file_path}"')
    return res[0]


def get_failed_test_cases(file_path: Path) -> List[TestResult]:
    job_report = JobReport.load(from_file=file_path)
    test_results = []  # type: List[TestResult]
    for tr in job_report.test_results:
        if tr.status == FAIL:
            tr.name = f"{tr.name} with NOT_OK"
            tr.status = OK
        elif tr.status == FAIL:
            tr.name = f"{tr.name} with NOT_OK"
            tr.status = FAIL
        else:
            # do not inver error status
            pass
        test_results.append(tr)
    return test_results


def process_all_results(
    file_paths: Sequence[Path],
) -> Tuple[str, str, TestResults]:
    all_results = []  # type: TestResults
    status = SUCCESS
    for job_report_path in file_paths:
        test_results = get_failed_test_cases(job_report_path)
        status_cur = FAILURE
        for tr in test_results:
            if tr.status == OK and status_cur == FAILURE:
                # we have atleast on OK result for "test with not ok"
                status_cur = SUCCESS
            elif tr.status == ERROR:
                # looks like ci infrastructure issue
                status_cur = ERROR
        status = SUCCESS if status_cur == SUCCESS else status_cur
        all_results.extend(test_results)

    description = "New tests reproduced a bug"
    if status == FAILURE:
        description = "New tests failed to reproduce a bug"
    elif status == ERROR:
        description = "Some error occured in tests"

    return status, description, all_results


def main():
    logging.basicConfig(level=logging.INFO)
    # args = parse_args()
    stopwatch = Stopwatch()
    jobs_to_validate = [JobNames.STATELESS_TEST_RELEASE, JobNames.INTEGRATION_TEST]
    functional_job_report_file = Path(TEMP_PATH) / "functional_test_job_report.json"
    integration_job_report_file = Path(TEMP_PATH) / "integration_test_job_report.json"
    jobs_report_files = {
        JobNames.STATELESS_TEST_RELEASE: functional_job_report_file,
        JobNames.INTEGRATION_TEST: integration_job_report_file,
    }
    jobs_scripts = {
        JobNames.STATELESS_TEST_RELEASE: "functional_test_check.py",
        JobNames.INTEGRATION_TEST: "integration_test_check.py",
    }

    for test_job in jobs_to_validate:
        report_file = jobs_report_files[test_job]
        test_script = jobs_scripts[test_job]
        if report_file.exists():
            report_file.unlink()
        extra_timeout_option = ""
        if test_job == JobNames.STATELESS_TEST_RELEASE:
            extra_timeout_option = str(3600)
        # "bugfix" must be present in checkname, as integration test runner checks this
        check_name = f"Validate bugfix: {test_job}"
        command = f"python3 {test_script} '{check_name}' {extra_timeout_option} --validate-bugfix --report-to-file {report_file}"
        print(f"Going to validate job [{test_job}], command [{command}]")
        _ = subprocess.run(
            command,
            stdout=sys.stdout,
            stderr=sys.stderr,
            text=True,
            check=False,
            shell=True,
        )
        assert (
            report_file.is_file()
        ), f"No job report [{report_file}] found after job execution"

    status, description, test_results = process_all_results(
        list(jobs_report_files.values())
    )

    additional_files = []
    for job_id, report_file in jobs_report_files.items():
        jr = JobReport.load(from_file=report_file)
        additional_files.append(report_file)
        for file in jr.additional_files:
            file = Path(file)
            file = file.rename(file.parent / file.name.replace(".", f"_{job_id.name}."))
            additional_files.append(file)

    JobReport(
        description=description,
        test_results=test_results,
        status=status,
        start_time=stopwatch.start_time_str,
        duration=stopwatch.duration_seconds,
        additional_files=additional_files,
    ).dump()


if __name__ == "__main__":
    main()
