from __future__ import annotations

import contextlib
import os
import ssl
import sys
import traceback
import uuid
from pathlib import Path
from subprocess import PIPE, Popen
from tempfile import NamedTemporaryFile
from types import ModuleType
from unittest import TestCase

from eventsourcing.utils import clear_topic_cache

BASE_DIR = Path(__file__).parents[1]


class TestDocs(TestCase):
    def tearDown(self) -> None:
        clear_topic_cache()
        with contextlib.suppress(KeyError):
            del os.environ["KURRENTDB_URI"]
        with contextlib.suppress(KeyError):
            del os.environ["KURRENTDB_ROOT_CERTIFICATES"]

    def test_readme(self) -> None:
        self._out = ""

        path = BASE_DIR / "README.md"
        if not path.exists():
            self.fail(f"README file not found: {path}")
        self.check_code_snippets_in_file(path, [])

    def check_code_snippets_in_file(  # noqa: C901
        self, doc_path: Path, failures: list[tuple[Path, str]]
    ) -> None:
        # Extract lines of Python code from the README.md file.

        lines = []
        num_code_lines = 0
        num_code_lines_in_block = 0
        is_code = False
        is_md = False
        is_rst = False
        last_line = ""
        is_literalinclude = False
        module = ""
        with doc_path.open() as doc_file:
            for line_index, orig_line in enumerate(doc_file):
                # print("Line index:", line_index)
                # print("Orig line:", orig_line)
                # print("Last line:", last_line)

                line = orig_line.strip("\n")
                if line.startswith("```python"):
                    # Start markdown code block.
                    if is_rst:
                        self.fail(
                            "Markdown code block found after restructured text block "
                            "in same file."
                        )
                    is_code = True
                    is_md = True
                    line = ""
                    num_code_lines_in_block = 0
                elif is_code and is_md and line.startswith("```"):
                    # Finish markdown code block.
                    if not num_code_lines_in_block:
                        self.fail(f"No lines of code in block: {line_index + 1}")
                    is_code = False
                    line = ""
                elif is_code and is_rst and line.startswith("```"):
                    # Can't finish restructured text block with markdown.
                    self.fail(
                        "Restructured text block terminated with markdown format '```'"
                    )
                elif line.startswith(".. code-block:: python") or (
                    line.strip() == ".." and "include-when-testing" in last_line
                ):
                    # Start restructured text code block.
                    if is_md:
                        self.fail(
                            "Restructured text code block found after markdown block "
                            "in same file."
                        )
                    is_code = True
                    is_rst = True
                    line = ""
                    num_code_lines_in_block = 0
                elif line.startswith(".. literalinclude::"):
                    is_literalinclude = True
                    literal_include_path = line.strip().split(" ")[
                        -1
                    ]  # get the file path
                    module = literal_include_path[:-3]  # remove the '.py' from the end
                    module = module.lstrip("./")  # remove all the ../../..
                    module = module.replace("/", ".")  # swap dots for slashes
                    line = ""

                elif is_literalinclude:
                    if "pyobject" in line:
                        # Assume ".. literalinclude:: ../../xxx/xx.py"
                        # Or ".. literalinclude:: ../xxx/xx.py"
                        # Assume "    :pyobject: xxxxxx"
                        pyobject = line.strip().split(" ")[-1]
                        statement = f"from {module} import {pyobject}"
                        line = statement
                    elif not line.strip():
                        is_literalinclude = False
                        module = ""

                elif is_code and is_rst and line and not line.startswith(" "):
                    # Finish restructured text code block.
                    if not num_code_lines_in_block:
                        self.fail(f"No lines of code in block: {line_index + 1}")
                    is_code = False
                    line = ""
                elif ":emphasize-lines:" in line:
                    line = ""
                elif is_code:
                    # Process line in code block. Restructured code block normally
                    # indented with four spaces.
                    if is_rst and len(line.strip()):
                        if not line.startswith("    "):
                            self.fail(
                                f"Code line needs 4-char indent: {line!r}: {doc_path}"
                            )
                        # Strip four chars of indentation.
                        line = line[4:]

                    if len(line.strip()):
                        num_code_lines_in_block += 1
                        num_code_lines += 1
                else:
                    line = ""
                lines.append(line)
                # if orig_line.strip():
                last_line = orig_line

        print(f"{num_code_lines} lines of code in {doc_path}")
        self.substitute_lines(lines)

        if num_code_lines == 0:
            return

        # Execute the code.
        lines[0] = "from __future__ import annotations"
        lines[1] = "from eventsourcing.domain import datetime_now_with_tzinfo"
        lines[2] = "started = datetime_now_with_tzinfo()"
        lines.append(
            "print(f'exec duration: "
            "{(datetime_now_with_tzinfo() - started).total_seconds()}s')"
        )

        source = "\n".join(lines) + "\n"

        try:
            code = compile(source, "__main__", "exec")
            exec_module = ModuleType("__main__")
            sys.modules["__main__"] = exec_module
            exec(code, exec_module.__dict__)  # noqa: S102
        except BaseException:
            error = traceback.format_exc()
            error = error.replace('File "__main__",', f'File "{doc_path}"')
            print(f"FAILED DOC: {doc_path}\nERROR: {error}\n")
            failures.append((doc_path, error))
            raise self.failureException from None

        print("Code executed OK")

        # Check the code with mypy and catch errors.
        with NamedTemporaryFile("w+") as tempfile:
            temp_path = tempfile.name
            tempfile.writelines(source)
            tempfile.flush()

            p = Popen(  # noqa: S603
                [
                    sys.executable,
                    "-m",
                    "mypy",
                    # "--disable-error-code=no-redef",
                    # "--disable-error-code=attr-defined",
                    # "--disable-error-code=name-defined",
                    # "--disable-error-code=truthy-function",
                    temp_path,
                ],
                stdout=PIPE,
                stderr=PIPE,
                env={
                    "PYTHONPATH": BASE_DIR,
                },
                encoding="utf-8",
            )

            out, err = p.communicate()

            # # Run the code and catch errors.
            # p = Popen(  # no qa: S603
            #     [sys.executable, temp_path],
            #     stdout=PIPE,
            #     stderr=PIPE,
            #     env={"PYTHONPATH": base_dir},
            #     encoding="utf-8",
            # )
            # out, err = p.communicate()
        # out = out.decode("utf8")
        # err = err.decode("utf8")

        # To get clickable links in PyCharm console, prefix absolute paths
        # with "file://". Using paths relative to project root folder doesn't work.
        # Actually doesn't work because PyCharm tries to open the file in a browser.
        # out = out.replace(temp_path, "file://" + str(doc_path)[:-4]+".py")
        # err = err.replace(temp_path, "file://" + str(doc_path)[:-4]+".py")

        # Got clickable links with PyCharm plugin "Clickable Output Links"
        # https://github.com/Shadow-Devil/output-link-filter

        out = out.replace(temp_path, str(doc_path))
        out = out.replace(": error:", " error:")

        exit_status = p.wait()

        # Check for errors running the code.
        if exit_status:
            print("Mypy errors:")
            print(out)
            print(err)
            # self.fail(out + err)
        else:
            print("No mypy errors")

    def substitute_lines(self, lines: list[str]) -> None:
        fido_suffix = str(uuid.uuid4())
        for i in range(len(lines)):
            line = lines[i]
            line = line.replace('"Fido"', '"Fido-' + fido_suffix + '"')
            lines[i] = line


class TestDocsSecure(TestDocs):
    def substitute_lines(self, lines: list[str]) -> None:
        super().substitute_lines(lines)
        for i in range(len(lines)):
            line = lines[i]
            if line.startswith("os.environ['KURRENTDB_URI']"):
                line = (
                    "os.environ['KURRENTDB_URI'] ="
                    " 'esdb://admin:changeit@localhost:2114'"
                )
            elif line.startswith("os.environ['KURRENTDB_ROOT_CERTIFICATES']"):
                root_certificates = ssl.get_server_certificate(addr=("localhost", 2114))
                root_certificates = "\\n".join(root_certificates.split("\n"))
                line = (
                    f"os.environ['KURRENTDB_ROOT_CERTIFICATES']"
                    f" = '{root_certificates}'"
                )
            lines[i] = line
