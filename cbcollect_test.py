from zipfile import ZipFile, ZIP_DEFLATED
import os
import sys
import shutil
import tempfile
import unittest
from io import BytesIO
from timeit import default_timer as timer


def load_cbcollect():
    from importlib.util import spec_from_loader, module_from_spec
    from importlib.machinery import SourceFileLoader
    import sys

    spec = spec_from_loader("cbcollect_info",
                            SourceFileLoader("cbcollect_info",
                                             "cbcollect_info"))
    if spec:
        cbcollect_info = module_from_spec(spec)
        loader = spec.loader
        if loader:
            loader.exec_module(cbcollect_info)
            sys.modules["cbcollect_info"] = cbcollect_info


# These two calls must go here and in this order. We cannot import everything
# from cbcollect_info until we dynamically load it. autopep8 will try and
# reorder this and put the import at the top.
load_cbcollect()
from cbcollect_info import *

if sys.version_info >= (3, 9):
    from random import randbytes
else:
    # Very simple/bad replacement to come up with "random" bytes
    from random import randint

    def randbytes(n: int) -> bytes:
        return bytes([randint(0, a % 256) for a in range(0, n)])


A = "A.log"
C = "C.log"
D = "D.log"
Z = "Z.log"

divider = b"==============================================================================\n"


class TestTaskSystem(unittest.TestCase):
    def test_create_categorize(self):
        unix = [UnixTask("uname", "uname -a", log_file=A),
                UnixTask("ntp peers", "ntpq -p", log_file=D),
                UnixTask("raw /etc/timezone", "cat /etc/timezone",
                         log_file=Z),
                UnixTask("Process list snapshot",
                         "export TERM=''; top -Hb -n1 || top -H n1",
                         log_file=C),
                UnixTask("uname", "uname -a"),
                UnixTask("ntp peers", "ntpq -p"),
                UnixTask("raw /etc/timezone", "cat /etc/timezone",
                         log_file=Z),
                UnixTask("Process list snapshot",
                         "export TERM=''; top -Hb -n1 || top -H n1"),
                CollectFileTask("Grab somefile", "testinput.log",
                                "insides/somefolder/testinput.log"),
                CollectFileTask("Grab somefile", "testinput.log",
                                "insides/somefolder2/testinput.log"),

                # even though this has same relative name, it'll still get
                # divided into another metatask
                CollectFileTask("Grab somefile", "longer/path/to/testinput.log",
                                "insides/somefolder2/testinput.log")]

        prefix = "abcdefg"
        tempdir = tempfile.mkdtemp()
        outfile = f"{tempdir}/testdir.zip"
        runner = TaskRunner(outfile, prefix=prefix, salt_value="abcdefg")
        output = runner._categorize_tasks(*unix)
        for row in output.items():
            if row[0] == A:
                self.assertEqual(len(row[1].subtasks), 1)
            elif row[0] == C:
                self.assertEqual(len(row[1].subtasks), 1)
            elif row[0] == D:
                self.assertEqual(len(row[1].subtasks), 1)
            elif row[0] == Z:
                self.assertEqual(len(row[1].subtasks), 2)
            elif row[0] == "couchbase.log":
                # The collectFileTask's now also create a LiteralTask in
                # couchbase.log to identify it (like it worked before)
                self.assertEqual(len(row[1].subtasks), 3)
            elif row[0] == "testinput.log":
                self.assertEqual(len(row[1].subtasks), 2)
            elif row[0] == "longer/path/to/testinput.log":
                self.assertEqual(len(row[1].subtasks), 1)

    def test_meta(self):
        unix = UnixTask("uname", "uname -a")
        unix2 = UnixTask("ntp peers", "ntpq -p")
        unix3 = UnixTask("raw /etc/timezone", "cat /etc/timezone")
        unix4 = UnixTask("Process list snapshot",
                         "export TERM=''; top -Hb -n1 || top -H n1")
        meta = MetaTask(unix, unix2, *[unix3, unix4], filename="test.log")
        out = meta.execute_all(BytesIO(b""))
        self.assertEqual(out, None)
        windows = WindowsTask("get directory listing", "dir")
        windows2 = WindowsTask("get directory listing", "ls ?")
        meta.append(windows)
        self.assertIn(windows, meta.subtasks)
        self.assertNotIn(windows2, meta.subtasks)

    def test_run_individual(self):
        output = BytesIO()
        unix = UnixTask("uname", "uname -a")
        out = unix.execute(output)
        self.assertEqual(out, 0)
        self.assertIn(b"\n", output.getvalue())
        self.assertGreater(len(output.getvalue()), 0)

    def test_pick_redacter(self):
        writer = BytesIO()
        redact = RedactStream(writer, "12345", "couchbase.log")
        inst = redact._pick_redactor("couchbase.log")
        self.assertIsInstance(inst, CouchbaseLogProcessor)
        inst = redact._pick_redactor("http_access.log")
        self.assertIsInstance(inst, AccessLogProcessor)
        inst = redact._pick_redactor("http_access_internal.log")
        self.assertIsInstance(inst, AccessLogProcessor)
        inst = redact._pick_redactor("otherlogname.log")
        self.assertIsInstance(inst, RegularLogProcessor)
        data = b"abcdefghijklmnop\n"
        output = redact.write(data)
        self.assertEqual(len(data), output)
        self.assertEqual(data, writer.getvalue())

    def test_task_runner(self):
        tempdir = tempfile.mkdtemp()
        collect_file_name = "testinput.log"
        with open(collect_file_name, "wb") as f:
            f.write(b"Some log information or whatever here")

        outfile = f"{tempdir}/testdir.zip"
        prefix = "abcdefg"
        filenames = [f"{prefix}/A.log", f"{prefix}/C.log", f"{prefix}/D.log",
                     f"{prefix}/Z.log", f"{prefix}/testinput.log",
                     f"{prefix}/couchbase.log", f"{prefix}/literal.log"]
        runner = TaskRunner(outfile, prefix=prefix,
                            salt_value="abcdefg")
        tasks: List[Task] = [
            MacOSXTask(
                "uname",
                "uname -a",
                log_file=A),
            MacOSXTask(
                "ntp peers",
                "ntpq -p",
                log_file=D),
            MacOSXTask(
                "raw /etc/timezone",
                "cat /etc/timezone",
                log_file=Z),
            MacOSXTask(
                "Process list snapshot",
                "export TERM=''; top -Hb -n1 || top -H n1",
                log_file=C),
            MacOSXTask(
                "uname",
                "uname -a"),
            MacOSXTask(
                "ntp peers",
                "ntpq -p"),
            MacOSXTask(
                "raw /etc/timezone",
                "cat /etc/timezone",
                log_file=Z),
            MacOSXTask(
                "Process list snapshot",
                "ps aux")]
        tasks_unix: List[Task] = [
            UnixTask(
                "uname",
                "uname -a",
                log_file=A),
            UnixTask(
                "ntp peers",
                "ntpq -p",
                log_file=D),
            UnixTask(
                "ls root",
                "ls -la /",
                log_file=Z),
            UnixTask(
                "Process list snapshot",
                "ps aux",
                log_file=C),
            UnixTask(
                "uname",
                "uname -a"),
            UnixTask(
                "ntp peers",
                "ntpq -p"),
            UnixTask(
                "raw /etc/timezone",
                "cat /etc/timezone",
                log_file=Z),
            UnixTask(
                "Process list snapshot",
                "ps aux")]
        tasks_linux: List[Task] = [
            LinuxTask(
                "uname",
                "uname -a",
                log_file=A),
            LinuxTask(
                "ntp peers",
                "ntpq -p",
                log_file=D),
            LinuxTask(
                "raw /etc/timezone",
                "cat /etc/timezone",
                log_file=Z),
            LinuxTask(
                "Process list snapshot",
                "export TERM=''; top -Hb -n1 || top -H n1",
                log_file=C),
            LinuxTask(
                "uname",
                "uname -a"),
            LinuxTask(
                "ntp peers",
                "ntpq -p"),
            LinuxTask(
                "raw /etc/timezone",
                "cat /etc/timezone",
                log_file=Z),
            LinuxTask(
                "Process list snapshot",
                "export TERM=''; top -Hb -n1 || top -H n1")]

        tasks_windows: List[Task] = [
            WindowsTask(
                "uname",
                "uname -a",
                log_file=A),
            WindowsTask(
                "ntp peers",
                "ntpq -p",
                log_file=D),
            WindowsTask(
                "raw /etc/timezone",
                "cat /etc/timezone",
                log_file=Z),
            WindowsTask(
                "Process list snapshot",
                "export TERM=''; top -Hb -n1 || top -H n1",
                log_file=C),
            WindowsTask(
                "uname",
                "uname -a"),
            WindowsTask(
                "ntp peers",
                "ntpq -p"),
            WindowsTask(
                "raw /etc/timezone",
                "cat /etc/timezone",
                log_file=Z),
            WindowsTask(
                "Process list snapshot",
                "export TERM=''; top -Hb -n1 || top -H n1")]

        tasks_more: List[Task] = [
            MacOSXTask(
                "uname",
                "uname -a",
                log_file=C),
            MacOSXTask(
                "ntp peers",
                "ntpq -p",
                log_file=C),
            CollectFileTask(
                "Collect teh A.log file",
                collect_file_name),
            LiteralTask(
                "Some literal task test",
                "Write this literal please",
                log_file="literal.log")]

        runner.run_tasks(*tasks, *tasks_more, *tasks_unix, *tasks_windows,
                         *tasks_linux)
        runner.close()
        with open(outfile, "r") as f:
            self.assertEqual(f.name, outfile)

        with ZipFile(outfile, mode="r") as zippy:
            files = zippy.filelist
            file_names = [f.filename for f in zippy.filelist]
            for f in filenames:
                self.assertIn(f, file_names)
            for f in files:
                self.assertEqual(f.compress_type, ZIP_DEFLATED)

        redacted_zip = f"{tempdir}/testdir-redacted.zip"
        with ZipFile(redacted_zip, mode="r") as zippy:
            files = zippy.filelist
            file_names = [f.filename for f in zippy.filelist]

            for f in filenames:
                self.assertIn(f, file_names)
            for f in files:
                self.assertEqual(f.compress_type, ZIP_DEFLATED)

        shutil.rmtree(tempdir)
        os.remove(collect_file_name)

    def test_double_stream(self):
        left = BytesIO()
        right = BytesIO()
        stream = DoubleStream(left, right)
        value = randbytes(1024)
        res = stream.write(value)
        self.assertEqual(1024, res)
        self.assertEqual(left.getvalue(), value)
        self.assertEqual(right.getvalue(), value)

    def test_artifacts(self):
        tempdir = tempfile.mkdtemp()
        collect_file_name = "testinput.log"
        artifact_name = "someotherfile.log"
        artifact_name2 = "someotherfile2.log"

        with open(collect_file_name, "wb") as f:
            f.write(b"Some log information or whatever here")

        outfile = f"{tempdir}/testdir.zip"
        prefix = "aaaa/bbbbb/cccc"
        filenames = [f"{prefix}/A.log", f"{prefix}/C.log", f"{prefix}/D.log",
                     f"{prefix}/Z.log", f"{prefix}/testinput.log",
                     f"{prefix}/couchbase.log", f"{prefix}/literal.log",
                     f"{prefix}/{artifact_name}", f"{prefix}/{artifact_name2}"]
        runner = TaskRunner(outfile, prefix=prefix,
                            salt_value="abcdefg", tmp_dir=tempdir)

        runner_tmp = runner.tmpdir

        artifact_path = f"{runner_tmp}/{artifact_name}"
        artifact_path2 = f"{runner_tmp}/{artifact_name2}"
        with open(artifact_path, "wb") as f:
            f.write(b"some artifact data here")
        with open(artifact_path2, "wb") as f:
            f.write(b"some artifact data here.... again")

        tasks: List[Task] = [
            MacOSXTask("uname", "uname -a", log_file=C),
            MacOSXTask("ntp peers", "ntpq -p", log_file=C),
            CollectFileTask("Collect teh A.log file", collect_file_name),
            LiteralTask("Some literal task test", "Write this literal please",
                        log_file="couchbase.log"),
            WindowsTask("Process list snapshot",
                        "export TERM=''; top -Hb -n1 || top -H n1",
                        artifacts=[artifact_name]),
            UnixTask("Process list snapshot", "ps aux",
                     artifacts=[artifact_name2]),
            MacOSXTask("Process list snapshot", "ps aux",
                       artifacts=[artifact_name])]
        runner.run_tasks(*tasks)
        runner.close()
        with open(outfile, "r") as f:
            self.assertEqual(f.name, outfile)

        with ZipFile(outfile, mode="r") as zippy:
            files = zippy.filelist
            for f in files:
                self.assertIn(f.filename, filenames)
                self.assertEqual(f.compress_type, ZIP_DEFLATED)

        redacted_zip = f"{tempdir}/testdir-redacted.zip"
        with ZipFile(redacted_zip, mode="r") as zippy:
            for f in zippy.filelist:
                self.assertIn(f.filename, filenames)
                self.assertEqual(f.compress_type, ZIP_DEFLATED)

        shutil.rmtree(tempdir)
        os.remove(collect_file_name)

    def test_num_samples(self):
        """
        HOPEFULLY this is 'platform independent' enough to work. It won't work
        on windows unfortunately because I don't want to check those responses.
        """
        if os.name != 'nt':
            tempdir = tempfile.mkdtemp()
            prefix = "aaaa/bbbbb/cccc"
            outfile = f"{tempdir}/testdir.zip"
            collect_file_name = "testinput.log"
            filenames = [f"{prefix}/C.log", f"{prefix}/{collect_file_name}",
                         f"{prefix}/couchbase.log", f"{prefix}/literal.log"]
            with open(collect_file_name, "wb") as f:
                f.write(b"Some log information or whatever here")

            runner = TaskRunner(outfile, prefix=prefix,
                                salt_value="abcdefg", tmp_dir=tempdir)

            tasks: List[Task] = [
                UnixTask(
                    "uname",
                    "uname -a",
                    log_file=C,
                    num_samples=2,
                    interval=1),
                UnixTask(
                    "ntp peers",
                    "ntpqabcd -p",
                    log_file=C,
                    num_samples=3,
                    interval=2),
                CollectFileTask(
                    "Collect teh A.log file",
                    collect_file_name),
                LiteralTask(
                    "Some literal task test",
                    "Write this literal please",
                    log_file="couchbase.log")]
            runner.run_tasks(*tasks)
            runner.close()
            with open(outfile, "r") as f:
                self.assertEqual(f.name, outfile)

            with ZipFile(outfile, mode="r") as zippy:
                files = zippy.filelist
                for f in files:
                    self.assertIn(f.filename, filenames)
                    self.assertEqual(f.compress_type, ZIP_DEFLATED)

            redacted_zip = f"{tempdir}/testdir-redacted.zip"
            with ZipFile(redacted_zip, mode="r") as zippy:
                for f in zippy.filelist:
                    self.assertIn(f.filename, filenames)
                    self.assertEqual(f.compress_type, ZIP_DEFLATED)
                with zippy.open(f"{prefix}/C.log", mode="r") as f:
                    first = f.readline()
                    self.assertEqual(divider, first)
                    second = f.readline()
                    second_expect = b"uname\n"
                    self.assertEqual(second_expect, second)
                    third = f.readline()
                    third_expect = b"uname -a\n"
                    self.assertEqual(third_expect, third)
                    fourth = f.readline()
                    self.assertEqual(divider, fourth)
                    fifth = f.readline()
                    self.assertIn(b"\n", fifth)
                    sixth = f.readline()
                    self.assertIn(b"\n", sixth)
                    seventh = f.readline()
                    self.assertEqual(divider, seventh)
                    eighth = f.readline()
                    eighth_expect = b"ntp peers\n"
                    self.assertEqual(eighth_expect, eighth)
                    ninth = f.readline()
                    ninth_expect = b"ntpqabcd -p\n"
                    self.assertEqual(ninth_expect, ninth)
                    tenth = f.readline()
                    self.assertEqual(divider, tenth)

                    eleventh = f.readline()
                    if sys.platform == "darwin":
                        eleventh_expect = b"/bin/sh: ntpqabcd: command not found\n"
                        self.assertEqual(eleventh_expect, eleventh)
                    elif sys.platform == "linux":
                        eleventh_expect = b"/bin/sh: 1: ntpqabcd: not found\n"
                        self.assertEqual(eleventh_expect, eleventh)
                    twelfth = f.readline()
                    self.assertEqual(eleventh_expect, twelfth)
                    thirteenth = f.readline()
                    self.assertEqual(eleventh_expect, thirteenth)
                    last = f.readline()
                    self.assertEqual(b"", last)
            shutil.rmtree(tempdir)

    def test_use_shell(self):
        """
        TODO: This may contain asserts that are too specifically based on
        outputs from the shell that may not match.

        The main thing this is supposed to test is the use_shell flag and one
        of the error cases when using it, as well.
        """
        if os.name != 'nt':
            tempdir = tempfile.mkdtemp()
            outfile = f"{tempdir}/testdir.zip"
            prefix = "aaaa/bbbbb/cccc"
            filenames = [f"{prefix}/A.log", f"{prefix}/couchbase.log"]
            runner = TaskRunner(outfile, prefix=prefix,
                                salt_value="abcdefg", tmp_dir=tempdir)
            tasks: List[Task] = [
                UnixTask("Process list [failure]", ["ps aux", "gibberish"],
                         log_file=A),
                UnixTask("Process list [success]", ["ps", "aux"], log_file=A)]
            runner.run_tasks(*tasks)
            runner.close()

            with open(outfile, "r") as f:
                self.assertEqual(f.name, outfile)

            with ZipFile(outfile, mode="r") as zippy:
                files = zippy.filelist
                for f in files:
                    self.assertIn(f.filename, filenames)
                    self.assertEqual(f.compress_type, ZIP_DEFLATED)

                redacted_zip = f"{tempdir}/testdir-redacted.zip"

            with ZipFile(redacted_zip, mode="r") as zippy:
                for f in zippy.filelist:
                    self.assertIn(f.filename, filenames)
                    self.assertEqual(f.compress_type, ZIP_DEFLATED)
                with zippy.open(f"{prefix}/A.log", mode="r") as f:
                    line = f.readline()
                    self.assertEqual(divider, line)
                    line = f.readline()
                    self.assertEqual(b'Process list [failure]\n', line)
                    line = f.readline()
                    self.assertEqual(b'ps aux gibberish\n', line)
                    line = f.readline()
                    self.assertEqual(divider, line)
                    line = f.readline()
                    # skip validating this line, too platform dependent

                    line = f.readline()
                    self.assertEqual(divider, line)
                    line = f.readline()
                    self.assertEqual(b'Process list [success]\n', line)
                    line = f.readline()
                    self.assertEqual(b'ps aux\n', line)
                    line = f.readline()
                    self.assertEqual(divider, line)
                    line = f.readline()
                    self.assertNotIn(b"Failed", line)
                    if sys.platform == "darwin":
                        self.assertEqual(
                            b'USER               PID  %CPU %MEM      VSZ    RSS   TT  STAT STARTED      TIME COMMAND\n',
                            line)
                    elif sys.platform == "linux":
                        self.assertEqual(
                            b'USER         PID %CPU %MEM    VSZ   RSS TTY      STAT START   TIME COMMAND\n',
                            line)

            # shutil.rmtree(tempdir)
            print(f"Tempdir: {tempdir}")

    def test_collect_directory(self):
        """
        Test out the collect_directory function and resulting tasks
        """
        if os.name != 'nt':
            tempdir = tempfile.mkdtemp()
            inner_dir = "some_directory"
            outdir = os.path.join(tempdir, inner_dir)
            os.mkdir(f"{outdir}")
            artifact_name = "someotherfile.log"
            artifact_path = os.path.join(outdir, artifact_name)
            artifact_name2 = "someotherfile2.log"
            artifact_path2 = os.path.join(outdir, artifact_name2)
            base3 = f"{outdir}/another"
            os.mkdir(base3)
            artifact_name3 = "another/someotherfile3.log"
            artifact_path3 = os.path.join(outdir, artifact_name3)
            with open(artifact_path, mode="wb") as f:
                f.write(b"Some data here")
            with open(artifact_path2, mode="wb") as f:
                f.write(b"Some data here .. 2")
            with open(artifact_path3, mode="wb") as f:
                f.write(b"Some data here .. 3, and more?")
            outfile = f"{tempdir}/testdir.zip"
            prefix = "aaaa/bbbbb/cccc"
            filenames = [f"{prefix}/couchbase.log",
                         f"{prefix}/{inner_dir}/{artifact_name}",
                         f"{prefix}/{inner_dir}/{artifact_name2}",
                         f"{prefix}/{inner_dir}/{artifact_name3}"]
            runner = TaskRunner(outfile, prefix=prefix,
                                salt_value="abcdefg", tmp_dir=tempdir)
            tasks: List[Task] = [
                LiteralTask(
                    "Some literal task test",
                    "Write this literal please",
                    log_file="couchbase.log"),
                UnixTask(
                    "Process list snapshot",
                    "ps aux")]
            collected_dir = CollectFileTask.create_directory_collection_tasks(
                outdir,
                inner_dir)

            runner.run_tasks(*tasks, *collected_dir)
            runner.close()
            with open(outfile, "r") as f:
                self.assertEqual(f.name, outfile)
            with ZipFile(outfile, mode="r") as zippy:
                files = zippy.filelist
                for f in files:
                    self.assertIn(f.filename, filenames)
                    self.assertEqual(f.compress_type, ZIP_DEFLATED)
            redacted_zip = f"{tempdir}/testdir-redacted.zip"
            with ZipFile(redacted_zip, mode="r") as zippy:
                for f in zippy.filelist:
                    self.assertIn(f.filename, filenames)
                    self.assertEqual(f.compress_type, ZIP_DEFLATED)
            shutil.rmtree(tempdir)

    def test_cmd_timeout(self):
        if os.name != 'nt':
            tempdir = tempfile.mkdtemp()
            outfile = f"{tempdir}/testdir.zip"
            prefix = "aaaa/bbbbb/cccc"
            filenames = [f"{prefix}/A.log"]
            runner = TaskRunner(outfile, prefix=prefix,
                                salt_value="abcdefg", tmp_dir=tempdir)
            tasks: List[Task] = [
                UnixTask("Sleep for 100 seconds", ["sleep", "100"],
                         log_file=A, timeout=2)]
            start = timer()
            runner.run_tasks(*tasks)
            end = timer()
            elapsed = end - start
            self.assertLess(elapsed, 5)
            runner.close()

            with open(outfile, "r") as f:
                self.assertEqual(f.name, outfile)

            with ZipFile(outfile, mode="r") as zippy:
                files = zippy.filelist
                files_names = [f.filename for f in zippy.filelist]
                self.assertEqual(len(files), 1)
                for f in filenames:
                    self.assertIn(f, files_names)

                for f in files:
                    self.assertEqual(f.compress_type, ZIP_DEFLATED)

                redacted_zip = f"{tempdir}/testdir-redacted.zip"

            with ZipFile(redacted_zip, mode="r") as zippy:
                files = zippy.filelist
                files_names = [f.filename for f in zippy.filelist]
                self.assertEqual(len(files), 1)
                for f in filenames:
                    self.assertIn(f, files_names)
                for f in files:
                    self.assertEqual(f.compress_type, ZIP_DEFLATED)

            shutil.rmtree(tempdir)

if __name__ == "__main__":
    unittest.main()
