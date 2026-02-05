import os.path

def basedir():
    # We are installed in $INSTALL_DIR/lib/python, so need to go up three
    # levels
    return os.path.normpath(
        os.path.join(
            os.path.abspath(__file__),
            '..', '..', '..'
        )
    )

def get_initargs_variants(root=basedir()):
    return [
        os.path.abspath(
            os.path.join(
                root,
                "var",
                "lib",
                "couchbase",
                "initargs")),
        "/opt/couchbase/var/lib/couchbase/initargs",
        os.path.expanduser("~/Library/Application Support/Couchbase/var/lib/couchbase/initargs")]

def find_binary(name, root=basedir()):
    path = os.path.join(root, "bin", name)
    if os.path.exists(path):
        return os.path.abspath(path)

    return None

def find_valid_binary(name, root=basedir()):
    path = find_binary(name, root);
    if path is  None:
        raise RuntimeError("Could not find binary")

    return path