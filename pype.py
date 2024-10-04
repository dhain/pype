import sys


if __name__ == '__pype__':
    import types
    sys.argv.pop(0)
    mod = types.ModuleType('__main__')
    compiled = compile(
        ''.join(iter(sys.stdin.readline, 'EOF\n')),
        sys.argv[0], 'exec'
    )
    exec(compiled, mod.__dict__)


elif __name__ == '__main__':
    import os
    import shlex
    import argparse
    import threading
    import selectors
    import subprocess

    parser = argparse.ArgumentParser(
        prog=sys.argv[0],
        description='Run a python program over an ssh pipe.'
    )
    parser.add_argument(
        '--ssh', '-s', default='ssh',
        help='Path to local ssh command.'
    )
    parser.add_argument(
        '--python', '-p', default='python3',
        help='Path to python command on remote.'
    )
    parser.add_argument(
        'connect',
        help='SSH connection string.'
    )
    parser.add_argument(
        'prog', type=argparse.FileType('rb'),
        help='Python file to run on remote.'
    )
    parser.add_argument(
        'args', nargs='*',
        help='Arguments to pass to remote program.'
    )

    class StreamCopier(threading.Thread):
        if hasattr(selectors, 'PollSelector'):
            selector = selectors.PollSelector
        else:
            selector = selectors.SelectSelector

        def __init__(self, src, dest):
            super().__init__()
            self.src = src
            self.dest = dest
            self.stop = False

        def run(self):
            with type(self).selector() as selector:
                selector.register(self.src, selectors.EVENT_READ)
                selector.register(self.dest, selectors.EVENT_WRITE)
                while selector.get_map() and not self.stop:
                    data = b''
                    for key, events in selector.select():
                        if key.fileobj is self.src:
                            chunk = os.read(key.fd, 32768)
                            if chunk:
                                data += chunk
                            else:
                                selector.unregister(key.fileobj)
                                key.fileobj.close()
                                self.src = None

                        elif key.fileobj is self.dest:
                            if data:
                                try:
                                    bytes_written = os.write(key.fd, data)
                                except BrokenPipeError:
                                    selector.unregister(key.fileobj)
                                    key.fileobj.close()
                                    self.dest = None
                                else:
                                    data = data[bytes_written:]

                            elif self.src is None:
                                selector.unregister(key.fileobj)
                                try:
                                    key.fileobj.close()
                                except BrokenPipeError:
                                    pass
                                self.dest = None


    opts = parser.parse_args(sys.argv[1:])

    cmd = shlex.join([
        opts.python, '-c',
        'import sys, types; '
        'mod=types.ModuleType("__pype__"); '
        'exec(compile("".join(iter(sys.stdin.readline,"EOF\\n")),'
        'sys.stdin.name,"exec"),mod.__dict__)',
        opts.prog.name
    ] + opts.args)

    with opts.prog:
        prog = opts.prog.read()

    with open(__file__, 'rb') as f:
        pype = f.read()

    p = subprocess.Popen([opts.ssh, opts.connect, cmd], stdin=subprocess.PIPE)
    t = None
    try:
        p.stdin.write(pype)
        p.stdin.write(b'\nEOF\n')
        p.stdin.write(prog)
        p.stdin.write(b'\nEOF\n')
        p.stdin.flush()
        t = StreamCopier(sys.stdin.buffer, p.stdin)
        t.start()
        p.wait()
    finally:
        if t is not None:
            t.stop = True
            t.join()
        p.terminate()

    sys.exit(p.returncode)
