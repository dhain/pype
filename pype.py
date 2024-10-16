import os
import sys
import types
import struct

CHUNK = struct.Struct('!BH')


def read_exact(fd, remain):
    buf = b''
    while remain:
        chunk = os.read(fd, remain)
        if not chunk:
            raise BrokenPipeError()
        buf += chunk
        remain -= len(chunk)
    return buf


def bootstrap():
    fd = sys.stdin.fileno()
    _, size = CHUNK.unpack(read_exact(fd, CHUNK.size))
    src = read_exact(fd, size).decode('utf-8')
    mod = types.ModuleType('__pype__')
    compiled = compile(src, '<stdin>', 'exec')
    exec(compiled, mod.__dict__)
    return mod

# END BOOTSTRAP

import selectors
import threading
import signal


class ShutdownError(Exception):
    pass


class IOThread(threading.Thread):
    def __init__(self):
        super().__init__()
        self.shutdown_r, self.shutdown_w = os.pipe()
        self.shutdown_w = os.fdopen(self.shutdown_w, 'wb')
        self.selector = selectors.DefaultSelector()
        self.selector.register(
            self.shutdown_r, selectors.EVENT_READ, self.do_shutdown)

    def do_shutdown(self, key):
        w = self.shutdown_w
        self.shutdown_w = None
        self.shutdown_r = None
        w.close()
        try:
            os.read(key.fd, 512)
        except Exception:
            pass
        try:
            os.close(key.fd)
        except Exception:
            pass
        self.selector.unregister(key.fd)
        return True

    def close(self):
        if self.shutdown_w:
            self.shutdown_w.write(b'x')
            self.shutdown_w.flush()

    def run(self):
        with self.selector:
            while self.selector.get_map():
                for key, event in self.selector.select():
                    if key.data(key):
                        self.close()
                        return

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, typ, val, tb):
        self.close()
        self.join()


class ReadThread(IOThread):
    def __init__(self, stream):
        super().__init__()
        self.stream = stream
        self.fd = stream.fileno()
        self.sid_to_fd = {}
        self.fd_to_sid = {}
        self.buf = b''
        self.sid = None
        self.size = None
        self.selector.register(
            self.fd, selectors.EVENT_READ, self.read_chunk_header)

    def open(self, sid, *args, **kwargs):
        if sid in self.sid_to_fd:
            raise ValueError('stream already open')
        r, w = os.pipe()
        self.sid_to_fd[sid] = w
        self.fd_to_sid[w] = sid
        return os.fdopen(r, *args, **kwargs)

    def read_chunk_header(self, key):
        data = os.read(key.fd, CHUNK.size - len(self.buf))
        if not data:
            return True
        self.buf += data
        if len(self.buf) < CHUNK.size:
            return
        self.sid, self.size = CHUNK.unpack(self.buf)
        self.buf = b''
        if self.size == 0:
            try:
                sfd = self.sid_to_fd[self.sid]
                os.close(sfd)
            except KeyError:
                pass
            self.sid = None
            self.size = None
        else:
            self.selector.modify(
                key.fd, selectors.EVENT_READ, self.read_chunk)

    def read_chunk(self, key):
        data = os.read(key.fd, self.size - len(self.buf))
        if not data:
            return True
        self.buf += data
        if len(self.buf) < self.size:
            return
        try:
            sfd = self.sid_to_fd[self.sid]
        except KeyError:
            self.selector.modify(
                key.fd, selectors.EVENT_READ, self.read_chunk_header)
        else:
            self.selector.unregister(key.fd)
            self.selector.register(
                sfd, selectors.EVENT_WRITE, self.write_chunk)

    def write_chunk(self, key):
        try:
            bytes_written = os.write(key.fd, self.buf)
        except BrokenPipeError:
            self.buf = b''
            try:
                os.close(key.fd)
            except Exception:
                pass
            sid = self.fd_to_sid.pop(key.fd)
            del self.sid_to_fd[sid]
        else:
            self.buf = self.buf[bytes_written:]
        if self.buf:
            return
        self.sid = None
        self.size = None
        self.selector.unregister(key.fd)
        self.selector.register(
            self.fd, selectors.EVENT_READ, self.read_chunk_header)


class WritePipe:
    def __init__(self, event, f):
        self._event = event
        self._f = f

    def write(self, *args, **kwargs):
        self._event.clear()
        self._f.write(*args, **kwargs)

    def __getattr__(self, attr):
        return getattr(self._f, attr)


class WriteThread(IOThread):
    maxread = 1024
    maxwrite = 10240

    def __init__(self, stream):
        super().__init__()
        self.stream = stream
        self.fd = stream.fileno()
        self.sid_to_fd = {}
        self.fd_to_sid = {}
        self.reader_to_writer = {}
        self.buf = b''
        self.write_enabled = False
        self.read_enabled = True
        self.flushed = threading.Event()
        self.flushed.set()

    def open(self, sid, *args, **kwargs):
        if sid in self.sid_to_fd:
            raise ValueError('stream already open')
        r, w = os.pipe()
        self.sid_to_fd[sid] = r
        self.fd_to_sid[r] = sid
        self.selector.register(
            r, selectors.EVENT_READ, self.read_chunk)
        writer = self.reader_to_writer[r] = WritePipe(
            self.flushed, os.fdopen(w, *args, **kwargs))
        return writer

    def read_chunk(self, key):
        try:
            sid = self.fd_to_sid[key.fd]
        except KeyError:
            return
        data = os.read(key.fd, self.maxread)
        if data:
            self.buf += CHUNK.pack(sid, len(data)) + data
        else:
            self.buf += CHUNK.pack(sid, 0)
            fd = self.sid_to_fd.pop(sid)
            del self.fd_to_sid[fd]
            self.selector.unregister(fd)
        if len(self.buf) > self.maxwrite:
            self.disable_read()
        self.enable_write()

    def write_chunk(self, key):
        try:
            bytes_written = os.write(key.fd, self.buf)
        except BrokenPipeError:
            self.close()
            return
        self.buf = self.buf[bytes_written:]
        if not self.read_enabled and len(self.buf) < self.maxwrite:
            self.enable_read()
        if self.buf:
            return
        self.flushed.set()
        self.write_enabled = False
        self.selector.unregister(key.fd)

    def enable_read(self):
        self.read_enabled = True
        for fd in self.fd_to_sid:
            self.selector.register(
                fd, selectors.EVENT_READ, self.read_chunk)

    def disable_read(self):
        for fd in self.fd_to_sid:
            self.selector.unregister(fd)

    def enable_write(self):
        if not self.write_enabled:
            self.write_enabled = True
            self.selector.register(
                self.fd, selectors.EVENT_WRITE, self.write_chunk)

    def flush(self):
        self.flushed.wait()

    def __exit__(self, typ, val, tb):
        for writer in self.reader_to_writer.values():
            writer.close()
        self.flush()
        return super().__exit__(typ, val, tb)


if __name__ == '__pype__':
    sys.argv.pop(0)
    with WriteThread(sys.stdout) as writer:
        sys.stdout = writer.open(1, 'w')
        print('hello world!')
    #load_module('__main__', sys.argv[0])


elif __name__ == '__main__':
    import shlex
    import argparse
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

    opts = parser.parse_args(sys.argv[1:])

    with open(__file__, 'r') as f:
        BOOTSTRAP = ''.join(iter(f.readline, '# END BOOTSTRAP\n'))
        PYPE = (BOOTSTRAP + '\n' + f.read()).encode('utf-8')
    BOOTSTRAP += 'bootstrap()\n'
    PYPE = CHUNK.pack(255, len(PYPE)) + PYPE

    cmd = shlex.join([opts.python, '-c', BOOTSTRAP, opts.prog.name] + opts.args)

    with opts.prog:
        prog = opts.prog.read()

    with subprocess.Popen(
        [opts.ssh, opts.connect, cmd],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE
    ) as p:
        p.stdin.write(PYPE)
        p.stdin.flush()
        with ReadThread(p.stdout) as reader:
            stdout = reader.open(1, 'r')
            print(f'from child: {stdout.read().strip()}')

    sys.exit(p.returncode)
