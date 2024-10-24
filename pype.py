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


class Interruptor:
    def __init__(self, cond):
        self.cond = cond
        self.cond.acquire()

    def __enter__(self):
        self.cond.wait()

    def __exit__(self, typ, val, tb):
        self.cond.release()


class IOThread(threading.Thread):
    def __init__(self):
        super().__init__()
        self.shutdown = False
        self.int_r, self.int_w = os.pipe()
        self.int_w = os.fdopen(self.int_w, 'wb')
        self.int_cond = threading.Condition()
        self.selector = selectors.DefaultSelector()
        self.selector.register(
            self.int_r, selectors.EVENT_READ, self.do_interrupt)

    def interrupt(self, wait=True):
        i = Interruptor(self.int_cond) if wait else None
        if self.int_w:
            self.int_w.write(b'x')
            self.int_w.flush()
        return i

    def do_interrupt(self, key):
        with self.int_cond:
            try:
                os.read(key.fd, 1)
            except Exception:
                pass
            self.int_cond.notify_all()

        if self.shutdown:
            w = self.int_w
            self.int_w = None
            self.int_r = None
            w.close()
            os.close(key.fd)
            self.selector.unregister(key.fd)

    def close(self):
        if not self.shutdown:
            self.shutdown = True
            self.interrupt(False)

    def run(self):
        with self.selector:
            while not self.shutdown and self.selector.get_map():
                for key, event in self.selector.select():
                    events = []
                    if event & selectors.EVENT_READ:
                        events.append('r')
                    if event & selectors.EVENT_WRITE:
                        events.append('w')
                    events = ''.join(events)
                    key.data(key)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, typ, val, tb):
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

    def set_stream(self, sid, fileobj):
        if sid in self.sid_to_fd:
            raise ValueError('stream already open')
        fd = fileobj if isinstance(fileobj, int) else fileobj.fileno()
        self.sid_to_fd[sid] = fd
        self.fd_to_sid[fd] = sid

    def read_chunk_header(self, key):
        data = os.read(key.fd, CHUNK.size - len(self.buf))
        if not data:
            self.close()
            return
        self.buf += data
        if len(self.buf) < CHUNK.size:
            return
        self.sid, self.size = CHUNK.unpack(self.buf)
        self.buf = b''
        if self.size == 0:
            try:
                sfd = self.sid_to_fd[self.sid]
            except KeyError:
                pass
            else:
                os.close(sfd)
                del self.fd_to_sid[sfd]
                del self.sid_to_fd[self.sid]
                if not self.sid_to_fd:
                    self.close()
                    return
            self.sid = None
            self.size = None
        else:
            self.selector.modify(
                key.fd, selectors.EVENT_READ, self.read_chunk)

    def read_chunk(self, key):
        data = os.read(key.fd, self.size - len(self.buf))
        if not data:
            self.close()
            return
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
            if not self.sid_to_fd:
                self.close()
                return
        else:
            self.buf = self.buf[bytes_written:]
        if self.buf:
            return
        self.sid = None
        self.size = None
        self.selector.unregister(key.fd)
        self.selector.register(
            self.fd, selectors.EVENT_READ, self.read_chunk_header)


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
        self.flush_cond = threading.Condition()

    def open(self, sid, *args, **kwargs):
        if sid in self.sid_to_fd:
            raise ValueError('stream already open')
        r, w = os.pipe()
        self.sid_to_fd[sid] = r
        self.fd_to_sid[r] = sid
        with self.interrupt():
            self.selector.register(
                r, selectors.EVENT_READ, self.read_chunk)
        writer = self.reader_to_writer[r] = os.fdopen(w, *args, **kwargs)
        return writer

    def set_stream(self, sid, fileobj):
        if sid in self.sid_to_fd:
            raise ValueError('stream already open')
        fd = fileobj if isinstance(fileobj, int) else fileobj.fileno()
        self.sid_to_fd[sid] = fd
        self.fd_to_sid[fd] = sid
        with self.interrupt():
            self.selector.register(
                fd, selectors.EVENT_READ, self.read_chunk)

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
        with self.flush_cond:
            self.buf = self.buf[bytes_written:]
            if not self.read_enabled and len(self.buf) < self.maxwrite:
                self.enable_read()
            if self.buf:
                return
            self.flush_cond.notify_all()
            if not self.fd_to_sid:
                self.close()
                return
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
        if self.shutdown:
            return
        with self.flush_cond:
            if self.buf:
                self.flush_cond.wait()


if __name__ == '__pype__':
    from importlib.abc import MetaPathFinder, Loader
    from importlib.util import decode_source, module_from_spec
    from importlib.machinery import ModuleSpec


    class RemoteImporter(MetaPathFinder, Loader):
        sid = 250

        def __init__(self, reader, writer):
            self.in_stream = reader.open(self.sid, 'rb')
            self.out_stream = writer.open(self.sid, 'wb')
            self.modules = {}
            self.lock = threading.Lock()

        def _read_source(self, fullname):
            self.out_stream.write(f'{fullname}\n'.encode('utf-8'))
            self.out_stream.flush()
            line = self.in_stream.readline()
            if not line:
                self.out_stream.close()
                return None
            try:
                remaining = int(line.strip())
            except ValueError:
                self.out_stream.close()
                return None
            buf = b''
            while remaining:
                chunk = self.in_stream.read(remaining)
                if not chunk:
                    self.out_stream.close()
                    return None
                remaining -= len(chunk)
                buf += chunk
            if buf:
                return decode_source(buf)

        def find_spec(self, fullname, path, target=None):
            try:
                source = self.modules[fullname]
            except KeyError:
                with self.lock:
                    source = self.modules[fullname] = self._read_source(fullname)
            if source:
                return ModuleSpec(fullname, self)

        def exec_module(self, module):
            fullname = module.__name__
            source = self.modules[fullname]
            compiled = compile(source, f'<remote: {fullname}>', 'exec')
            exec(compiled, module.__dict__)

        def exec_main(self):
            spec = self.find_spec('__main__', None)
            module = module_from_spec(spec)
            self.exec_module(module)

        def close(self):
            self.out_stream.close()
            self.in_stream.close()


    sys.argv.pop(0)
    with WriteThread(sys.stdout) as writer:
        sys.stdout = writer.open(1, 'w')
        try:
            with ReadThread(sys.stdin) as reader:
                sys.stdin = reader.open(0, 'r')
                importer = RemoteImporter(reader, writer)
                sys.meta_path.append(importer)
                try:
                    importer.exec_main()
                finally:
                    importer.close()
                    reader.close()
        finally:
            sys.stdout.close()


elif __name__ == '__main__':
    import shlex
    import argparse
    import subprocess
    from importlib.util import find_spec

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

    sys.path.insert(0, os.path.dirname(opts.prog.name))


    class ImportResponder:
        sid = 250

        def __init__(self, reader, writer, modules):
            self.in_stream = reader.open(self.sid, 'rb')
            self.out_stream = writer.open(self.sid, 'wb')
            self.modules = modules

        def import_source(self, fullname):
            spec = find_spec(fullname)
            if not spec:
                return b''
            try:
                source = spec.loader.get_source(fullname)
            except ImportError:
                return b''
            return source.encode('utf-8')

        def run(self):
            try:
                while True:
                    fullname = self.in_stream.readline()
                    if not fullname:
                        return
                    fullname = fullname.decode('utf-8').strip()
                    try:
                        source = self.modules[fullname]
                    except KeyError:
                        source = self.import_source(fullname)
                    self.out_stream.write(f'{len(source)}\n'.encode('utf-8'))
                    if source:
                        self.out_stream.write(source)
                    self.out_stream.flush()
            finally:
                self.out_stream.close()
                self.in_stream.close()


    with subprocess.Popen(
        [opts.ssh, opts.connect, cmd],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE
    ) as p:
        p.stdin.write(PYPE)
        p.stdin.flush()
        with ReadThread(p.stdout) as reader:
            reader.set_stream(1, sys.stdout)
            with WriteThread(p.stdin) as writer:
                writer.set_stream(0, sys.stdin)
                try:
                    ImportResponder(reader, writer, {
                        '__main__': prog,
                    }).run()
                finally:
                    writer.close()

    sys.exit(p.returncode)
