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
import pickle


class ShutdownError(Exception):
    pass


class Interruptor:
    def __init__(self):
        self.r, self.w = os.pipe()

    def fileno(self):
        return self.r

    def close(self):
        try:
            os.close(self.w)
        except Exception:
            pass
        try:
            os.close(self.r)
        except Exception:
            pass

    def interrupt(self):
        while not os.write(self.w, b'x'):
            pass

    def ack(self):
        try:
            os.read(self.r, 1)
        except Exception:
            pass


class IOThread(threading.Thread):
    def __init__(self):
        super().__init__()
        self.shutdown = False
        self.interruptor = Interruptor()
        self.selector = selectors.DefaultSelector()
        self.selector.register(
            self.interruptor, selectors.EVENT_READ, self.handle_interrupt)

    def _unreg_fd(self, fd):
        try:
            self.selector.unregister(fd)
        except KeyError:
            pass

    def _close_fd(self, fd):
        self._unreg_fd(fd)
        try:
            os.close(fd)
        except Exception:
            pass

    def interrupt(self):
        self.interruptor.interrupt()

    def handle_interrupt(self, _):
        self.interruptor.ack()
        if self.shutdown:
            self._unreg_fd(self.interruptor)
            self.interruptor.close()
            self.interruptor = None

    def close(self):
        self.shutdown = True
        try:
            self.interrupt()
        except Exception:
            pass

    def run(self):
        with self.selector:
            while not self.shutdown and self.selector.get_map():
                for key, event in self.selector.select():
                    key.data(key)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, typ, val, tb):
        self.join()


class DemuxThread(IOThread):
    def __init__(self, stream):
        super().__init__()
        self.stream = stream
        self.fd = stream.fileno()
        self.sid_to_fd = {}
        self.fd_to_sid = {}
        self.buf = b''
        self.sid = None
        self.size = None
        self.flush_cond = threading.Condition()
        self.selector.register(
            self.fd, selectors.EVENT_READ, self.read_chunk_header)

    def close(self):
        self._unreg_fd(self.fd)
        self.stream.close()
        self.flush()
        for sid in list(self.sid_to_fd):
            self.close_stream(sid)
        return super().close()

    def open(self, sid, *args, **kwargs):
        if sid in self.sid_to_fd:
            raise ValueError('stream already open')
        r, w = os.pipe()
        self.set_stream(sid, w)
        return os.fdopen(r, *args, **kwargs)

    def set_stream(self, sid, fileobj):
        if sid in self.sid_to_fd:
            raise ValueError('stream already open')
        fd = fileobj if isinstance(fileobj, int) else fileobj.fileno()
        self.sid_to_fd[sid] = fd
        self.fd_to_sid[fd] = sid

    def close_stream(self, sid):
        try:
            sfd = self.sid_to_fd.pop(sid)
        except KeyError:
            return
        self._close_fd(sfd)
        try:
            del self.fd_to_sid[sfd]
        except KeyError:
            pass

    def read_chunk_header(self, _):
        try:
            data = os.read(self.fd, CHUNK.size - len(self.buf))
        except OSError:
            data = b''
        if not data:
            return self.close()
        self.buf += data
        if len(self.buf) < CHUNK.size:
            return
        self.sid, self.size = CHUNK.unpack(self.buf)
        self.buf = b''
        if self.size == 0:
            self.close_stream(self.sid)
            if not self.sid_to_fd:
                return self.close()
            self.sid = None
            self.size = None
        else:
            self.selector.modify(
                self.fd, selectors.EVENT_READ, self.read_chunk)

    def read_chunk(self, _):
        try:
            data = os.read(self.fd, self.size - len(self.buf))
        except OSError:
            data = b''
        if not data:
            return self.close()
        self.buf += data
        if len(self.buf) < self.size:
            return
        try:
            sfd = self.sid_to_fd[self.sid]
        except KeyError:
            self.selector.modify(
                self.fd, selectors.EVENT_READ, self.read_chunk_header)
        else:
            self._unreg_fd(self.fd)
            self.selector.register(
                sfd, selectors.EVENT_WRITE, self.write_chunk)

    def write_chunk(self, key):
        try:
            bytes_written = os.write(key.fd, self.buf)
        except OSError:
            self.buf = b''
            self._close_fd(key.fd)
            try:
                sid = self.fd_to_sid.pop(key.fd)
            except KeyError:
                pass
            else:
                del self.sid_to_fd[sid]
            if not self.sid_to_fd:
                return self.close()
        else:
            self.buf = self.buf[bytes_written:]
        with self.flush_cond:
            if self.buf:
                return
            self.sid = None
            self.size = None
            self._unreg_fd(key.fd)
            self.selector.register(
                self.fd, selectors.EVENT_READ, self.read_chunk_header)
            self.flush_cond.notify_all()

    def flush(self):
        if self.shutdown:
            return
        with self.flush_cond:
            if self.buf:
                self.flush_cond.wait()


class MuxThread(IOThread):
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

    def close(self):
        for sid in list(self.sid_to_fd):
            self.close_stream(sid)
        self.flush()
        self._unreg_fd(self.fd)
        self.stream.close()
        return super().close()

    def open(self, sid, *args, **kwargs):
        if sid in self.sid_to_fd:
            raise ValueError('stream already open')
        r, w = os.pipe()
        self.set_stream(sid, r)
        writer = self.reader_to_writer[r] = os.fdopen(w, *args, **kwargs)
        return writer

    def set_stream(self, sid, fileobj):
        if sid in self.sid_to_fd:
            raise ValueError('stream already open')
        fd = fileobj if isinstance(fileobj, int) else fileobj.fileno()
        self.sid_to_fd[sid] = fd
        self.fd_to_sid[fd] = sid
        self.selector.register(
            fd, selectors.EVENT_READ, self.read_chunk)
        self.interrupt()

    def close_stream(self, sid):
        try:
            sfd = self.sid_to_fd.pop(sid)
        except KeyError:
            return
        self._close_fd(sfd)
        self.append_buf(sid, b'')
        try:
            del self.fd_to_sid[sfd]
        except KeyError:
            pass

    def append_buf(self, sid, data):
        self.buf += CHUNK.pack(sid, len(data)) + data
        if len(self.buf) > self.maxwrite:
            self.disable_read()
        self.enable_write()

    def read_chunk(self, key):
        try:
            sid = self.fd_to_sid[key.fd]
        except KeyError:
            return self._unreg_fd(key.fd)
        try:
            data = os.read(key.fd, self.maxread)
        except OSError:
            return self.close_stream(sid)
        if data:
            self.append_buf(sid, data)
        else:
            self.close_stream(sid)

    def write_chunk(self, _):
        try:
            bytes_written = os.write(self.fd, self.buf)
        except OSError:
            return self.close()
        with self.flush_cond:
            self.buf = self.buf[bytes_written:]
            if not self.read_enabled and len(self.buf) < self.maxwrite:
                self.enable_read()
            if self.buf:
                return
            self.flush_cond.notify_all()
        self._unreg_fd(self.fd)
        self.write_enabled = False

    def enable_read(self):
        self.read_enabled = True
        for fd in self.fd_to_sid:
            self.selector.register(
                fd, selectors.EVENT_READ, self.read_chunk)

    def disable_read(self):
        for fd in self.fd_to_sid:
            self._unreg_fd(fd)

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


def send_pickle(s, obj):
    d = pickle.dumps(obj)
    s.write(f'{len(d)}\n'.encode('ascii'))
    s.write(d)
    s.flush()


def read_pickle(s):
    try:
        remain = int(s.readline().strip())
    except ValueError:
        raise BrokenPipeError()
    buf = b''
    while remain:
        d = s.read(remain)
        if not d:
            raise BrokenPipeError()
        remain -= len(d)
        buf += d
    return pickle.loads(buf)


if __name__ == '__pype__':
    from importlib.abc import MetaPathFinder, Loader
    from importlib.util import module_from_spec


    class RemoteImporter(MetaPathFinder, Loader):
        sid = 250

        def __init__(self, demux, mux):
            self.in_stream = demux.open(self.sid, 'rb')
            self.out_stream = mux.open(self.sid, 'wb')

        def _req(self, method, *args):
            send_pickle(self.out_stream, (method, *args))
            e, ret = read_pickle(self.in_stream)
            if e:
                raise e
            return ret

        def find_spec(self, name, path, target=None):
            spec = self._req('find_spec', name, path)
            if spec:
                spec.loader = self
            return spec

        def get_source(self, fullname):
            return self._req('get_source', fullname)

        def get_code(self, fullname):
            source = self.get_source(fullname)
            return compile(source, f'<remote: {fullname}>', 'exec')

        def exec_module(self, module):
            code = self.get_code(module.__name__)
            exec(code, module.__dict__)

        def exec_main(self):
            spec = self.find_spec('__main__', None)
            module = module_from_spec(spec)
            self.exec_module(module)

        def close(self):
            self.out_stream.close()
            self.in_stream.close()


    sys.argv.pop(0)
    with MuxThread(sys.stdout) as mux:
        try:
            sys.stdout = mux.open(1, 'w')
            with DemuxThread(sys.stdin) as demux:
                sys.stdin = demux.open(0, 'r')
                try:
                    importer = RemoteImporter(demux, mux)
                    try:
                        sys.meta_path.append(importer)
                        importer.exec_main()
                    finally:
                        importer.close()
                        sys.stdout.close()
                        sys.stderr.close()
                finally:
                    demux.close()
        finally:
            mux.close()


elif __name__ == '__main__':
    import shlex
    import argparse
    import subprocess
    from importlib.util import find_spec, decode_source
    from importlib.machinery import ModuleSpec

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
        prog = decode_source(opts.prog.read())

    sys.path.insert(0, os.path.dirname(opts.prog.name))


    class ImportResponder:
        sid = 250

        def __init__(self, demux, mux, modules):
            self.in_stream = demux.open(self.sid, 'rb')
            self.out_stream = mux.open(self.sid, 'wb')
            self.modules = modules

        def find_spec(self, name, path):
            if name in self.modules:
                spec = ModuleSpec(name, None)
            else:
                spec = find_spec(name, path)
            if spec:
                spec.loader = None
            return spec

        def get_source(self, name):
            try:
                return self.modules[name]
            except KeyError:
                spec = find_spec(name)
                return spec.loader.get_source(name)

        def run(self):
            try:
                while True:
                    try:
                        method, *args = read_pickle(self.in_stream)
                    except BrokenPipeError:
                        return
                    try:
                        ret = getattr(self, method)(*args)
                    except Exception as e:
                        send_pickle(self.out_stream, (e, None))
                    else:
                        send_pickle(self.out_stream, (None, ret))
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
        with DemuxThread(p.stdout) as demux:
            try:
                demux.set_stream(1, sys.stdout)
                with MuxThread(p.stdin) as mux:
                    try:
                        mux.set_stream(0, sys.stdin)
                        ImportResponder(demux, mux, {
                            '__main__': prog,
                        }).run()
                    finally:
                        mux.close()
                sys.stderr.flush()
            finally:
                demux.close()

    sys.exit(p.returncode)
