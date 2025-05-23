import os
import io
import sys
import types
import pickle
import threading
import selectors
from importlib.machinery import ModuleSpec
from importlib.abc import MetaPathFinder, Loader
from importlib.util import module_from_spec, find_spec, decode_source


def bootstrap(stream):
    m = types.ModuleType(pickle.load(stream))
    c = pickle.load(stream)
    exec(compile(c, '<stdin>', 'exec'), m.__dict__)


def set_nonblocking(obj):
    if not isinstance(obj, int):
        obj = obj.fileno()
    os.set_blocking(obj, False)


def close_all(files):
    for f in files:
        if isinstance(f, int):
            try:
                os.close(f)
            except OSError:
                pass
        else:
            f.close()


def pp():
    return os.getpid()


class NeedMore(Exception):
    pass


class PartialUnpickler(pickle.Unpickler):
    def __init__(self, file, *args, **kwargs):
        self.__file_read1 = file.read1
        self.__partial_buf = io.BytesIO()
        super().__init__(self.__partial_buf, *args, **kwargs)

    def load(self):
        data = self.__file_read1(4096)
        if data is None:
            raise NeedMore
        if not data:
            raise EOFError
        self.__partial_buf.write(data)
        if pickle.STOP[0] in data:
            self.__partial_buf.seek(0)
            obj = super().load()
            after = self.__partial_buf.read()
            self.__partial_buf.seek(0)
            self.__partial_buf.write(after)
            self.__partial_buf.truncate()
            return obj
        raise NeedMore


class IntCtx:
    def __init__(self, interruptor, fd):
        self.interruptor = interruptor
        self.fd = fd

    def fileno(self):
        return self.fd

    def __enter__(self):
        return self

    def __exit__(self, typ, val, tb):
        self.interruptor.unregister()


class Interruptor:
    def __init__(self):
        self.fds = {}
        self.mutex = threading.Lock()

    def interrupt(self):
        with self.mutex:
            for r, w in self.fds.values():
                os.write(w, b'x')

    def register(self):
        with self.mutex:
            r, w = self.fds[threading.get_ident()] = os.pipe()
        return IntCtx(self, r)

    def unregister(self):
        with self.mutex:
            fds = self.fds.pop(threading.get_ident())
        for fd in fds:
            try:
                os.close(fd)
            except OSError:
                pass

    def ack(self):
        os.read(self.fds[threading.get_ident()][0], 1)


class MuxDemux:
    def __init__(self, muxin, muxout):
        self._shutdown = False
        self._interruptor = Interruptor()
        self._thread_died = threading.Condition()

        self.muxin = muxin
        self.demux_thread = None
        self._demux_fds = None
        self._demux_mutex = threading.Lock()

        self.muxout = muxout
        self.mux_thread = None
        self._mux_selector = None
        self._mux_cond = threading.Condition()

    def close(self):
        self._shutdown = True
        self.interrupt()

    def interrupt(self):
        self._interruptor.interrupt()

    def mux_open(self, sid):
        r, w = os.pipe()
        self.set_mux(sid, open(r, 'rb'))
        return w

    def set_mux(self, sid, fd):
        with self._mux_cond:
            if not self._mux_selector:
                raise OSError('mux is not running')
            self._mux_selector.register(fd, selectors.EVENT_READ, sid)
            self._mux_cond.notify_all()
            self.interrupt()

    def unset_mux(self, fd):
        with self._mux_cond:
            if not self._mux_selector:
                raise OSError('mux is not running')
            try:
                self._mux_selector.unregister(fd)
            except (ValueError, KeyError):
                pass
            else:
                self._mux_cond.notify_all()
                self.interrupt()

    def run_mux(self):
        try:
            pickler = pickle.Pickler(self.muxout)
            with self._interruptor.register() as int_fd:
                with self._mux_cond:
                    selector = self._mux_selector = selectors.DefaultSelector()
                    selector.register(int_fd, selectors.EVENT_READ, self._interruptor)
                try:
                    while not self._shutdown:
                        for key, _ in selector.select():
                            if self._shutdown:
                                break
                            sid = key.data
                            if sid is self._interruptor:
                                self._interruptor.ack()
                                continue
                            f = key.fileobj
                            data = f.read1(4096)
                            try:
                                pickler.dump((sid, data))
                                self.muxout.flush()
                            except BrokenPipeError:
                                return
                            if not data:
                                with self._mux_cond:
                                    selector.unregister(f)
                                    self._mux_cond.notify_all()
                                f.close()
                finally:
                    with self._mux_cond:
                        close_all(selector.get_map())
                        self._mux_cond.notify_all()
                        self._mux_selector = None
                    selector.close()
        finally:
            with self._thread_died:
                self.mux_thread = None
                self._thread_died.notify_all()

    def demux_open(self, sid):
        r, w = os.pipe()
        self.set_demux(sid, open(w, 'wb'))
        return r

    def set_demux(self, sid, fd):
        with self._demux_mutex:
            if self._demux_fds is None:
                raise OSError('demux is not running')
            self._demux_fds[sid] = fd

    def run_demux(self):
        try:
            unpickler = PartialUnpickler(self.muxin)
            with self._demux_mutex:
                self._demux_fds = {}
            set_nonblocking(self.muxin)
            selector = selectors.DefaultSelector()
            selector.register(self.muxin, selectors.EVENT_READ)
            try:
                with self._interruptor.register() as int_fd:
                    selector.register(int_fd, selectors.EVENT_READ, self._interruptor)
                    while not self._shutdown:
                        for key, _ in selector.select():
                            if self._shutdown:
                                break
                            if key.data is self._interruptor:
                                self._interruptor.ack()
                                continue
                            try:
                                sid, data = unpickler.load()
                            except NeedMore:
                                continue
                            except EOFError:
                                return
                            try:
                                out = self._demux_fds[sid]
                            except KeyError:
                                # got data for unknown sid. maybe we should warn here.
                                continue
                            if data:
                                out.write(data)
                                out.flush()
                            else:
                                with self._demux_mutex:
                                    try:
                                        del self._demux_fds[sid]
                                    except KeyError:
                                        pass
                                out.close()
            finally:
                with self._demux_mutex:
                    close_all(self._demux_fds.values())
                    self._demux_fds = None
                selector.close()
        finally:
            with self._thread_died:
                self.demux_thread = None
                self._thread_died.notify_all()

    def wait_for_mux(self):
        with self._mux_cond:
            while self._mux_selector and any(
                k.data is not self._interruptor
                for k in self._mux_selector.get_map().values()
            ):
                self._mux_cond.wait()

    def __enter__(self):
        with self._thread_died:
            self.mux_thread = threading.Thread(target=self.run_mux)
            self.demux_thread = threading.Thread(target=self.run_demux)
        self.mux_thread.start()
        self.demux_thread.start()
        return self

    def __exit__(self, typ, val, tb):
        self.wait_for_mux()
        self.close()
        self.join()

    def join(self):
        with self._thread_died:
            while self.mux_thread or self.demux_thread:
                self._thread_died.wait()


class RemoteImporter(MetaPathFinder, Loader):
    sid = 250

    def __init__(self, muxdemux):
        self.muxdemux = muxdemux
        self.in_stream = open(muxdemux.demux_open(self.sid), 'rb')
        self.out_stream = open(muxdemux.mux_open(self.sid), 'wb')
        self.pickle_in = pickle.Unpickler(self.in_stream)
        self.pickle_out = pickle.Pickler(self.out_stream)

    def _req(self, method, *args):
        self.pickle_out.dump((method, *args))
        self.out_stream.flush()
        e, ret = self.pickle_in.load()
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


class ImportResponder:
    sid = 250

    def __init__(self, muxdemux, modules):
        self.muxdemux = muxdemux
        self.in_stream = open(muxdemux.demux_open(self.sid), 'rb')
        self.out_stream = open(muxdemux.mux_open(self.sid), 'wb')
        self.pickle_in = pickle.Unpickler(self.in_stream)
        self.pickle_out = pickle.Pickler(self.out_stream)
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
                    method, *args = self.pickle_in.load()
                except (BrokenPipeError, EOFError):
                    return
                try:
                    ret = getattr(self, method)(*args)
                except Exception as e:
                    self.pickle_out.dump((e, None))
                else:
                    self.pickle_out.dump((None, ret))
                self.out_stream.flush()
        finally:
            self.out_stream.close()
            self.in_stream.close()


if __name__ == '__pype__':
    sys.argv.pop(0)
    with MuxDemux(sys.stdin.buffer, sys.stdout.buffer) as m:
        sys.stdin = open(m.demux_open(0), 'r')
        sys.stdout = open(m.mux_open(1), 'w')
        importer = RemoteImporter(m)
        try:
            sys.meta_path.append(importer)
            importer.exec_main()
            sys.stdout.flush()
        finally:
            importer.close()
            sys.stdout.close()


if __name__ == '__main__':
    import __main__
    import subprocess
    import argparse
    import inspect
    import shlex

    CMD = '\n'.join(line.strip() for line in inspect.getsourcelines(bootstrap)[0][1:])
    CMD = CMD.replace('stream', 'sys.stdin.buffer')
    CMD = f'import sys, types, pickle\n{CMD}'
    PYPE = inspect.getsource(__main__)

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
        'args', nargs='*',
        help='Arguments to pass to remote program.'
    )

    opts = parser.parse_args(sys.argv[1:])

    if opts.args:
        if opts.args[0].startswith('-'):
            if opts.args[0] == '-c':
                opts.main = opts.args.pop(1)
            else:
                parser.error('unknown option')
        else:
            with open(opts.args[0], 'rb') as f:
                opts.main = decode_source(f.read())
    else:
        opts.main = 'import code; code.interact()'

    args = ' '.join(shlex.quote(a) for a in [
        opts.python, '-c', CMD
    ])
    args = [opts.ssh, opts.connect, args]
    args.extend(shlex.quote(a) for a in opts.args)
    with subprocess.Popen(
        args,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
    ) as p:
        pickle.dump('__pype__', p.stdin)
        pickle.dump(PYPE, p.stdin)
        p.stdin.flush()
        with MuxDemux(p.stdout, p.stdin) as m:
            m.set_mux(0, sys.stdin.buffer)
            m.set_demux(1, sys.stdout.buffer)
            ImportResponder(m, {'__main__': opts.main}).run()
            m.unset_mux(sys.stdin.buffer)
