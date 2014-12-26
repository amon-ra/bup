#!/usr/bin/env python
import os, sys, subprocess, struct
from bup import options,path,git
from bup.helpers import *
import time
import logging


import zerorpc



# try:
#     try:
#         f = getattr(self, method)
#     except AttributeError:
#         #self.thread_list[args[0]] es el hilo donde estan los datos
#         f = getattr(self.repository, method)
#     res = f(*args)
# except Exception as e:
#     s.send(msgpack.packb((1, msgid, e.__class__.__name__, e.args)))
# else:
#     s.send(msgpack.packb((1, msgid, None, res)))


# logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)
# ch = logging.StreamHandler()
# ch.setLevel(logging.INFO)
# logger.addHandler(ch)

def _check(w, expected, actual, msg):
    if expected != actual:
        w.abort()
        raise Exception(msg % (expected, actual))

class Buprpcd(object):



    def __init__(self):
        
        # self._door_open = False
        # self._lights_on = False

        # self.door_open_changed = Signal()
        # self.lights_on_changed = Signal()

        # self.color_changed = Signal()

        #self._pool = futures.ThreadPoolExecutor(max_workers=1)
        self._busy = None
        self.dumb_server_mode = False
        self.suspended_w = None
        self.suggested = set()
        self.cat_pipe = None   
        debug1("bup rpcd: init\n")     


    def do_help(self):
        return 'Commands:\n    %s\n' % '\n    '.join(sorted(commands))

    def _set_mode(self):
        self.dumb_server_mode = os.path.exists(git.repo('bup-dumb-server'))
        debug1('bup rpcd: serving in %s mode\n' 
               % (self.dumb_server_mode and 'dumb' or 'smart'))


    def _init_session(self,reinit_with_new_repopath=None):
        if reinit_with_new_repopath is None and git.repodir:
            return
        git.check_repo_or_die(reinit_with_new_repopath)
        # OK. we now know the path is a proper repository. Record this path in the
        # environment so that subprocesses inherit it and know where to operate.
        os.environ['BUP_DIR'] = git.repodir
        debug1('bup rpcd: bupdir is %r\n' % git.repodir)
        self._set_mode()


    def init_dir(self, arg):
        git.init_repo(arg)
        debug1('bup rpcd: bupdir initialized: %r\n' % git.repodir)
        self._init_session(arg)


    def set_dir(self, arg):
        self._init_session(arg)

    @zerorpc.stream        
    def list_indexes(self):
        self._init_session()
        suffix = ''
        if self.dumb_server_mode:
            suffix = ' load'
        for f in os.listdir(git.repo('objects/pack')):
            if f.endswith('.idx'):
                d= '%s%s' % (f, suffix)
                yield d

    @zerorpc.stream
    def send_index(self, name):
        self._init_session()
        debug2("rpcd send_index: " + str(name))
        assert(name.find('/') < 0)
        assert(name.endswith('.idx'))
        idx = git.open_idx(git.repo('objects/pack/%s' % name))
        #return len(idx.map),idx.map
        return idx.map


    def receive_objects_v2(self):
        self._busy = 'receive-objects-v2' 
        self._init_session()
        self.suggested = set()
        if self.suspended_w:
            self.w = self.suspended_w
            self.suspended_w = None
        else:
            if self.dumb_server_mode:
                self.w = git.PackWriter(objcache_maker=None)
            else:
                self.w = git.PackWriter()

    def send_last(self):
        debug2('bup rpcd: received %d object%s.\n' 
                    % (self.w.count, self.w.count!=1 and "s" or ''))
        fullpath = self.w.close(run_midx=not self.dumb_server_mode)
        self._busy = None
        if fullpath:
            (dir, name) = os.path.split(fullpath)
            return '%s.idx' % name
        return ''


    def send_cancel(self):
        #Cancelamos la conexion
        #elif n == 0xffffffff:
        debug2('bup rpcd: receive-objects suspended.\n')
        self.suspended_w = self.w
        self._busy = None

    def send_object(self,shar,crcr,buf):
        ret=''
        if crcr == 0:
            return self.send_last()
        #shar = conn.read(20)
        #crcr = struct.unpack('!I', conn.read(4))[0]
        #n -= 20 + 4
        #buf = conn.read(n)  # object sizes in bup are reasonably small
        #debug2('read %d bytes\n' % n)
        #_check(w, n, len(buf), 'object read: expected %d bytes, got %d\n')
        if not self.dumb_server_mode:
            oldpack = self.w.exists(shar, want_source=True)
            if oldpack:
                assert(not oldpack == True)
                assert(oldpack.endswith('.idx'))
                (dir,name) = os.path.split(oldpack)
                if not (name in self.suggested):
                    debug2("bup rpcd: suggesting index %s\n"
                           % git.shorten_hash(name))
                    debug2("bup rpcd:   because of object %s\n"
                           % shar.encode('hex'))
                    #conn.write('index %s\n' % name)
                    ret  = 'index %s' % name
                    self.suggested.add(name)
        nw, crc = self.w._raw_write((buf,), sha=shar)
        _check(self.w, crcr, crc, 'object read: expected crc %d, got %d\n')
        return ret


    def read_ref(self, refname):
        debug2("bup rpcd: read_ref")
        self._init_session()
        r = git.read_ref(refname)
        debug2("bup rpcd: %s" % r)
        return '%s' % (r or '').encode('hex')


    def update_ref(self, refname, newval, oldval):
        self._init_session()
        git.update_ref(refname, newval.decode('hex'), oldval.decode('hex'))

    @zerorpc.stream
    def cat(self, id):
        #global cat_pipe
        self._init_session()
        self._busy = 'cat' 
        if not self.cat_pipe:
            self.cat_pipe = git.CatPipe().join(id)
        return self.cat_pipe

    def cat_end (self):
        #conn.write(struct.pack('!I', len(blob)))
        self._busy= None
        self.cat_pipe = None


    def quit(self):
        pass    


s = zerorpc.Server(Buprpcd())
s.bind("tcp://0.0.0.0:4242")
s.run()

debug1("bup rpcd: done\n")
