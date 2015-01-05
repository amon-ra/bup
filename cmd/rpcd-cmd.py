#!/usr/bin/env python
import os, sys, subprocess, struct, multiprocessing
from bup import options,path,git
from bup.helpers import *
import time
import logging
import collections

import udt
import gevent
import zmq.green as zmq
import cbor

gevent.server.socket = udt.socket


PACKETSIZE=34
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


class RpcServerUdt(object):

    def __init__(self, obj, threads=10, max_bandwidth=1024*52*10):
        self.methods = self._parse_methods(obj)
        # self.ctx = zmq.Context()
        # self.socket = self.ctx.socket(zmq.REP)
        self.socket = udt.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        self.thread_count = 0
        self.threads_num = threads
        self.clients = None
        self.epoll = None
        self.max_bandwidth =max_bandwidth



    def _parse_methods(self, obj):
        methods = {}
        for method in dir(obj):
            if not method.startswith('_'):
                methods[method] = getattr(obj, method)
        for method in dir(self):
            if not method.startswith('_'):
                methods[method] = getattr(self, method)

        return methods

    def bind(self, *args):
        #self.socket.bind(*args)
        try:
            self.socket.setsockopt(0, udt.UDT_REUSEADDR, True)
            self.socket.setsockopt(0, udt.UDT_SNDSYN, False)
#           self.udtserversock.setsockopt(0, udt.UDT_RCVSYN, False)
            self.socket.bind(*args)
            self.socket.listen(10)
        except socket.error:
            print 'UDT server socket bind error'            
        self.thread_count = 0
        self.clients = {}
        self.epoll = udt.epoll()
        for x in xrange(0, self.threads_num):
            child, host = self.socket.accept()
            child.setsockopt(0, udt.UDT_MAXBW, self.max_bandwidth)
            self.clients[child.fileno()] = child
            self.epoll.add_usock(child.fileno(), udt.UDT_EPOLL_IN)        

    def _recv(self):
        return msgpack.unpackb(self.socket.recv())

    def _send(self, msg, req, pid, sec, enc=0):
        #calcular el crc
        size = len(msg)
        #H1 Cabecera (No encriptada): Identificacion del client,secuencia,Encriptacion (ninguna,cabeceras,cabeceras + datos, tamaño h2, tamaño total,crc)
        header=cbor.dumps(struct.pack("!IIHIII",pid,sec,self.version,enc,size,crc))
        self.clients[req].send(header,0) 
        self.clients[req].send(msg,0) 

    def _stream(self,req,pid,sec,gen_pid,packetsize,size,enc=0):
        try:
            msg=self.gens[gen_pid].next()
            size=len(msg)
        except:
            #Last packet
            size=0
        h2=cbor.dumps([size,size,"RAW",gen_pid])
        self._send(h2,req, pid, sec ,enc)
        if size > 0:
            self.clients[req].send(msg) 

    #close socket if size is 0
    def _istop(self,h2size=0):
        if h2size = 0:
            self.clients[i].send("OK", 0)
            self.clients[i].close()
            self.epoll.remove_usock(i)
            del self.clients[i]
            return True
        return False

    def _getData(self,req,size,bufsize=8096):
        i=bufsize
        while i <= size:
            yield self.clients[req].recv(bufsize,0)
            i+=bufsize
        rest = size % bufsize
        if rest > 0: 
            yield self.clients[req].recv(rest,0)

    def _route (self):
        return cbor.dumps([0,0,"ROUTE","NEW ip:port"])

    def _response(self,method,args):
        if method = "STREAM":
            self._stream(req,pid,sec,packetsize,size)
            return True
        return False

    def _process2(self,req,h2size,crcr,enc=False):
        try:
            packetsize,size,method,args = cbor.loads(self.clients[req].recv(h2size,0))
        except:
            #must send the error
            return
        #test crc
        #generate an iterator for the data
        if self._response():
            return
        if packetsize > 0: 
            data = self._getData(req,size,packetsize)
        args.append(data)
        try:
            rtn = self.methods[method](*args)
        except:
            #si se provoca una route exception devolvemos ROUTE
            rtn=self._route()
        if isinstance(rtn, collections.Iterable) and (not isinstance(rtn, collections.Sized)):
            #is an generator
            gen_pid=urandom(24)
            while gen_pid in self.gens:
                gen_pid=urandom(24)
            self.gens[gen_pid]=rtn
            rtn=cbor.dumps([0,0,"STREAM",gen_pid])
        else:
            rtn=cbor.dumps([0,0,"RET",rtn])

        self._send(rtn,req,pid,sec)
        #Send the data ruturns

    #Recivimos los parametros del packete de cabecera: Tamaño de buffer. Tamaño de la cabezera del paquete
    #El formato de la cabezera sera: tamaño(1),sha(20),crc(8)
    #en esta primera version no se utiliza un buffer sino que se lee el fich completo
    #H1 Cabecera (No encriptada): Identificacion del client,secuencia,Encriptacion (ninguna,cabeceras,cabeceras + datos, tamaño h2, tamaño total,crc)
    #H2 (puede ir encriptada) :  tamaño de buffer, tamaño de data , methodo, argumentos 
    #El crc es calculado uniendo H1 y H2
    def _process(self, req):
        # header, method, args = req
        # rtn = self.methods[method](*args)
        # h = {'response_to': 0, 'message_id':0, 'v': 3}
        # return (h, 'OK', [rtn])
        try:
            pid,sec,version,enc,h2size,crcr = cbor.loads(self.clients[req].recv(PACKETSIZE,0))
            #Finalizamos si el tamaño es excesivo (para evitar ataques)
            if h2size > MAX_PACKETSIZE:
                h2size =0
            if self._istop(h2size):
                return None
        except:
            return self._istop()
        return gevent.spawn(self._process2,req,h2size,crcr,enc)
        # text = self.clients[i].recv(1024, 0)
        # print text

        # if text != "OK":
        #     gevent.spawn(handle,self.clients[i])
        #     #p = Thread(target=test, args=(clients[i], text))
        #     #p.start()
        #     poll.append(p)
        # else:
        #     #print text
        #     self.clients[i].send("OK", 0)
        #     self.clients[i].close()
        #     self.epoll.remove_usock(i)
        #     del clients[i]

    def run(self):
        # while True:
        #     req = self._recv()
        #     rep = self._process(req)
        #     self._send(rep)
        poll=[]
        while True:
            print 'wait..'
            sets = self.epoll.epoll_wait(-1)
            #print sets
            poll = []
            for i in sets[0]:
                p= self._process(i)
                if p:
                    poll.append(p)
            if not clients:
                self.epoll.release()
                break
        try:
            gevent.joinall(poll)
        except:
            pass

class RpcClientUdt(object):

    def __init__(self,enc=0,version=0,sec=0):
        self.socket = udt.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        self.slist = {}
        self.gen = None
        self.pid = 0
        self.sec = sec
        self.enc = enc
        self.version = version

    def __call__(self, method, *args):
        self._send(method, args)
        h, status, rtn = self._recv()
        return rtn[0]


    def _sendmore(self,socket,size,msg):
        #packetsize,size,method,args = self._sockrecv(socket)
        if size = 0:
            try:
                while True:
                    socket.send(msg.next(),0)
            except StopIteration:
                pass
        else:
            i=self.packetsize
            while i <= size:
                socket.send(msg,self.packetsize)
                i+=self.packetsize
            rest = size % self.packetsize
            if rest > 0: 
                socket.send(msg,self.packetsize)
            

    def _send(self, method,args, msg=None):
        size=0
        psize=0
        try:
            socket=self.slist[method]
        except:
            socket=self.socket        
        if isinstance(msg, collections.Iterable) and (not isinstance(msg,collections.Sized)):
            psize = self.packetsize
        elif msg:
            size = len(msg)
            psize = self.packetsize
        h2=cbor.dumps([psize,size,method,args])
        #calcular el crc
        crc=0
        size = len(h2)
        #H1 Cabecera (No encriptada): Identificacion del client,secuencia,Encriptacion (ninguna,cabeceras,cabeceras + datos, tamaño h2, tamaño total,crc)
        header=cbor.dumps(struct.pack("!IIHIII",self.pid,self.sec,self.version,self.enc,size,crc))
        socket.send(header,0) 
        socket.send(h2,0) 
        if psize>0:
            self._sendmore(socket,size,msg)

    def _sockrecv(self,socket):
        try:
            pid,sec,version,enc,h2size,crcr = cbor.loads(socket.recv(PACKETSIZE,0))
            #Finalizamos si el tamaño es excesivo (para evitar ataques)
            if h2size > MAX_PACKETSIZE:
                h2size =0
            if self._istop(h2size):
                return None
            packetsize,size,method,args = cbor.loads(socket.recv(h2size,0))

        except:
            return self._istop()
        return packetsize,size,method,args 

    def _recv(self,method=None):
        try:
            socket=self.slist[method]
        except:
            socket=self.socket
        packetsize,size,method,args = self._sockrecv(socket)

    def connect(self, *args):
        self.socket.connect(*args)


class BupRpcd(object):


    def __init__(self):
        
        # self._door_open = False
        # self._lights_on = False

        # self.door_open_changed = Signal()
        # self.lights_on_changed = Signal()

        # self.color_changed = Signal()

        #self._pool = futures.ThreadPoolExecutor(max_workers=1)
        self._busy = None
        self.dumb_server_mode = False
        self.suspended_w = False
        self.self.suggested = set()
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

    #returns a list    
    def list_indexes(self):
        self._init_session()
        suffix = ''
        ret=[]
        if self.dumb_server_mode:
            suffix = ' load'
        for f in os.listdir(git.repo('objects/pack')):
            if f.endswith('.idx'):
                ret.append('%s%s' % (f, suffix))

    #returns a list
    def send_index(self, name):
        self._init_session()
        debug2("rpcd send_index: " + str(name))
        assert(name.find('/') < 0)
        assert(name.endswith('.idx'))
        idx = git.open_idx(git.repo('objects/pack/%s' % name))
        #return len(idx.map),idx.map
        #return a list of strings
        return idx.map


    def receive_objects_v2(self):
        self._busy = 'receive-objects-v2' 
        self._init_session()
        self.self.suggested = set()
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
                if not (name in self.self.suggested):
                    debug2("bup rpcd: suggesting index %s\n"
                           % git.shorten_hash(name))
                    debug2("bup rpcd:   because of object %s\n"
                           % shar.encode('hex'))
                    #conn.write('index %s\n' % name)
                    ret  = 'index %s' % name
                    self.self.suggested.add(name)
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
        if not self.cat_pipe and not self._busy:
            self._init_session()
            self._busy = 'cat' 
            self.cat_pipe = (id,git.CatPipe().join(id))

        

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

def server_zmq():
    ctx = zmq.Context()
    file = open("testdata", "r")

    router = ctx.socket(zmq.ROUTER)

    router.bind("tcp://*:6000")

    while True:
        # First frame in each message is the sender identity
        # Second frame is "fetch" command
        try:
            msg = router.recv_multipart()
        except zmq.ZMQError as e:
            if e.errno == zmq.ETERM:
                return   # shutting down, quit
            else:
                raise

        identity, command, offset_str, chunksz_str = msg

        assert command == b"fetch"

        offset = int(offset_str)
        chunksz = int(chunksz_str)

        # Read chunk of data from file
        file.seek(offset, os.SEEK_SET)
        data = file.read(chunksz)

        # Send resulting chunk to client
        router.send_multipart([identity, data])


#class TransferServer(DatagramServer):
#     __q = gevent.queue.Queue()
#     __request_processing_greenlet = gevent.spawn(process_request,__q)

#     def handle(self,data,address):
#         self.socket.sendto(data,address)


# TransferServer(':5670').serve_forever()

#Format: Command,size,sha,token. enc:https://pypi.python.org/pypi/randenc/0.1
def get_command(f, sock):
    ns = conn.read(4)
            if not ns:
                w.abort()
                raise Exception('object read: expected length header, got EOF\n')
            n = struct.unpack('!I', ns)[0]    




class BupCommandServer():

    def __init__(self):
        self.cat_pipe = None
        self.suspended_w = None
        self.conn = None
        self.dumb_server_mode = None
        self.suspended_w = None
        self.buffer={}
        self._init_session()
        if self.dumb_server_mode:
            self.w = git.PackWriter(objcache_maker=None)
        else:
            self.w = git.PackWriter()

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


    def _proccess (self,method):
        return getattr(self,method.strip())

    def con(self,conn):

        while True:
            try:
                #Debemos comprobar el crcr
                data = cbor.loads(conn.read(PACKETSIZE))
                name = data[0]
                argsize=struct.unpack('!I',data[1])
                totalsize=struct.unpack('!I',data[2])
                crcr=struct.unpack('!I',data[3])
                args = cbor.loads(conn.read(argsize))

            except:
                return     
            f = _proccess(name)
            crc = f(args)
            if zlib.crc32(args) != crcr:
                return 


    def err(self,msg):
        pass

    def cat(self,id):
        if not self.conn:
            return
        if not self.cat_pipe:
            self.cat_pipe = git.CatPipe()
        try:
            for blob in self.cat_pipe.join(id):
                self.conn.write(struct.pack('!I', len(blob)))
                self.conn.write(blob)
        except KeyError, e:
            log('server: error: %s\n' % e)
            self.conn.write('\0\0\0\0')
            self.conn.error(e)
        else:
            self.conn.write('\0\0\0\0')
            self.conn.ok()

    def _check(self, expected, actual, msg):
        if expected != actual:
            self.w.abort()
            self.err(msg % (expected, actual))

    #Recivimos los parametros del packete de cabecera: Tamaño de buffer. Tamaño de la cabezera del paquete
    #El formato de la cabezera sera: tamaño(1),sha(20),crc(8)
    #en esta primera version no se utiliza un buffer sino que se lee el fich completo
    def receive_objects_v2(self,bufsize,pos,shar=None):
        #UTilizamos un socket udp con un CRC añadido encriptado.
        self.suggested = set()
        while not self.suspended_w:

            buf = conn.read(bufsize)  # object sizes in bup are reasonably small
            #debug2('read %d bytes\n' % n)
            self._check(bufsize, len(buf), 'object read: expected %d bytes, got %d\n')
            try:
                self.buffer[shar][pos]=buf
            except:
                #first item create buffer
                self.buffer[shar]=[None]*pos
            if pos <= 0:
                if not dumb_server_mode:
                    oldpack = self.w.exists(shar, want_source=True)
                    if oldpack:
                        assert(not oldpack == True)
                        assert(oldpack.endswith('.idx'))
                        (dir,name) = os.path.split(oldpack)
                        if not (name in self.suggested):
                            debug1("bup server: suggesting index %s\n"
                                   % git.shorten_hash(name))
                            debug1("bup server: because of object %s\n"
                                   % shar.encode('hex'))
                            conn.write('index %s\n' % name)
                            self.suggested.add(name)
                        continue
                #w, crc = self.w._raw_write((''.join(self.buffer[shar]),), sha=shar)
                self._check_crc(crcr, crc, 'object read: expected crc %d, got %d\n')

        #End of transfer
        #w, crc = self.w._raw_write((''.join(self.buffer[shar]),), sha=shar)
        self._check_crc(crcr, crc, 'object read: expected crc %d, got %d\n')        
        debug1('bup server: received %d object%s.\n' 
            % (self.w.count, self.w.count!=1 and "s" or ''))
        fullpath = self.w.close(run_midx=not dumb_server_mode)
        if fullpath:
            (dir, name) = os.path.split(fullpath)
            conn.write('%s.idx\n' % name)
        conn.ok()
        return
        # elif n == 0xffffffff:
        #     debug2('bup server: receive-objects suspended.\n')
        #     suspended_w = w
        #     conn.ok()
        #     return
        # NOTREACHED


class UdtServer:
    def __init__(self,obj):
        #self.methods = self._parse_methods(obj)
        self.srv = obj


    def _parse_methods(self, obj):
        methods = {}
        for method in dir(obj):
            if not method.startswith('_'):
                methods[method] = getattr(obj, method)

        return methods

    def _process(self, conn, method, args):
        #header, method, args = req
        self.methods['con'](conn)
        rtn = self.methods[method](*args)
        #h = {'response_to': 0, 'message_id':0, 'v': 3}
        return rtn

    #Formato: [metodo(22 caracteres),tamaño de argumentos, crc]
    def serve(self,sock, client_addr):
        conn = sock.makefile()
        self.srv.con(sock,client_addr)
        #name has a fixed size we strip to obtain method name
        #self._process(conn,name,args)



s = udt.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
s.bind(("127.0.0.1", 5555))
s.listen(10)
i = 0
clients = {}
epoll = udt.epoll()









multiprocessing.Process(target=server_udp)



