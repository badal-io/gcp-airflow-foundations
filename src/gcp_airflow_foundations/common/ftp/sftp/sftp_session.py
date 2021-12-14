import paramiko
import socket
import os
from stat import S_ISDIR

class SFTPSession(object):
    def __init__(self,hostname,username='root',key_file=None,password=None):
        transport = paramiko.Transport((hostname, 22))
        mykey = paramiko.RSAKey.from_private_key_file(key_file)
        transport.connect(username= username, pkey=mykey)
        self.sftp = paramiko.SFTPClient.from_transport(transport)
        
    def command(self,cmd):
        #  Breaks the command by lines, sends and receives 
        #  each line and its output separately
        #
        #  Returns the server response text as a string
        
        chan = self.t.open_session()
        chan.get_pty()
        chan.invoke_shell()
        chan.settimeout(20.0)
        ret=''
        try:
            ret+=chan.recv(1024)
        except:
            chan.send('\n')
            ret+=chan.recv(1024)
        for line in cmd.split('\n'):
            chan.send(line.strip() + '\n')
            ret+=chan.recv(1024)
        return ret

    def get_stats(self, remotefile):
        return self.sftp.stat(remotefile)

    def put(self,localfile,remotefile):
        #  Copy localfile to remotefile, overwriting or creating as needed.
        self.sftp.put(localfile,remotefile)
    
    def remotepath_join(self,*args):
        #  Bug fix for Windows clients, we always use / for remote paths
        return '/'.join(args)
    
    
    def put_all(self,localpath,remotepath):
        #  recursively upload a full directory
        os.chdir(os.path.split(localpath)[0])
        parent=os.path.split(localpath)[1]
        for path,_,files in os.walk(parent):
            try:
                self.sftp.mkdir(self.remotepath_join(remotepath,path))
            except:
                pass
            for filename in files:
                self.put(os.path.join(path,filename),self.remotepath_join(remotepath,path,filename))
    
    def get(self,remotefile,localfile):
        #  Copy remotefile to localfile, overwriting or creating as needed.
        self.sftp.get(remotefile,localfile)
    
    def sftp_walk(self,remotepath):
        # Kindof a stripped down  version of os.walk, implemented for 
        # sftp.  Tried running it flat without the yields, but it really
        # chokes on big directories.
        path=remotepath
        files=[]
        folders=[]
        for f in self.sftp.listdir_attr(remotepath):
            if S_ISDIR(f.st_mode):
                folders.append(f.filename)
            else:
                files.append(f.filename)
        yield path,folders,files
        for folder in folders:
            new_path=self.remotepath_join(remotepath,folder)
            for x in self.sftp_walk(new_path):
                yield x
        
    def get_all(self,remotepath,localpath):
        #  recursively download a full directory
        #  Harder than it sounded at first, since paramiko won't walk
        #
        # For the record, something like this would generally be faster:
        # ssh user@host 'tar -cz /source/folder' | tar -xz
        
        self.sftp.chdir(os.path.split(remotepath)[0])
        parent=os.path.split(remotepath)[1]
        try:
            os.mkdir(localpath)
        except FileExistsError:
            pass
        for path,_,files in self.sftp_walk(parent):
            try:
                os.mkdir(self.remotepath_join(localpath,path))
            except FileExistsError:
                pass
            for filename in files:
                print(self.remotepath_join(path,filename),os.path.join(localpath,path,filename))
                self.get(self.remotepath_join(path,filename),os.path.join(localpath,path,filename))
    def write_command(self,text,remotefile):
        #  Writes text to remotefile, and makes remotefile executable.
        #  This is perhaps a bit niche, but I was thinking I needed it.
        #  For the record, I was incorrect.
        self.sftp.open(remotefile,'w').write(text)
        self.sftp.chmod(remotefile,755)