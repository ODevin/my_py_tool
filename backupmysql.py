#!/usr/bin/env python3
import pymysql,time,datetime,sys,os,shutil,re,subprocess,argparse

class getParameter(object):
    def getmysqlParameter(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('backupdir', action='store',help='backup dir path')
        parser.add_argument('configurefile', action='store',help='my.cnf path')
        parser.add_argument('-A', action='store_true',default=False,dest='fullbackup',help='back up all data')
        parser.add_argument('-u', action='store',default="root",dest='user',help='mysql user,default value "root"')
        parser.add_argument('-p', action='store',default="",dest='passwd',help='mysql user password , default value is None')
        parser.add_argument('--host', action='store',default="localhost",dest='host',help='mysql server host')
        args,unkuown=parser.parse_known_args()
        return args

class readFile(object):
    def MysqlConfigureFile(self,configfile):
        configinfo={}
        for i in open(configfile):
            i=i.strip()
            if i.find("#")==-1 and len(i)!=0 and i.find("[")==-1:
                entry=i.split("=")
                if len(entry)==2:
                    configinfo[entry[0].strip()]=entry[1].strip()
                else:
                    configinfo["MYISVALUE"]=entry[0].strip()
            else:
                pass
        return configinfo #dict

    def MysqlBinlongIndexFile(self,filefile):
        filecontent=[]
        for i in open(filefile):
            i=i.strip()
            if len(i)!=0:
                filecontent.append(i)
            else:
                pass
        return filecontent #List

class backup(object):
    def backupMysqlBinLog(self,sourcefilelist,destinationdir,data): # sourcefilelist is type of list,destinationdir is a string
        if os.path.exists(destinationdir):
            for i in sourcefilelist:
                shutil.copy(i,os.path.join(destinationdir,os.path.basename(i)+"-"+data))
        else:
            os.makedirs(destinationdir)
            for i in sourcefilelist:
                shutil.copy(i,os.path.join(destinationdir,os.path.basename(i)+"-"+data))

    def backupMysqlAlldata(self,tool=None,backuptodir=None,socker=None,mysqlconfigurefile=None,mysqluser=None,userpassword=None,port=None,host=None):
        date_time = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M')
        newestdir = []
        if os.path.exists(tool) and re.search("[1|3|5|7]+",oct(os.stat(tool).st_mode)[-3:]) != None:
            if host!=None and userpassword!="":
                cmd=tool+" --defaults-file="+mysqlconfigurefile+" --user="+mysqluser+" --host="+host+" --password='"+userpassword+"' --port="+port+"  "+backuptodir
            elif host == None and userpassword!="":
                cmd=tool+" --defaults-file="+mysqlconfigurefile+" --user="+mysqluser+" --socket="+socker+" --password='"+userpassword+"' --port="+port+"  "+backuptodir
            elif host!=None and userpassword == "":
                cmd=tool+" --defaults-file="+mysqlconfigurefile+" --user="+mysqluser+" --host="+host+" --port="+port+"  "+backuptodir
            else:
                cmd=tool+" --defaults-file="+mysqlconfigurefile+" --user="+mysqluser+" --port="+port+"  "+backuptodir
            print("whil go %s"%cmd)
            subprocess.call(cmd,shell=True)
        else:
            pass
        if os.path.exists(backuptodir):
            pass
        else:
            os.makedirs(backuptodir)
        l = os.listdir(backuptodir)
        rulestr="("+date_time+")+"
        for i in l:
            if os.path.isdir(os.path.join(backuptodir,i)) and re.search(rulestr,i) != None:
                newestdir.append(i)
            else:
                pass
        if len(newestdir) != 0:
            for j in newestdir:
                cmd =tool+" --apply-log "+os.path.join(backuptodir,j)
                subprocess.call(cmd,shell=True)
                backupfile = os.path.join(backuptodir,date_time+".tar.gz")
                cmd = "tar zcvf "+backupfile+" -C "+backuptodir+" "+j
                subprocess.call(cmd,shell=True)
                shutil.rmtree(os.path.join(backuptodir,j))
        else:
            print(" Not a directory or is not existent,maybe "+tool+ " backup is fail!!!")
            sys.exit(4)

    def connAndbackupbinlog(self,cnffile=None,backupdir=None,host=None,user=None,password=None):
        readinfo = readFile()
        if "log-bin" in readinfo.MysqlConfigureFile(cnffile):
            mysqlbinlogindexfile = readinfo.MysqlConfigureFile(cnffile)["log-bin"]
        else:
            os.error("Mysql is not enabled binary log !!!")
            sys.exit(5)
        if "port" in readinfo.MysqlConfigureFile(cnffile):
            mysqlport = readinfo.MysqlConfigureFile(cnffile)["port"]
        else:
            mysqlport="3306"
        if "bind-address" in readinfo.MysqlConfigureFile(cnffile):
            bindadd = readinfo.MysqlConfigureFile(cnffile)["bind-address"]
        else:
            bindadd = host
        if "socket" in readinfo.MysqlConfigureFile(cnffile):
            socket = readinfo.MysqlConfigureFile(cnffile)["socket"]
        else:
            socket = "/tmp/mysql.sock"
        mysqlbinlogpath = os.path.dirname(mysqlbinlogindexfile)
        datetoday = datetime.datetime.now().strftime('%Y%m%d')
        flag=bindadd+"-"+mysqlport
        backupdir = os.path.join(backupdir,flag)
        binlogbackupdir = os.path.join(backupdir,"bin")
        if bindadd==None :
            conn = pymysql.connect(unix_socket=socket, port=int(mysqlport), user=user, passwd=password, db="mysql", charset="utf8")
        elif bindadd=="localhost" or bindadd=="127.0.0.1":
            conn = pymysql.connect(host=bindadd,unix_socket=socket, port=int(mysqlport), user=user, passwd=password, db="mysql", charset="utf8")
        else:
            conn = pymysql.connect(host=bindadd, port=int(mysqlport), user=user, passwd=password, db="mysql", charset="utf8")
        cur = conn.cursor()
        cur.execute("flush logs")
        time.sleep(3)

        allbinlogfile = readinfo.MysqlBinlongIndexFile(mysqlbinlogindexfile)
        exceptlastbinlogfilelist = allbinlogfile[0:len(allbinlogfile)-1]
        lastbinlogfile = allbinlogfile[-1]
        lastbinlogfilename = os.path.basename(lastbinlogfile)
        self.backupMysqlBinLog(exceptlastbinlogfilelist,binlogbackupdir,datetoday)
        time.sleep(3)
        cur.execute("purge binary logs to '" + lastbinlogfilename + "'")
        conn.close()

    def connAndbackupalldata(self,tool=None,backupdir=None,mysqlconfigurefile=None,mysqluser=None,userpassword=None,host=None):
        readinfo = readFile()
        if "port" in readinfo.MysqlConfigureFile(mysqlconfigurefile):
            mysqlport = readinfo.MysqlConfigureFile(mysqlconfigurefile)["port"]
        else:
            mysqlport="3306"
        if "bind-address" in readinfo.MysqlConfigureFile(mysqlconfigurefile):
            bindadd = readinfo.MysqlConfigureFile(mysqlconfigurefile)["bind-address"]
        else:
            bindadd = host
        if "socket" in readinfo.MysqlConfigureFile(mysqlconfigurefile):
            socket = readinfo.MysqlConfigureFile(mysqlconfigurefile)["socket"]
        else:
            socket = "/tmp/mysql.sock"
        flag = bindadd + "-" + mysqlport
        datetoday = datetime.datetime.now().strftime('%Y%m%d')
        backupdir = os.path.join(backupdir,flag)
        self.backupMysqlAlldata(tool=tool,backuptodir=backupdir,mysqlconfigurefile=mysqlconfigurefile,mysqluser=mysqluser,userpassword=userpassword,port=mysqlport,host=host,socker=socket)

#===================================================================================================
parameter=getParameter()
args=parameter.getmysqlParameter()
BU=backup()
if args.fullbackup:
    BU.connAndbackupalldata(tool="/usr/bin/innobackupex",backupdir=args.backupdir,mysqlconfigurefile=args.configurefile,mysqluser=args.user,userpassword=args.passwd,host=args.host)
else:
    BU.connAndbackupbinlog(backupdir=args.backupdir,cnffile=args.configurefile, user=args.user, password=args.passwd,host=args.host)
