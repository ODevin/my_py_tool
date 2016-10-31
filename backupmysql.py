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
        parser.add_argument('--host', action='store',default="",dest='host',help='mysql server host')
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

    def backupMysqlAlldata(self,tool=None,backuptodir=None,socker=None,mysqlconfigurefile=None,mysqluser=None,userpassword=None,port=None):
        date_time = datetime.datetime.now().strftime('%Y-%m-%d_%H')
        newestdir = ""
        if os.path.exists(tool) and re.search("[1|3|5|7]+",oct(os.stat(tool).st_mode)[-3:]) != None:
            cmd=tool+" --defaults-file="+mysqlconfigurefile+" --user="+mysqluser+" --password='"+userpassword+"' --port="+port+"  "+backuptodir
            subprocess.call(cmd,shell=True)
        else:
            pass
        l = os.listdir(backuptodir)
        rulestr="("+date_time+")+"
        for i in l:
            if os.path.isdir(os.path.join(backuptodir,i)) and re.search(rulestr,i) != None:
                newestdir = i
        cmd =tool+" --apply-log "+os.path.join(backuptodir,newestdir)
        subprocess.call(cmd,shell=True)
        backupfile = os.path.join(backuptodir,date_time+".tar.gz")
        cmd = "tar zcvf "+backupfile+" -C "+backuptodir+" "+newestdir
        subprocess.call(cmd,shell=True)

    def connAndbackupbinlog(self,cnffile,backupdir,host,user,password):
        readinfo = readFile()
        mysqlbinlogindexfile = readinfo.MysqlConfigureFile(cnffile)["log-bin"]
        mysqlport = readinfo.MysqlConfigureFile(cnffile)["port"]
        mysqlbinlogpath = os.path.dirname(mysqlbinlogindexfile)
        datetoday = datetime.datetime.now().strftime('%Y%m%d')
        backupdir = os.path.join(backupdir,mysqlport)
        binlogbackupdir = os.path.join(backupdir,"bin")

        conn = pymysql.connect(host=host, port=mysqlport, user=user, passwd=password, db="mysql", charset="utf8")
        cur = conn.cursor()
        cur.execute("flush logs")
        conn.close()
        time.sleep(3)

        allbinlogfile = readinfo.MysqlBinlongIndexFile(mysqlbinlogindexfile)
        exceptlastbinlogfilelist = allbinlogfile[0:len(allbinlogfile)-1]
        lastbinlogfile = allbinlogfile[-1]
        lastbinlogfilename = os.path.basename(lastbinlogfile)
        self.backupMysqlBinLog(exceptlastbinlogfilelist,binlogbackupdir,datetoday)
        time.sleep(3)
        conn = pymysql.connect(host=host, port=port, user=user, passwd=password, db="mysql", charset="utf8")
        cur = conn.cursor()
        cur.execute("purge binary logs to '" + lastbinlogfilename + "'")
        conn.close()

    def connAndbackupalldata(self,tool,backupdir,mysqlconfigurefile,mysqluser,userpassword,host):
        readinfo = readFile()
        mysqlport = readinfo.MysqlConfigureFile(mysqlconfigurefile)["port"]
        datetoday = datetime.datetime.now().strftime('%Y%m%d')
        backupdir = os.path.join(backupdir,mysqlport)
        self.backupMysqlAlldata(tool=tool,backuptodir=backupdir,mysqlconfigurefile=mysqlconfigurefile,mysqluser=mysqluser,userpassword=userpassword,port=mysqlport)

#===================================================================================================
parameter=getParameter()
args=parameter.getmysqlParameter()
# print(args)
if args.fullbackup:
    backup.connAndbackupalldata(tool="/usr/bin/innobackupex",backupdir=args.backupdir,mysqlconfigurefile=args.configurefile,mysqluser=args.user,userpassword=args.passwd,host=args.host)
else:
    backup.connAndbackupbinlog(backupdir=args.backupdir,
                               cnffile=args.configurefile, user=args.user, password=args.passwd,
                                host=args.host)