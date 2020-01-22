# !/usr/bin/python
# -*- coding: utf-8 -*-
import os
import logging
import time
from ftplib import FTP
from datetime import datetime, timedelta

from utils import path_manager

logger = logging.getLogger('ftpsync')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
fh = logging.FileHandler(path_manager.LOCAL_LOG_FILE_DIR + 'ftpsync.log')
fh.setLevel(logging.WARNING)
FORMAT = '%(asctime)s -%(name)s-%(levelname)s-%(module)s:%(message)s'
fomatter = logging.Formatter(FORMAT)
fh.setFormatter(fomatter)
logger.addHandler(fh)
logger.addHandler(ch)


def ftpconnect(host, username, password):
    ftp = FTP()
    # ftp.set_debuglevel(2)
    ftp.connect(host, 21)
    ftp.login(username, password)
    return ftp

# def checkAvailableDateRemote(back_periods=7):
#     ftp = ftpconnect(path_manager.FTP_IP_ADDRESS, "", "")
#     ftp_file_list = ftp.nlst(path_manager.FTP_DATA_STORAGE)
#     now = datetime.now()
#     # datestring = now.strftime('%Y-%m-%d')
#     target_dir = []
#     for i in range(back_periods):
#         timespan = timedelta(days=i)
#         previous_day = now - timespan
#         pre_day = previous_day.strftime('%Y-%m-%d')
#         cur_dir = path_manager.FTP_DATA_STORAGE + pre_day
#         if cur_dir in ftp_file_list:
#             all_files = ftp.nlst(cur_dir)
#             if (cur_dir + '/finish') in all_files:
#                 target_dir.append(cur_dir)
#     ftp.quit()
#     return target_dir
#
# # 鼓励用FTP_File_Transfer的download方法
# def download_ftp_ver(version):
#     local_target_dir = path_manager.LOCAL_ENV_DATA_STORAGE + version + '/'
#     remote_target_dir = path_manager.FTP_DATA_STORAGE + version
#     # 如果没有这个目录就新建
#     if not os.path.isdir(local_target_dir):
#         os.makedirs(local_target_dir)
#     ftps = ftpconnect(path_manager.FTP_IP_ADDRESS, "", "")
#     # 建立安全的数据连接，之后才能返回数据。
#     ftps.cwd(remote_target_dir)
#     files = ftps.nlst()
#     # 进入本地目录
#     os.chdir(local_target_dir)
#     # 循环下载每个文件
#     count = 1
#     for file in files:
#         fp = open(file, 'wb')
#         ftps.retrbinary('RETR %s' % file, fp.write)
#         if count % 100 == 0:
#             time.sleep(1) #seconds
#             logger.info(str(count) + ' : ' + file)
#         count += 1
#     ftps.close()
#     # 本地二次检查是否完成同步
#     if os.path.exists(local_target_dir + 'finish'):
#         logger.info('sync successfully! version: ' + version)
#         return True
#     else:
#         logger.warn('sync failed! version: ' + version)
#         return False


class FTP_File_Transfer(object):
    '''
    用于上传和下载FTP服务器上的文件夹
    '''

    def __init__(self):
        self.ftp = None

    def __del__(self):
        pass

    def setFtpParams(self, ip, uname, pwd, port=21, timeout=60):
        self.ip = ip
        self.uname = uname
        self.pwd = pwd
        self.port = port
        self.timeout = timeout

    def initEnv(self):
        if self.ftp is None:
            self.ftp = FTP()
            logger.info('### connect ftp server: %s ...' % self.ip)
            self.ftp.connect(self.ip, self.port, self.timeout)
            self.ftp.login(self.uname, self.pwd)
            logger.info(self.ftp.getwelcome())

    def clearEnv(self):
        if self.ftp:
            self.ftp.close()
            logger.info('### disconnect ftp server: %s!' % self.ip)
            self.ftp = None

    def uploadDir(self, localdir, remotedir):
        if not os.path.isdir(localdir):
            return
        self.ftp.cwd(remotedir)
        for file in os.listdir(localdir):
            src = os.path.join(localdir, file)
            if os.path.isfile(src):
                self.uploadFile(src, file)
            elif os.path.isdir(src):
                try:
                    self.ftp.mkd(file)
                except:
                    logger.error('the dir is exists %s' % file)
                self.uploadDir(src, file)
        self.ftp.cwd('..')

    def uploadFile(self, localpath, remotepath):
        if not os.path.isfile(localpath):
            return
        logger.info('+++ upload %s to %s:%s' % (localpath, self.ip, remotepath))
        self.ftp.storbinary('STOR ' + remotepath, open(localpath, 'rb'))

    def __filetype(self, src):
        #print(src)
        if os.path.isfile(src):
            index = src.rfind('\\')
            if index == -1:
                index = src.rfind('/')
            return 'FILE', src[index + 1:]
        elif os.path.isdir(src):
            return 'DIR', ''
        else:
            index = src.rfind('\\')
            if index == -1:
                index = src.rfind('/')
            return 'DIR', src[index + 1:]

    def upload(self, serverDir, localDir , version):
        #filetype, filename = self.__filetype(localDir)
        self.initEnv()
        all_dirs = serverDir.split('/')
        for thedir in all_dirs:
            if thedir == '.':
                continue
            if thedir == '':
                continue
            dirlists = self.ftp.nlst()
            if thedir not in dirlists:
                logger.info('create server directory %s!' % thedir)
                self.ftp.mkd(thedir)
            self.ftp.cwd(thedir)
        files = self.ftp.nlst()
        if version not in files:
            logger.info('create server directory %s!' % version)
            self.ftp.mkd(version)
        self.srcDir = localDir
        self.uploadDir(self.srcDir+version, version)
        self.clearEnv()

    def downloadDir(self, dirname):
        # 如果没有这个目录就新建
        if dirname not in os.listdir('./'):
            os.makedirs(dirname)
        os.chdir(dirname)
        self.ftp.cwd(dirname)
        logger.info("change diretocry into " + dirname + ' to download....')
        filelines = []
        self.ftp.dir(filelines.append)
        filelines_bk = self.ftp.nlst()
        i = 0
        for file in filelines:
            if 'd' in file.split()[0]:
                self.downloadDir(filelines_bk[i])
                self.ftp.cwd('..')
                os.chdir('..')
                logger.info("back to upper directory to downlaod....")
            else:

                fd = open(filelines_bk[i], 'wb')
                self.ftp.retrbinary('RETR %s' % filelines_bk[i], fd.write)
                fd.close()
                logger.info(filelines_bk[i] + ' download done....')
            i += 1

    def download(self, serverDir, localDir, version, verification_flag=False):
        local_target_dir = localDir + version + '/'
        self.initEnv()
        os.chdir(localDir)
        self.ftp.cwd(serverDir)
        self.downloadDir(version)
        self.clearEnv()

        if verification_flag:
            # 本地二次检查是否完成同步
            if os.path.exists(local_target_dir + 'finish'):
                logger.info('sync successfully! version: ' + version)
                return True
            else:
                logger.warn('sync failed! version: ' + version)
                return False
        else:
            logger.info('sync successfully! version: ' + version)


def upload_datafiles(serverDir, localDir, version):
    fft = FTP_File_Transfer()
    fft.setFtpParams(path_manager.FTP_IP_ADDRESS, '', '')
    fft.upload(serverDir, localDir, version)
    return True


def upload_pictures(serverDir, localDir, version):
    fft = FTP_File_Transfer()
    fft.setFtpParams(path_manager.ALI_FTP_IP_ADDRESS, 'u3d_data', 'u3d12345678')
    fft.upload(serverDir, localDir, version)


if __name__ == "__main__":
    # 获取ftp上最近n天的已完成下载的数据集合，返回以日期命名的文件夹名
    xx = checkAvailableDateRemote(15)
    print(xx)
    # 从指定路径获取整个文件夹下的所有文件
    '''
    for temp_dir in xx:
        temp_dir = temp_dir.split('/')[-1]
        download_ftp_ver(temp_dir)
    '''

    version = '2018-03-06'
    srcDir = path_manager.LOCAL_CLIENT_DATA_STORAGE + version
    serverDir = path_manager.FTP_CLIENT_DATA_STORAGE + 'version/ttt/wew/'
    localDir = path_manager.LOCAL_CLIENT_DATA_STORAGE
    fft = FTP_File_Transfer()
    fft.setFtpParams('192.168.0.133', '', '')
    fft.upload(serverDir, localDir, version)
    #fft.download(serverDir, localDir, version)
    #fft.upload(srcFile)


