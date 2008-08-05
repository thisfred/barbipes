#! /usr/bin/env python2.5
# all database threading code by Wim Schut, copied from here:
# http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/496799

import sys, os
import urllib2, threading
import urlparse
import sqlite3
import random
import Queue, time, thread
from datetime import datetime, timedelta
from threading import Thread
import re

from torvalddj.config import configuration
from torvalddj.utils import log_stdout, url_to_filename

_threadex = thread.allocate_lock()
qthreads = 0
sqlqueue = Queue.Queue()
urlqueue = Queue.Queue()

ConnectCmd = "connect"
SqlCmd = "SQL"
StopCmd = "stop"
threads = 4

class DbCmd:
    def __init__(self, cmd, params=None):
        self.cmd = cmd
        self.params = params

class DbWrapper(Thread):
    def __init__(self, path, nr):
        Thread.__init__(self)
        self.path = path
        self.nr = nr
    def run(self):
        global qthreads
        con = sqlite3.connect(self.path)
        cur = con.cursor()
        while True:
            s = sqlqueue.get()
            #print "Conn %d -> %s -> %s" % (self.nr, s.cmd, s.params)
            if s.cmd == SqlCmd:
                commitneeded = False
                res = []
                # s.params is a list to bundle statements into a "transaction"
                for sql in s.params:
                    cur.execute(sql[0],sql[1])
                    if not sql[0].upper().startswith("SELECT"): 
                        commitneeded = True
                    for row in cur.fetchall(): res.append(row)
                if commitneeded: con.commit()
                s.resultqueue.put(res)
            else:
                _threadex.acquire()
                qthreads -= 1
                _threadex.release()
                # allow other threads to stop
                sqlqueue.put(s)
                s.resultqueue.put(None)
                break

def execSQL(s):
    if s.cmd == ConnectCmd:
        global qthreads
        _threadex.acquire()
        qthreads += 1
        _threadex.release()
        wrap = DbWrapper(s.params, qthreads)
        wrap.start()
    elif s.cmd == StopCmd:
        s.resultqueue = Queue.Queue()
        sqlqueue.put(s)
        # sleep until all threads are stopped
        while qthreads > 0: time.sleep(0.1)
    else:
        s.resultqueue = Queue.Queue()
        sqlqueue.put(s)
        return s.resultqueue.get()

def get_page(url, log):
    """Retrieve URL and return comments, log errors."""
    try:
        page = urllib2.urlopen(url)
        body = page.read()
        page.close()
    except:
        log("Error retrieving: " + url)
        return ''
    return body

def find_links(html):
    """return a list of links in HTML"""
    links = re.compile('<[aA][^>]*[hH][rR][eE][fF]=["\'](.*?)["\'][^>]*>')
    result = links.findall(html)
    print "found %s links" % len(result) 
    return result

def is_download_link(link):
    extensions = ['mp3', 'ogg']
    return link.split('.')[-1].lower() in extensions
    
def should_ignore(link):
    extensions = [
        'jpg', 'jpeg', 'gif', 'png', 'pdf', 'rar', 'zip', 'mov', 'mp4',
        'avi', 'mpg', 'mpeg', 'm4a']
    return link.split('.')[-1].lower() in extensions

def alternate_urls(url):
    alts = [url]
    if '://www.' in url:
        alts.append(url.replace('://www.', '://'))
    else:
        alts.append(url.replace('://', '://www.'))
    a = url.split('/')
    if '%' in url:
        try:
            alts.append('/'.join((a[:-1] + [urllib2.unquote(a[-1])])))
        except:
            pass
    else:
        try:
            alts.append('/'.join((a[:-1] + [urllib2.quote(a[-1])])))
        except:
            pass
    return alts

def create_db():
    """ Set up the database
    """
    connection = sqlite3.connect(configuration.database)
    cursor = connection.cursor()
    cursor.execute(
        'CREATE TABLE blog_urls (blog_id INTEGER PRIMARY KEY AUTOINCREMENT, title VARCHAR(200), url VARCHAR(200), updated DATE, last_seen DATE, banned BOOLEAN, score INTEGER)')
    cursor.execute(
        'CREATE TABLE file_urls (file_id INTEGER PRIMARY KEY AUTOINCREMENT, blog_id INTEGER, url VARCHAR(300), downloaded BOOLEAN, invalid BOOLEAN, tagged BOOLEAN, duplicate BOOLEAN, purged BOOLEAN)')
    cursor.execute(
	'CREATE VIEW blog_stats AS SELECT b.url AS url, (SELECT COUNT(*) FROM file_urls AS f WHERE f.blog_id = b.blog_id) AS total, (SELECT COUNT(*) FROM file_urls AS f WHERE f.blog_id = b.blog_id AND f.invalid = 1 AND f.duplicate ISNULL) AS invalid, (SELECT COUNT(*) FROM file_urls AS f WHERE f.blog_id = b.blog_id AND f.invalid ISNULL AND f.downloaded = 1 AND f.tagged ISNULL) AS untaged FROM blog_urls AS b ORDER BY total ASC, invalid ASC')
    connection.commit()


class Spider(Thread):
    """
    The heart of this program, finds all links within a web site.

    process_page() retrieves each page and finds the links.
    """
    def __init__(self, queue, action, thread, max_depth=1):
        self.max_depth = max_depth
        self.action = action
        self.queue = queue
        self.thread = thread
        Thread.__init__(self)
        self.log = log_stdout

    def run(self):
        if self.action == 'spider':
            while True:
                #grabs host from queue
                self.process_url(self.queue.get())            
                #signals to queue job is done
                self.queue.task_done()
            return
        if self.action == 'download':
            while True:
                #grabs host from queue
                self.download_file(self.queue.get())            
                #signals to queue job is done
                self.queue.task_done()
            return

    def set_start_url(self, url_id, url):
        self.URLs = set()
        self.start_url = url
	self.start_url_id = url_id
        self.URLs.add(url)
        self._links_to_process = [(url, 0)]

    def insert_blog_url(self):
        row = execSQL(DbCmd(
            SqlCmd,
            [("SELECT * FROM blog_urls WHERE url = ?", (self.start_url,))]))
        if row:
            print "T%s: blog already added" % self.thread
            return
        execSQL(DbCmd(
            SqlCmd,
            [("INSERT INTO blog_urls (url) VALUES (?)", (self.start_url,))]))

    def insert_file_url(self, url, downloaded=False):
        row = execSQL(DbCmd(SqlCmd,
            [("SELECT * FROM file_urls WHERE url = ?", (url,))]))
        if row:
            print "T%s: file already added" % self.thread
            return
        execSQL(DbCmd(SqlCmd,
            [("INSERT INTO file_urls (blog_id, url, downloaded) VALUES"
	      "(? ,?, ?)", (self.start_url_id, url, downloaded))]))
	execSQL(DbCmd(SqlCmd,
            [("UPDATE blog_urls SET last_seen = DATETIME('now') WHERE blog_id = ?",
	      (self.start_url_id,))]))
        
    def update_blog_url(self):
        execSQL(DbCmd(SqlCmd,
            [("UPDATE blog_urls SET updated = DATETIME('now')  WHERE blog_id = ?",
            (self.start_url_id,))]))
        
    def url_exists(self, url):
        for alt in alternate_urls(url):
            row = execSQL(DbCmd(SqlCmd,[(
                "SELECT * FROM file_urls WHERE url = ?", (alt,))]))
            if row:
                return True
        return False
         
    def downloaded_file_url(self, url):
        execSQL(DbCmd(SqlCmd,[(
            "UPDATE file_urls SET downloaded = 1 WHERE url = ?", (url,))]))

    def process_url(self, start_url):
        #process list of URLs one at a time
        print "STARTING T%s, maxdepth %s" % (self.thread, self.max_depth)
	self.set_start_url(*start_url)
        self.update_blog_url()
        while self._links_to_process:
            url, url_depth = self._links_to_process.pop()
            self.log("T%s: Retrieving: %s - %s " % (
                self.thread, url_depth, url))
            self.process_page(url, url_depth)
        
    def url_in_site(self, link):
        #checks weather the link starts with the base URL
        try:
            return link.startswith(self.start_url)
        except UnicodeDecodeError:
            return False

    def process_page(self, url, depth):
        #retrieves page and finds links in it
        html = get_page(url, self.log)
        new_depth = depth + 1
        for link in find_links(html):
            #handle relative links
            try:
                link = urlparse.urljoin(url,link)
            except:
                continue
            if '#' in link:
                link = link.split("#")[0]
            if '?' in link:
                link = link.split("?")[0]
            #make sure this is a new URL within current site
            if link in self.URLs:
                continue
            if should_ignore(link):
                continue
            if is_download_link(link):
                if self.url_exists(link):
                    self.URLs.add(link)
                    continue
                self.log("T%s: adding %s" %(self.thread, link))
                self.URLs.add(link)
                self.insert_file_url(link)
            elif (new_depth <= self.max_depth and self.url_in_site(link)):
                self.URLs.add(link)
                self._links_to_process.append((link, new_depth))
            time.sleep(0.1)

    def download_file(self, url):
	url_ascii = url.encode('ascii', 'replace')
        print "T%s: %s" % (self.thread, url_ascii)
        if not url.startswith("http://") and not url.startswith("https://"):
            print "T%s: weird link %s" % (self.thread, url_ascii)
	    self.downloaded_file_url(url)
            return
        for alt in alternate_urls(url):
            if os.path.exists(url_to_filename(alt)):
                print "T%s: already there" % self.thread
		self.downloaded_file_url(url)
                return
        try:
            os.popen(
                u'curl -s --max-time 600 --connect-timeout 10 --user-agent'
		u' "Mozilla/5.0" --create-dirs --globoff --max-filesize'
		u' 30000000 -o "%s" "%s"' % (url_to_filename(url), url))
        except:
            pass
	self.downloaded_file_url(url)

def get_blog_urls():
    week = timedelta(7)
    last_week = datetime.now() - week
    year = last_week.year
    month = last_week.month
    day = last_week.day
    urls = execSQL(DbCmd(SqlCmd, [
        ("SELECT blog_id, url FROM blog_urls WHERE updated < '%04d-%02d-%02d' AND banned ISNULL ORDER BY updated;" % (year, month, day),
         ())]))
    print "%s blogs to harvest" % str(len(urls))
    return urls
    
def download_files(n=100):
    """Download some files.
    """
    execSQL(DbCmd(ConnectCmd, configuration.database))
    for i in range(threads):
        s = Spider(urlqueue, 'download', str(i))
        s.setDaemon(True)
        s.start()
    all_files = execSQL(DbCmd(
        SqlCmd,
        [("SELECT url FROM file_urls WHERE downloaded = ?;", (False,))]))
    random.shuffle(all_files)
    for row in all_files[:n]:
        urlqueue.put(row[0])
    urlqueue.join()
    execSQL(DbCmd(StopCmd))

def check_files(fix=False, invalidate=False):
    """Check that all files have been downloaded.
    """
    connection = sqlite3.connect(configuration.database)
    cursor = connection.cursor()
    cursor.execute("SELECT url FROM file_urls WHERE downloaded = 1 AND invalid ISNULL")
    missing = 0
    for entry in cursor.fetchall():
	url = entry[0]
	filename = url_to_filename(url)
	if not os.path.exists(filename):
	    log_stdout(u"Missing file %s - url %s" % (filename, url))
	    missing += 1
	    if fix:
		cursor.execute("UPDATE file_urls SET downloaded = 0 WHERE url = ?", (url,))
	    elif invalidate:
		cursor.execute("UPDATE file_urls SET invalid = 0 WHERE url = ?", (url,))
    log_stdout("%d missing files." % missing)
    if fix or invalidate:
	connection.commit()
    connection.close()

def clean_files():
    """Delete invalid files.
    """
    connection = sqlite3.connect(configuration.database)
    cursor = connection.cursor()
    cursor.execute("SELECT url FROM file_urls WHERE invalid = 1 AND purged ISNULL")
    purged = 0
    for entry in cursor.fetchall():
	url = entry[0]
	filename = url_to_filename(url)
	try:
	    if os.path.exists(filename):
		os.remove(filename)
	    cursor.execute("UPDATE file_urls SET purged = 1 WHERE url = ?", (url,))
	    purged += 1
	    os.removedirs(os.path.dirname(filename))
	except OSError:
	    pass
    log_stdout("%d files purged." % purged)
    connection.commit()
    connection.close()
        
def add_blog(url):
    """Add a blog if it's not already exists.
    """
    log_stdout("adding %s" % url)
    connection = sqlite3.connect(configuration.database)
    cursor = connection.cursor()
    cursor.execute(
        "SELECT * FROM blog_urls WHERE url = ?", (url,))
    row = cursor.fetchone()
    if row:
        log_stdout("blog already added")
        return
    cursor.execute("INSERT INTO blog_urls (url, updated) VALUES"
                   "(?, '2001-01-01')", (url,))
    connection.commit()
    connection.close()

def list_blogs():
    connection = sqlite3.connect(configuration.database)
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM blog_stats")
    entries = cursor.fetchall()
    for entry in entries:
	url, nb_file, nb_invalid, nb_notagged = entry
	display = u'blog - %s, files - %d' % (url, nb_file,)
	flags = U''
	if nb_notagged:
	    flags += u'+'
	    display += u'\n   no tag  - % 3d / % 3d%%' % \
		       (nb_notagged, (nb_notagged * 100.0) / nb_file)
	else:
	    flags += u' '
	if nb_invalid:
	    flags += u'*'
	    display += u'\n   invalid - % 3d / % 3d%%' % \
		       (nb_invalid, (nb_invalid * 100.0) / nb_file)
	else:
	    flags += u' '
	log_stdout(flags + display)
    log_stdout("%d blogs." % len(entries))
    cursor.execute("SELECT COUNT(*) FROM file_urls")
    total_file = cursor.fetchone()
    cursor.execute("SELECT COUNT(*) FROM file_urls WHERE downloaded = 1")
    downloaded_file = cursor.fetchone()
    cursor.execute("SELECT COUNT(*) FROM file_urls WHERE invalid = 1")
    invalid_file = cursor.fetchone()
    log_stdout("%s files, %s downloaded, %s invalids." %
	       (total_file[0], downloaded_file[0], invalid_file[0]))
    cursor.execute("SELECT COUNT(*) FROM file_urls WHERE tagged = 1")
    valid_file = cursor.fetchone()
    log_stdout("%s valid files may have been found." % valid_file[0])
    connection.close()

def delete_blog(url):
    """Delete a blog.
    """
    connection = sqlite3.connect(configuration.database)
    cursor = connection.cursor()
    cursor.execute("DELETE FROM blog_urls WHERE url = ?", (url,))
    connection.commit()
    connection.close()

def undo():
    """Un-download a file.
    """
    connection = sqlite3.connect(configuration.database)
    cursor = connection.cursor()
    undos = open("undo.txt", "r")
    for undo in undos.readlines():
        cursor.execute(
            "UPDATE file_urls SET downloaded = 0 WHERE url = ?",
            ("http://" + undo.strip(),))
        log_stdout(undo.strip())
        connection.commit()
    undos.close()
    connection.close()

def spider(depth=1):
    """Search for URLs.
    """
    execSQL(DbCmd(ConnectCmd, configuration.database))
    for i in range(threads):
        s = Spider(urlqueue, 'spider', str(i), max_depth=depth)
        s.setDaemon(True)
        s.start()
    for url in get_blog_urls():
        urlqueue.put(url)
    urlqueue.join()
    execSQL(DbCmd(StopCmd))
            
if __name__ == '__main__':
    if len(sys.argv) < 2:
        spider()
    elif sys.argv[1] == 'quick':
        spider(depth=0)
    elif sys.argv[1] == 'download':
        if len(sys.argv) > 2:
            download_files(int(sys.argv[2]))
        else:
            download_files()
    elif sys.argv[1] == 'createdb':
        create_db()
    elif sys.argv[1] == 'add':
        add_blog(sys.argv[2])
    elif sys.argv[1] == 'delete':
	delete_blog(sys.argv[2])
    elif sys.argv[1] == 'list':
	list_blogs()
    elif sys.argv[1] == 'check':
	fix = False
	invalidate = False
	if len(sys.argv) == 3:
	    if sys.argv[2] == 'fix':
		fix = True
	    elif sys.argv[2] == 'invalidate':
		invalidate = True
	check_files(fix, invalidate)
    elif sys.argv[1] == 'undo':
        undo()
    elif sys.argv[1] == 'clean':
	clean_files()
	
