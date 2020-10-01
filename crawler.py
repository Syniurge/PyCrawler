#! /usr/bin/env python3
# -*- coding: utf8 -*-

import os
import sys
import argparse
import queue
import urllib.parse, urllib.request
import multiprocessing as mp
from html.parser import HTMLParser
from PIL import Image

#from whoosh import index
#from whoosh.fields import Schema, TEXT, KEYWORD, ID, STORED
#from whoosh.analysis import StemmingAnalyzer

class CrawlerParser(HTMLParser):
    def __init__(self, q, ongoing_tasks, ongoing_tasks_lock, crawled_files, shared, baseurl, depth):
        super().__init__()
        self.q = q
        self.ongoing_tasks = ongoing_tasks
        self.ongoing_tasks_lock = ongoing_tasks_lock
        self.crawled_files = crawled_files
        self.shared = shared
        self.baseurl = baseurl
        self.depth = depth

    def handle_starttag(self, tag, attrs):
        task_args = None

        if tag == 'a' and self.depth < self.shared['max_depth']:
            for _, href in filter(lambda attr: attr[0] == 'href', attrs):
                task_args = (self.ongoing_tasks, self.ongoing_tasks_lock, self.crawled_files, self.shared, href, self.baseurl, self.depth)
        elif tag == 'img':
            for _, src in filter(lambda attr: attr[0] == 'src', attrs):
                task_args = (self.ongoing_tasks, self.ongoing_tasks_lock, self.crawled_files, self.shared, src, self.baseurl, self.depth, True)

        if task_args is not None:
            self.ongoing_tasks_lock.acquire()
            self.ongoing_tasks.value += 1
            self.ongoing_tasks_lock.release()

            self.q.put(task_args)

    #def handle_data(self, data):
        #ix = index.open_dir(shared['index_dir'])
        #writer = ix.writer()


def _crawl_url(q, ongoing_tasks, ongoing_tasks_lock, crawled_files, shared, urlstr, baseurl = None, depth = 0, is_img = False):
    urlstr = bytes(urlstr, "utf-8").decode("unicode_escape").strip().strip('"').strip('\'') # pour enlever les \n et ", ' de certains <a>
    #print ("URL = {}".format(urlstr), urllib.parse.urlparse(urlstr))

    if baseurl is None:
        baseurl = urlstr
    else:
        urlnetloc = urllib.parse.urlparse(urlstr).netloc
        if urlnetloc != '' and urlnetloc != urllib.parse.urlparse(baseurl).netloc:
            shared['num_external_links'] += 1
            return  # différent domaine
        urlstr = urllib.parse.urljoin(baseurl, urlstr)

    url = urllib.parse.urlparse(urlstr)
    if url.scheme not in ['http', 'https']:
        return

    shared['num_internal_links'] += 1

    basename = os.path.basename(url.path)
    if basename == "": basename = "index.html"
    dirname = os.path.dirname(url.path[1:])

    out_filename = os.path.join(dirname, basename)
    if out_filename in crawled_files:
        return
    crawled_files[out_filename] = True

    print("[{}] Téléchargement {}...".format(depth, urlstr))

    if dirname != '':
        try:
            os.makedirs(dirname)
        except FileExistsError:
            pass

    for attempt in range(5):
        try:
            with urllib.request.urlopen(urlstr, timeout=10) as u:
                data = u.read()
            break
        except urllib.error.URLError as e:
            if attempt < 4:
                print("Erreur à l'ouverture de {}, nouvel essai...".format(urlstr))
                pass
            elif isinstance(e, urllib.error.HTTPError) and e.code >= 400:
                print("Erreur HTTP {}, lien ignoré".format(e.code))
                return
            else:
                print("Erreur fatale(?): {}".format(e.reason))
                raise

    with open(out_filename, 'wb') as f:
        f.write(data)

    if not is_img:
        parser = CrawlerParser(q, ongoing_tasks, ongoing_tasks_lock, crawled_files, shared, urlstr, depth + 1)
        parser.feed(str(data))
    elif is_img:
        im = Image.open(out_filename)

        def human_size(bytes, units=['B','KB','MB']):
            return str(bytes) + units[0] if bytes < 1024 else human_size(bytes>>10, units[1:])

        print("[{}] Image info: ".format(depth), basename, im.format, human_size(os.stat(out_filename).st_size), im.size, im.mode)

def crawl_url(q, ongoing_tasks, ongoing_tasks_lock, crawled_files, shared, urlstr, baseurl = None, depth = 0, is_img = False):
    try:
        _crawl_url(q, ongoing_tasks, ongoing_tasks_lock, crawled_files, shared, urlstr, baseurl, depth, is_img)
    except Exception as e:
        print(e)

    ongoing_tasks_lock.acquire()
    ongoing_tasks.value -= 1
    ongoing_tasks_lock.release()

#def crawl_index():
    #while ongoing_tasks.value != 0:

def main():
    p = argparse.ArgumentParser(description='My first Web crawler')
    p.add_argument('url', type=str, help='URL à aspirer')
    p.add_argument('--output-dir', type=str, help='Répertoire de sortie', default='OUT')
    p.add_argument('--max-depth', type=int, help='Profondeur maximale', default=5)
    #p.add_argument('--index-dir', type=int, help='Répertoire d\'indexation', default="index")
    args = p.parse_args()

    #args.index_dir = os.path.abspath(args.index_dir) # avant le chdir()

    try:
        os.makedirs(args.output_dir)
        #os.makedirs(args.index_dir)
    except FileExistsError:
        pass
    os.chdir(args.output_dir)

    #schema = Schema(title=TEXT(stored=True, analyzer=StemmingAnalyzer()),
                #desc=TEXT(stored=True, analyzer=StemmingAnalyzer()),
                #body=TEXT(analyzer=StemmingAnalyzer()))
    #index.create_in(args.index_dir, schema)

    with mp.Manager() as manager:
        q, q_ix = manager.Queue(), manager.Queue()
        ongoing_tasks = manager.Value('i', 1)
        ongoing_tasks_lock = manager.Lock()
        crawled_files = manager.dict()

        shared = manager.dict()
        shared['max_depth'] = args.max_depth
        shared['num_internal_links'] = 0
        shared['num_external_links'] = 0

        #p_ix = Process(target=crawl_index, args=(q_ix, ongoing_tasks, args.index_dir))
        #p_ix.start()

        with mp.Pool() as pool:
            q.put((ongoing_tasks, ongoing_tasks_lock, crawled_files, shared, args.url))

            while ongoing_tasks.value != 0:
                #print(ongoing_tasks)
                try:
                    cargs = q.get(timeout=0.2)
                    #crawl_url(q, *cargs)
                    pool.apply_async(crawl_url, (q, *cargs))
                except queue.Empty:
                    pass

        #p_ix.join()

        print("\nFin du crawling sur {}".format(args.url))
        print(" == Liens internes : {}".format(shared['num_internal_links']))
        print(" == Liens externes : {}".format(shared['num_external_links']))


if __name__ == "__main__":
    sys.exit(main())
