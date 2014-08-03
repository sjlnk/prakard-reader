import logging
import urllib
import urllib.error
import urllib.request
import re
import azlib as az
import azlib.azlogging
import pickle
import os.path
import argparse
import pandas as pd
from datetime import timedelta


def topic_dict_to_string(tdict):
    dt = tdict['date_added'].astimezone(az.get_localtz()).strftime('%Y-%m-%d %H:%M')
    s = "\t{}: {} - {}\n\t\t{}".format(dt, tdict['header'], tdict['thread_starter'], tdict['href'])
    return s

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="prakard-reader")

    vgroup = parser.add_mutually_exclusive_group()
    vgroup.add_argument("-v", action="count", default=0, help="verbosity")
    vgroup.add_argument("--quiet", action="store_true", help="disable stdout-output")

    parser.add_argument("--cutoffhours", type=float, default=24, help='cutoff hours')

    args = parser.parse_args()

    az.azlogging.quick_config(args.v, args.quiet, fmt="")

    with open('sites.txt', 'r') as sitesfile:
        sites = sitesfile.readlines()

    for i in range(len(sites)):
        sites[i] = sites[i].strip()

    condodict = {}
    if os.path.isfile("condodata.p"):
        condodict = pickle.load(open('condodata.p', 'rb'))

    ignorelist = []
    if os.path.isfile('ignore.txt'):
        with open('ignore.txt', 'r') as ignorefile:
            ignorelist = ignorefile.readlines()
            for i in range(len(ignorelist)):
                ignorelist[i] = ignorelist[i].strip()

    for site in sites:

        logging.debug("Downloading {} ...".format(site))
        req = urllib.request.urlopen(site, timeout=60)
        data = req.read()
        datastr = data.decode('Thai')
        logging.debug("{} bytes of data downloaded.".format(len(data)))

        topics = re.findall(r'<tr class="post">.*</tr>', datastr)
        logging.debug("Found {} topics.".format(len(topics)))

        re_condono_end = re.compile(r'[0-9]+')
        re_condono_start = re.compile(r'f=')
        re_title_start = re.compile(r'<title id="ForumTitle">')
        re_title_end = re.compile(r' - ')
        re_topic_header_start = re.compile(r'<a target=_blank href=..default.aspx.g=posts&t=[0-9]+\' class=\'post_link\'>')
        re_link_end = re.compile(r'.*?</a>')
        re_href_start = re.compile(r'href=[\'"]')
        re_quotation_end = re.compile(r'.*?[\'"]')
        re_thread_end = re.compile(r'href=.*?t=[0-9]*')
        re_thread_start = re.compile(r't=')
        re_topic_starter_start = re.compile(r'profile&u=[0-9]*\'>')
        re_rent = re.compile('(rent|leasing|เช่า)', flags=re.IGNORECASE)

        title_start = re_title_start.search(datastr).end()
        title_end = re_title_end.search(datastr, pos=title_start).end() - 3
        title = datastr[title_start:title_end]
        logging.info("\n{}:".format(title))

        condono_start = re_condono_start.search(site).end()
        condono_end = re_condono_end.search(site, pos=condono_start).end()
        condono = int(site[condono_start:condono_end])

        if not condono in condodict:
            condodict[condono] = {}

        thisdict = condodict[condono]

        new_topics = []

        for topic in topics:

            if not re_rent.search(topic):
                continue

            header_start_res = re_topic_header_start.search(topic)
            linkarea_start = header_start_res.start()
            header_start = header_start_res.end()
            header_end = re_link_end.search(topic, pos=header_start).end() - 4
            header = topic[header_start:header_end]
            topic_starter_start = re_topic_starter_start.search(topic, pos=header_end).end()
            topic_starter_end = re_link_end.search(topic, pos=topic_starter_start).end() - 4
            topic_starter = topic[topic_starter_start:topic_starter_end]
            href_start = re_href_start.search(topic, pos=linkarea_start).end()
            href_end = re_quotation_end.search(topic, pos=href_start).end() - 1
            href = "http://www.prakard.com" + topic[href_start:href_end]
            threadarea = re_thread_end.search(topic, pos=linkarea_start)
            thread_start = re_thread_start.search(topic, pos=threadarea.start()).end()
            thread_end = threadarea.end()
            thread = int(topic[thread_start:thread_end])

            if topic_starter in ignorelist:
                continue
            if header in thisdict:
                continue

            thisdict[header] = {'header': header, "thread_starter": topic_starter, "href": href,
                                "date_added": az.utcnow()}
            new_topics.append(header)

        if new_topics:
            logging.info("\n*New topics*")
            for header in new_topics:
                logging.info('\n' + topic_dict_to_string(thisdict[header]))

        topics_to_show = pd.Series()
        cutoffdate = az.utcnow() - timedelta(hours=args.cutoffhours)
        for header, tdict in thisdict.items():
            if tdict['date_added'] > cutoffdate and header not in new_topics \
                    and tdict['thread_starter'] not in ignorelist:
                topics_to_show[tdict['date_added']] = header
        topics_to_show.sort_index()

        if not topics_to_show.empty:
            logging.info("\n*Topics in the last {} hours:*".format(args.cutoffhours))
            for header in topics_to_show:
                logging.info('\n' + topic_dict_to_string(thisdict[header]))

    pickle.dump(condodict, open('condodata.p', 'wb'))


            # logging.info('"{}" - {} - {} - {}'.format(header, topic_starter, href, thread))


