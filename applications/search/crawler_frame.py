import logging
from datamodel.search.SdkuoJermainzSampath1_datamodel import SdkuoJermainzSampath1Link, OneSdkuoJermainzSampath1UnProcessedLink
from spacetime.client.IApplication import IApplication
from spacetime.client.declarations import Producer, GetterSetter, Getter
#from lxml import html,etree
from BeautifulSoup import BeautifulSoup
from urlparse import urljoin, urlparse
import re, os
from time import time
from uuid import uuid4
import operator

from urlparse import urlparse, parse_qs
from uuid import uuid4

logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"
visited = set()
subDomains = dict()
mostLinks = dict()

@Producer(SdkuoJermainzSampath1Link)
@GetterSetter(OneSdkuoJermainzSampath1UnProcessedLink)
class CrawlerFrame(IApplication):
    app_id = "SdkuoJermainzSampath1"

    def __init__(self, frame):
        self.app_id = "SdkuoJermainzSampath1"
        self.frame = frame


    def initialize(self):
        self.count = 0
        links = self.frame.get_new(OneSdkuoJermainzSampath1UnProcessedLink)
        if len(links) > 0:
            print "Resuming from the previous state."
            self.download_links(links)
        else:
            l = SdkuoJermainzSampath1Link("http://www.ics.uci.edu/")
            print l.full_url
            self.frame.add(l)

    def update(self):
        unprocessed_links = self.frame.get_new(OneSdkuoJermainzSampath1UnProcessedLink)
        if unprocessed_links:
            self.download_links(unprocessed_links)

    def download_links(self, unprocessed_links):
        for link in unprocessed_links:
            print "Got a link to download:", link.full_url
            downloaded = link.download()
            links = extract_next_links(downloaded)
            for l in links:
                if is_valid(l):
                    self.frame.add(SdkuoJermainzSampath1Link(l))

    def shutdown(self):
        print (
            "Time time spent this session: ",
            time() - self.starttime, " seconds.")
    
def extract_next_links(rawDataObj):
    outputLinks = []
    '''
    rawDataObj is an object of type UrlResponse declared at L20-30
    datamodel/search/server_datamodel.py
    the return of this function should be a list of urls in their absolute form
    Validation of link via is_valid function is done later (see line 42).
    It is not required to remove duplicates that have already been downloaded. 
    The frontier takes care of that.
    
    Suggested library: lxml
    '''
    try:
        if(not rawDataObj.http_code == 200):
            raise exception(rawDataObj.error_message)
    
        url = ""
        if(rawDataObj.is_redirected):
            url = rawDataObj.final_url
        else:
            url = rawDataObj.url

        soup = BeautifulSoup(rawDataObj.content)
        soup.prettify()
        rawlinks = []
        for anchor in soup.findAll("a", href=True):
            rawlinks.append(anchor.get("href"))
    
        for link in rawlinks:
            if(not link.startswith("http://") or not link.startswith("https://")):
                link = urljoin(url, link)
            outputLinks.append(str(link))

        mostLinks[url] = len(outputLinks)

    except Exception as error:
        print("caught error: " + repr(error))

    return outputLinks

def is_valid(url):
    '''
    Function returns True or False based on whether the url has to be
    downloaded or not.
    Robot rules and duplication rules are checked separately.
    This is a great place to filter out crawler traps.
    '''
    if url in visited:
      return False
    visited.add(url)

    parsed = urlparse(url)
    if parsed.scheme not in set(["http", "https"]):
        return False

    print(url)
    try:
        if re.match(r"^.*(calendar|\?|mailman/listinfo)", url):
            return False
  
        if re.match(r"^.*?(/.+?/).*?\1.*$|^.*?/(.+?/)\2.*$", url):
            return False

        if re.match(r"^.*(/misc|/sites|/all|/themes|/modules|/profiles" \
            +"|/css|/field|/node|/theme){3}.*$", url):
            return False

        if re.match(r"^.*/[^/]{300,}$", url):
            return False

        #check content of url against other data types
        valid = ".ics.uci.edu" in parsed.hostname \
            and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4"\
             + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
             + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
             + "|thmx|mso|arff|rtf|jar|csv"\
             + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower())

<<<<<<< HEAD
=======
        if valid:
            suburl = urlparse(url)
            subdomain = suburl.hostname
            if subdomain not in subDomains:
                subDomains[subdomain] = 1
            else:
                subDomains[subdomain] += 1

            file = open("output.txt", "w")
            maxUrl = max(mostLinks.iteritems(), key=operator.itemgetter(1))

            file.write("Most outgoing links: {}\n{}\n".format(maxUrl[1], maxUrl[0]))
            file.write("Url count:\n")
            for sub, num in subDomains.iteritems():
                file.write("{} - {}\n".format(sub, num))
            file.close()
            print(url)
>>>>>>> e4f779c8f361c42ad8a8f78822ad8302d6559ba2
    
        return valid
            
    except TypeError:
        print ("TypeError for ", parsed)
        return False


