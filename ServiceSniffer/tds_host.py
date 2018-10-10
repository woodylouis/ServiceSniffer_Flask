import requests
import sqlite3
from sqlite3 import Error

import store
from bs4 import BeautifulSoup
from collections import OrderedDict

hostServiceDict = {}
def get_services(candidate_list):
    #print(candidate_list)
    #print(len(candidate_list))
    ###
    # Candiadte_list = ['http://host_ip_1:port/thredds/catalog.html', 'http://host_ip_2:port/thredds/catalog.html', 'http://host_ip_3:port/thredds/catalog.html'] #
    ###

    xmls = []

    accessibleUrl = []

    """
    get valid URL address and convert them to xml format
    """
    for url in candidate_list:
        # print(url)
        r = requests.get(url, timeout=0.5, allow_redirects=False)
        # print(r.url, r.status_code)
        if r.status_code == 200:
            accessibleUrl.append(r.url)
    # print(accessibleUrl)

    for candidate in accessibleUrl:
        #print(candidate)
        if 'html' in candidate:
            links = candidate.replace("html", "xml")
            xmls.append(links)
    #print(xmls)

    serverDescriptionDict = {}
    serviceTemp = {}
    serviceTypeDict = {}
    services = []
    temp = []
    ls = []
    for links in xmls:
        #print(links)
        r = requests.get(links, timeout=0.5, allow_redirects=False)
        #print(r)
        ls.append(r.url)
        soup = BeautifulSoup(r.text, features="lxml")
        #print(soup)
        """
        get serverDescription for each valid server URL
        e.g. {'http://192.168.1.1:80/thredds/catalog.xml': 'test THREDDS Server Default Catalog', 'http://192.168.1.2:80/thredds/catalog.xml': 'test THREDDS Server Default Catalog'} 
        """
        serverDescription = soup.find('catalog')['name']
        if r.url not in serverDescriptionDict:
            serverDescriptionDict[r.url] = serverDescription
        """
        get services types for each valid server URL
        e.g. {'http://192.168.1.1:80/thredds/catalog.xml': ['odap', 'OPENDAP'], 'http://192.168.1.2:80/thredds/catalog.xml': ['odap', 'OPENDAP', 'NCML'],
        """
        catalog = soup.find('catalog')

        for services in catalog.find_all('service'):
            # print(services)
            for service in services.find_all('service'):
                s = (r.url, service['servicetype'])
                # print(s)
                serviceTemp.setdefault(r.url, []).append(service['servicetype'].replace('"', ''))

    for u, s in serviceTemp.items():
        i = list(OrderedDict.fromkeys(s))
        if u not in serviceTypeDict:
            serviceTypeDict[u] = i
    #(serviceTypeDict)

    """""""""
    Remove duplicated hosts with different port e.g. 80/8080 but they have same service types and server description
    """""""""
    serviceTypeDictResult = {}
    serverDescriptionDictResult = {}
    result = {}

    seen_ips = set()
    for url, services in serviceTypeDict.items():
        ip = url.strip('http://').strip('thredds/catalog.xml').split(':')[0]
        if ip not in seen_ips:
            seen_ips.add(ip)
            serviceTypeDictResult[url] = services
    #print(serviceTypeDictResult)
    seen_ip = set()
    for url, description in serverDescriptionDict.items():
        ip = url.strip('http://').strip('thredds/catalog.xml').split(':')[0]
        if ip not in seen_ip:
            seen_ip.add(ip)
            serverDescriptionDictResult[url] = description
    # print(serverDescriptionDictResult)

    """""""""
    Return a final dictionary for each server: unique service type and description for the data service
    """""""""
    for i, description in serverDescriptionDictResult.items():
        for j, service in serviceTypeDict.items():
            if i == j:
                result[i] = [service, description]
    # print(result)
    return result
"""""""""
capture data to database here
"""""""""
def capture_host_in_db(result):
    #print(result)
    #print(hostServiceDict)
    database = "C:\\Users\LI252\PycharmProjects\servicesniffer\database\database.sqlite"
    conn = store.create_connection(database)
    # print(conn)
    with conn:

        hostTemp = []
        existingHostIps = []
        existingHostId = []

        for urls, services in result.items():
            host_port = urls.strip('http://').strip('thredds/catalog.xml').split(':')
            #print(host_port)
            hostTemp.append(host_port[0])
            #print(hostTemp)
            for host in hostTemp:
                """
                Check if the hosts that already in the database. If not in the database, add the hosts.
                """
                try:
                    if host != store.select_host_by_host_ip(conn, host):
                        thredds = (host_port[0], host_port[1], urls, services[1])
                #print(thredds)
                        store.create_unique_host(conn, thredds)
                except:
                    print("Fail to store host into the host table!")

            for service in services[0]:

                """
                Create unique service
                """
                theService = store.select_service(conn, service)
                if service != theService:
                    store.create_unique_service(conn, service)

        for urls, services in result.items():
            host = urls.strip('http://').strip('thredds/catalog.xml').split(':')
            hostId = store.select_host_id_by_host_ip(conn, host[0])
            # print(hostId)
            for service in services[0]:
                # print(service)
                theService = store.select_service(conn, service)
                if service == theService:
                    serviceId = store.select_service_id_by_name(conn, service)
                    # print(hostId, serviceId)
                    hs = hostId, serviceId
                    # print(hs)
                    """
                    compare existing hosts and services in host_services_table, if not exist, add to the table
                    """
                    existingPair = store.select_host_services_by_host_id_and_service_id(conn, hs)
                    #print(existingPair)
                    if hs != existingPair:
                        store.create_unique_host_service(conn, hs)

def getCandiateUrl():
    candidateList = []
    try:
        with open(f"C:/Users/LI252/Desktop/threddsCandiates.txt", "r") as tdsFile:
            for url in tdsFile.read().splitlines():
                candidateList.append(url)
            return candidateList

    except:
        print("Invalid file to interrogate valid thredds")





if __name__ == '__main__':
    hosts = getCandiateUrl()
    hosts_services = get_services(hosts)
    capture_host_in_db(hosts_services)



