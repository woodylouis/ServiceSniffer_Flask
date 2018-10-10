from bs4 import BeautifulSoup
import requests
import store
import tds_host
from urllib.parse import urlsplit
#from tds_host import hosts_services
import helper

hostFilesDict = {}

###
"""
Get unqiue links
"""
linkTemp = []
def getTDSInfo(host_services):
    for hosts in host_services.keys():
        linkTemp.append(hosts)

    return linkTemp


"""
Get nc files if they are located at home page
"""
def find_files_from_page(urls):
    #print(len(urls))
    urlTempList = []
    serviceTemp = {}
    hostDatasetDict = {}


    for url in urls:
        host = url.strip('http://').strip('thredds/catalog.xml').split(':')
        hostIP = host[0]
        port = host[1]

        r = requests.get(url, timeout=0.5, allow_redirects=False)
        soup = BeautifulSoup(r.text, features="lxml")
        catalog = soup.find('catalog')

        for services in catalog.find_all('service'):
            for service in services.find_all('service'):
                name = service['name']
                serviceType = service['serviceType'.lower()]
                base = service['base']
                if name not in serviceTemp:
                    serviceTemp[base] = [name, serviceType]
    #print(serviceTemp)
        try:
            for header in catalog.find_all('dataset'):

                for serviceName in header.find('servicename'):
                    description = header['id']
                    datasetName = header['name']
                    ncURLPath = header['urlpath']

                    for bases, names in serviceTemp.items():
                        if serviceName in names[0]:
                            path = f"http://{hostIP}:{port}{bases}{ncURLPath}"
                            # print(description, path, ncName)
                            if path not in hostDatasetDict:
                                hostDatasetDict[path] = [description, datasetName]
        except:
            pass


    return (hostDatasetDict)
"""
Capture dataset in the dataset if they are located at home page
"""
def catch_data_into_dataset_table(hostDatasetDict):
    # for keys, items
    database = "C:\\Users\LI252\PycharmProjects\servicesniffer\database\database.sqlite"
    conn = store.create_connection(database)

    with conn:
        for keys, items in hostDatasetDict.items():

            urlSplite = keys.split('/')
            #print(urlSplite)
            serviceName = urlSplite[4]
            #print(serviceName)
            urlPortion = urlsplit(keys, allow_fragments=True)
            hostIp = urlPortion.hostname
            hostId = store.select_host_id_by_host_ip(conn, hostIp)
            #print(hostId)
            if serviceName == 'dodsC':
                serviceType = 'opendap'.upper()
                serviceId = store.select_service_id_by_name(conn, serviceType)
                #print(serviceId)
            elif serviceName == 'ncss':
                serviceType = 'NetcdfSubset'
                #serviceId = store.select_service_id_by_name(conn, serviceType)
            elif serviceName == 'fileServer':
                serviceType = 'HTTPServer'
                serviceId = store.select_service_id_by_name(conn, serviceType)
                #print(serviceId)
            else:
                serviceId = store.select_service_id_by_name(conn, serviceName.upper())
                #print(serviceId)
            description = items[0]
            ncName = items[1]
            ncRecord = (ncName, description, keys, serviceId, hostId)
            existing = store.select_nc_name_and_description_and_url_path_and_service_id_and_host_id(conn, ncRecord)
            #
            if ncRecord != existing:
                store.create_nc_files_record(conn, ncRecord)


# """
# Get CatalogueRef link if they are located at home page
# """
def getHomeLink(urls):
    listOfLinksInHomePage = []
    for url in urls:

        urlPortion = urlsplit(url, allow_fragments=True)
        #print(urlPortion)
        hostIpWithPort = urlPortion.netloc
        # print(hostIpWithPort)

        r = requests.get(url, timeout=0.5, allow_redirects=False)
        soup = BeautifulSoup(r.text, features="lxml")
        catalog = soup.find('catalog')


        """
        There are two situations: one is catalogRef directly shown in homepage xml, but another one is catalogRef is stored
        under dataset tag. We can get all directory links in the home catalog page here
        """
        try:

            try:
                for catalogRef in catalog.find_all('catalogref'):
                    #print(url, catalogRef['xlink:href'])
                    catalogRefUrl = catalogRef['xlink:href']
                    if "/thredds/" in catalogRefUrl:
                        completeURL = f"http://{hostIpWithPort}{catalogRefUrl}"
                        listOfLinksInHomePage.append(completeURL)
                    else:
                        completeURL = f"http://{hostIpWithPort}/thredds/{catalogRefUrl}"
                        listOfLinksInHomePage.append(completeURL)
            except:
                for dataset in catalog.find_all('dataset'):
                    for catalogRef in dataset.find_all('catalogref'):
                        catalogRefUrl = catalogRef['xlink:href']
                        if "/thredds/" in catalogRefUrl:
                            completeURL = f"http://{hostIpWithPort}{catalogRefUrl}"
                            listOfLinksInHomePage.append(completeURL)
                            #print(listOfLinksInHomePage)
                        else:
                            completeURL = f"http://{hostIpWithPort}/thredds/{catalogRefUrl}"
                            listOfLinksInHomePage.append(completeURL)
        except:
            print("Something Wrong")

    return listOfLinksInHomePage
    # print(listOfLinksInHomePage)

"""
Iterate to find in the CatalogueRef link in home page (from getHomeLink())
"""
def getFilesFromHomeDirectories(listOfLinks):
    subURLs = []
    subCatalogRefHrefList = []
    links = []
    database = "C:\\Users\LI252\PycharmProjects\servicesniffer\database\database.sqlite"
    conn = store.create_connection(database)
    with conn:
        for link in listOfLinks:
            #print(link)
            urlPortion = urlsplit(link, allow_fragments=True)
            hostIpWithPort = urlPortion.netloc
            hostIp = urlPortion.hostname
            hostId = store.select_host_id_by_host_ip(conn, hostIp)
            r = requests.get(link, timeout=0.5, allow_redirects=False)
            #print(r.status_code)
            soup = BeautifulSoup(r.text, features="lxml")
            catalog = soup.find('catalog')
            for dataset in catalog.find_all('dataset'):
                for subDataset in dataset.find_all('dataset'):
                    ncName = subDataset['name']
                    description = subDataset['id']
                    ncUrlPath = subDataset['urlpath']
                    #print('filename:',ncName, 'description:', description, 'path', ncUrlPath)
                    for services in catalog.find_all('service'):
                        for service in services.find_all('service'):
                            serviceName = service['name']
                            # print(serviceName)
                            serviceType = service['serviceType'.lower()]
                            serviceTyp = helper.getURLPostfix(serviceType)
                            base = service['base']
                            #print(base)
                            # print(base)
                            if serviceTyp:
                                completeURL = f"http://{hostIpWithPort}{base}{ncUrlPath}{serviceTyp}"
                            else:
                                completeURL = f"http://{hostIpWithPort}{base}{ncUrlPath}"
                            serviceId = helper.compare(conn, serviceName)

                    datasetRecord = (ncName, description, completeURL, serviceId, hostId)
                    #print(datasetRecord)
                    existing = store.select_nc_name_and_description_and_url_path_and_service_id_and_host_id(conn, datasetRecord)
                    if datasetRecord != existing:
                        #print(ncRecord)
                        store.create_nc_files_record(conn, datasetRecord)

                    #print(urlPortion.hostname, subDataset['name'])

        for link in listOfLinks:
            postfix = 'catalog.xml'
            host = urlsplit(link, allow_fragments=True)
            # print(host)
            hostIP = host.netloc
            if postfix in link:
                urlPrefix = link.strip(postfix)
                # print(urlPrefix)
            else:
                urlPrefix = f"http://{hostIP}"

            r = requests.get(link, timeout=0.5, allow_redirects=False)
            soup = BeautifulSoup(r.text, features="lxml")
            catalog = soup.find('catalog')
            # print(catalog)
            for catalogRef in catalog.find_all('catalogref'):
                # print(catalogRef)
                catalogRefHref = catalogRef['xlink:href']
                # print(catalogRefHref)
                # ls.append(catalogRefHref)
                # print(urlPrefix, catalogRefHref)
                completeURL = urlPrefix + catalogRefHref
                subURLs.append(completeURL)

        for subURL in subURLs:
            host = urlsplit(subURL, allow_fragments=True)
            hostIpWithPort = host.netloc
            hostIp = host.hostname
            hostId = store.select_host_id_by_host_ip(conn, hostIp)
            try:
                r = requests.get(subURL, timeout=0.5, allow_redirects=False)
                soup = BeautifulSoup(r.text, features="lxml")
                # print(r.url, r.status_code)
                subCatalog = soup.find('catalog')
                for subCatalogRef in subCatalog.find_all('catalogref'):
                    subCatalogRefHref = subCatalogRef['xlink:href']
                    ### We can get a larget list of sub URL here
                    subCatalogRefHrefList.append(subCatalogRefHref)

                for dataset in subCatalog.find_all('dataset'):
                    if '.nc' in dataset['name']:
                        datasetName = dataset['name']
                        description = dataset['id']
                        ncUrlPath = dataset['urlpath']
                        for services in subCatalog.find_all('service'):
                            for service in services.find_all('service'):
                                serviceName = service['name']
                                # print(serviceName)
                                serviceType = service['serviceType'.lower()]
                                serviceTyp = helper.getURLPostfix(serviceType)
                                base = service['base']
                                # print(base)
                                if serviceTyp:
                                    completeURL = f"http://{hostIpWithPort}{base}{ncUrlPath}{serviceTyp}"
                                else:
                                    completeURL = f"http://{hostIpWithPort}{base}{ncUrlPath}"

                                serviceId = helper.compare(conn, serviceName)

                        datasetRecord = (datasetName, description, completeURL, serviceId, hostId)
                        # print(datasetRecord)

                        existing = store.select_nc_name_and_description_and_url_path_and_service_id_and_host_id(conn, datasetRecord)
                        if datasetRecord != existing:
                            # print(ncRecord)
                            store.create_nc_files_record(conn, datasetRecord)

            except AttributeError:
                pass

            except requests.exceptions.HTTPError:
                """An HTTP error occurred."""
                pass

            except requests.exceptions.ConnectionError:
                """A Connection error occurred."""
                pass
            except requests.exceptions.Timeout:
                """
                The request timed out while trying to connect to the remote server.

                Requests that produced this error are safe to retry.
                """
                pass
            except requests.exceptions.RequestException:
                """
                There was an ambiguous exception that occurred while handling your request.
                """
                pass



if __name__ == '__main__':
    hosts = tds_host.getCandiateUrl()
    hosts_services = tds_host.get_services(hosts)
    ##unique URLs
    urls = getTDSInfo(hosts_services)
    listOfFile = find_files_from_page(urls)
    catch_data_into_dataset_table(listOfFile)
    # get list of Catalogue link from Home page
    listOfLinks = getHomeLink(urls)
    getFilesFromHomeDirectories(listOfLinks)






