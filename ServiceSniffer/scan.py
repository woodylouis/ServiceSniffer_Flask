import nmap
import time
import datetime
import requests

date = datetime.datetime.now().strftime("%d%m%Y_%H%M%S")
reportName = str(date) + "_ScanReport" + ".txt"
scanReport = open(reportName, "a")

def get_thredds_hosts(network_prefix):
    t1 = time.time()
    nm = nmap.PortScanner()
    activeHosts = []
    portDict = {}
    hostInfoDict = {}
    """
    Perform simple ping and create a list of online hosts
    This is acceptable scanning speed and realiability for bunch of /16 network
    """
    pingResult = nm.scan(hosts=network_prefix,
                         arguments='--min-hostgroup=5000 --max-hostgroup=10000 --min-parallelism=100 --max-parallelism=200 --host-timeout=2s -T4 -n -sP')

    for pResult in pingResult['scan'].values():
        hostStatus = pResult['status']['state']
        host = pResult['addresses']['ipv4']
        if hostStatus == 'up':
            activeHosts.append(host)
    totalActiveHosts = ("There are " + str(len(activeHosts)) + " active hosts online. The hosts are: \n" + '\n'.join(
        '{}: {}'.format(*k) for k in enumerate(activeHosts, start=1)) + "\n")
    # scanReport.write(str(totalActiveHosts))
    # scanReport.write(totalActiveHosts)
    # """
    # 1-1024 popular port
    # 1194 openVPN
    # 1433 Microsoft SQL Server
    # 1434 Microsoft SQL Monitor
    # 2483-2484 Oracle database
    # 3306 MySQL database service
    # 4333 mSQL
    # 5432 PostgreSQL database
    # 8080 HTTP Proxy such as Apache
    # 27017 Mongo database
    # """
    for host in activeHosts:
        aDict = nm.scan(hosts=host,
                        arguments='--min-hostgroup=5000 --max-hostgroup=10000 --min-parallelism=100 --max-parallelism=200 --host-timeout=2s -T5 -n -v',
                        ports="80, 1433-1434, 2483-2484, 800, 3306, 4333, 5432, 5000, 8080, 9000, 8433, 27017, 50000")
        scanReport.write(str(aDict))

        for serviceScan in aDict['scan'].values():
            host = serviceScan['addresses']['ipv4']
            try:
                portInfo = serviceScan['tcp']
            except:
                pass
            for port, info in portInfo.items():
                hostServices = info['name']
                portState = info['state']
                """
                Get all hosts' HTTP service and port in a dictionary
                """
                if portState == 'open' and 'http' in hostServices:
                    if port not in portDict:
                        portDict[port] = hostServices
                    if host not in hostInfoDict:
                        hostInfoDict[host] = portDict
    # print(hostInfoDict)

    httpHostStatusDict = {}
    for host, hostInfo in hostInfoDict.items():
        for port in hostInfo.keys():
            urls = f'http://{host}:{port}/thredds/catalog.html'
            # print(urls)
            try:
                r = requests.get(urls, timeout=0.5, allow_redirects=False)
                # print(r.url)
                if str(r.url) not in httpHostStatusDict:
                    httpHostStatusDict[str(r.url)] = str(r.status_code)
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
    # print(httpHostStatusDict)
    threddsCandidateHostList = []
    noThreddsInstalledHostList = []
    redirectToOtherURL = []
    unknownError = []
    for urls, status in httpHostStatusDict.items():
        if status == '404':
            noThreddsInstalledHostList.append(urls)
        elif status == '302':
            # redirect to CISCO gateway login page or link has been moved permanently
            redirectToOtherURL.append(urls)
        elif status == '200':
            threddsCandidateHostList.append(urls)
        else:
            unknownError.append(urls)
        # print("There are", len(redirectToOtherURL), "urls redirect to firewall login page\n", redirectToOtherURL, '\n')
        # print("There are", len(threddsCandidateHostList), "urls may have Thredds installed\n", threddsCandidateHostList, '\n')
        # print("There are", len(noThreddsInstalledHostList), "urls have no Thredds installed in these hosts\n", noThreddsInstalledHostList, '\n')
        # print("There are", len(unknownError), "unknown error urls\n", unknownError, '\n')
        # for candidate in threddsCandidateHostList:
        #     print(type(candidate))
    return threddsCandidateHostList
    # print(threddsCandidateHostList)
    # """


try:
    f = open(input("Please enter the path for the files that contains address: "), "r")
    network = f.read()
except:
    print("Please enter a valid file path")


if __name__ == '__main__':
    hosts = get_thredds_hosts(network)



