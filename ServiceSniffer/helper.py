import store

def compare(conn, serviceName):
    serviceId = ''
    if serviceName == 'odap':
        serviceType = 'opendap'.upper()
        serviceId = store.select_service_id_by_name(conn, serviceType)
        # print(serviceId, hostId)
    elif serviceName == 'dodsC':
        serviceType = 'opendap'.upper()
        serviceId = store.select_service_id_by_name(conn, serviceType)
        # print(serviceId, hostId)
    elif serviceName == 'ncss':
        serviceType = 'NetcdfSubset'
        serviceId = store.select_service_id_by_name(conn, serviceType)
        # print(serviceId, hostId)
    elif serviceName == 'dap4':
        serviceType = 'DAP4'
        serviceId = store.select_service_id_by_name(conn, serviceType)
        # print(serviceId, hostId)
    elif serviceName == 'subsetServer':
        serviceType = 'NetcdfSubset'
        serviceId = store.select_service_id_by_name(conn, serviceType)
        # print(serviceId, hostId)
    elif serviceName == 'fileServer':
        serviceType = 'HTTPServer'
        serviceId = store.select_service_id_by_name(conn, serviceType)
        # print(serviceId, hostId)
    elif serviceName == 'http':
        serviceType = 'HTTPServer'
        serviceId = store.select_service_id_by_name(conn, serviceType)
        # print(serviceId, hostId)
    elif serviceName == 'wms':
        serviceType = 'WMS'
        serviceId = store.select_service_id_by_name(conn, serviceType)
        # print(serviceId, hostId)
    elif serviceName == 'wcs':
        serviceType = 'WCS'
        serviceId = store.select_service_id_by_name(conn, serviceType)
        # print(serviceId, hostId)
    elif serviceName == 'ncml':
        serviceType = 'NCML'
        serviceId = store.select_service_id_by_name(conn, serviceType)
        # print(serviceId, hostId)
    elif serviceName == 'uddc':
        serviceType = 'UDDC'
        serviceId = store.select_service_id_by_name(conn, serviceType)
        # print(serviceId, hostId)
    elif serviceName == 'iso':
        serviceType = 'ISO'
        serviceId = store.select_service_id_by_name(conn, serviceType)

    return serviceId


## OPENDAP & NetcdfSubset nc files can be opened from browsers
def getURLPostfix(serviceName):
    postfix = ''

    if serviceName == 'OPENDAP':
        postfix = '.html'
    elif serviceName == 'NetcdfSubset':
        postfix = '/dataset.html'

    return postfix
