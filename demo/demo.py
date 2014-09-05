from ldclient import LdClient

if __name__ == '__main__':
    apiKey = 'feefifofum'
    client = LdClient(apiKey)
    print client._apiKey    