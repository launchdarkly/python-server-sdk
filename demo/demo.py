from ldclient import LDClient

if __name__ == '__main__':
    apiKey = 'feefifofum'
    client = LDClient(apiKey)
    print client._apiKey    