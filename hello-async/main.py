import asyncio

import ldclient.client
from ldclient import Config, Context

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    async def main():
        client = ldclient.LDClient(Config(sdk_key="sdk-95f77c0e-6ce3-440b-9990-0d1cc6e1b34e", stream=False), loop=asyncio.get_running_loop())
        await client.wait_for_initialization()
        res = await client.variation('not-boolean-flag', Context.create('basic-user'), None)
        print("Got this res", res)

asyncio.ensure_future(main())
asyncio.get_event_loop().run_forever()
