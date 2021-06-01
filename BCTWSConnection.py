"""
BlueCity Sensor Real-Time Data Query Tutorial,
By Mohammad Pourmeydani Last Updated: Feb 12, 2021
This notebook will include the BlueCity Sensor API features and how to query data from their API using Python. 
1.Request a token from BlueCity to use API access. This token will allow you to view the sensor data of the sensors installed at University Blvd and Westbrook Mall Ave at UBC Vancouver. 
2.Check the README file to learn what to enter for UDID. 
"""

##Real-Time API
import time
import asyncio
import websockets
import time
import multiprocessing
import datetime


def utf8len(s):
    return len(s.encode('utf-8'))


class BCTWSConnection(multiprocessing.Process):
    def __init__(self, UDID, token, url="realtime.bluecitytechnology.com", cacheSize=10):

        self.UDID = UDID
        self.token = token
        self.url = url
        self.frames = multiprocessing.Queue()
        self.cacheSize = cacheSize

        super().__init__(None)

        self.start()

    def run(self):
        asyncio.get_event_loop().run_until_complete(
            self.connectToServer('wss://{}'.format(self.url), self.UDID, self.token))

    async def connectToServer(self, uri, UDID, token):
        while True:
            try:
                async with websockets.connect(uri+"/ws/realtime/{}/".format(UDID), extra_headers={"token": token, "UDID": UDID}) as websocket:
                    print('----BCT realtime service---- Connected.')
                    while True:
                        res = await websocket.recv()
                        # if self.frames.qsize() > self.cacheSize:
                        #     self.get()

                        self.frames.put(res)
            except:
                print(
                    '----BCT realtime service---- Connection failed. Retry to connect in 2 seconds.')
                time.sleep(2)

    def get(self):
        return self.frames.get()


if __name__ == "__main__":

    c_ = BCTWSConnection(
        "BCT_3D_5G_0103002", "t6oSb-MepdXvDm88rjv-3UybUFprW-cX58CSO9DmmxkimwC5Qj01vwuVwT_1X95AkjBRfb53PunWpQDN")

    count = 0
    lenght = 0
    st = time.time()
    while count < 10000:
        f_ = c_.get()
        print(c_.get())
        lenght += utf8len(f_)
        print(f_.split(','))
        print(f_)
        count += 1
        time.sleep(1)
    
    c_.terminate()

    ft = time.time()
    print(lenght/(ft-st))
