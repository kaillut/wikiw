import rpyc
import requests as rq
from anytree import Node
from concurrent.futures import *
import time

##class for the services offered to the server
class EndService(rpyc.Service):
    ##init for the service
    def __init__(self,r,w,s):
        self.worker = w
        self.requester = r
        self.stop = s

    ##end function that server can call
    def exposed_End(self,node):
        print("Solution found: ",node)
        global Stop
        Stop = True
        ##sends shutdown commands for threadpools
        self.requester.shutdown(wait=False,cancel_futures=True)
        self.worker.shutdown(wait=False,cancel_futures=True)
        ##closes the connection (throws error on server side which causes the thread serving this connection to close)
        c.close()

##worker function that handles json data
def worker(json):
    for i in json["query"]["pages"].values():
        ##go through the links
        for titles in i["links"]:
            ##prevent node to refer to itself
            if not titles["title"] == i["title"]:
                ##try if the connection closed because other thread/client found answer causing connection to close
                try:
                    ##calls the add node function on server (adds the node to tree)
                    c.root.Add_Node(titles["title"],i["title"])
                except:
                    pass
## requester function that makes request to wiki                   
def req(topic):
    try:
        ##checks if the topic already handled
        if c.root.a_handled(topic):
            print("already handled")
            return
        print("Starting wikipedia session:  ",topic)
        ##make request to wiki
        Session = rq.Session()
        endpoint = "https://en.wikipedia.org/w/api.php"
        ##500 links from page
        parameters = {"action":"query","plnamespace":"0","format":"json","prop":"links","pllimit":"500","titles":topic}
        data = Session.get(url=endpoint,params=parameters,timeout = 5).json()
        print("Got response")
        ##throw the json to the workerthreadpool
        workerpool.submit(worker,data)
        ##checks if there is continue in json
        while "continue" in data:
            print("Sending continue")
            ##make new request (500 links continue)
            parameters = {"action":"query","plnamespace":"0","format":"json","prop":"links","pllimit":"500","titles":topic,"plcontinue":data["continue"]["plcontinue"]}
            data = Session.get(url=endpoint,params=parameters,timeout = 5).json()
            print("Got response")
            ##throw the new json to workerthreadpool
            workerpool.submit(worker,data)
        ##add the topic to handled
        c.root.handled(topic)
        print("Handled:  ",topic)
        return
    except RuntimeError:
        ## caused if some other thread called shutdown on threadpool and this thread called submit on threadpool
        pass
    except EOFError as e:
        print("Server closed or lost connection")
    except Exception as e:
        print(e)
        print("Failed to get data, title:  "+article)

        
Stop = False
##ask for connection information
print("address:")
adr = input()
print("port:")
port = int(input())
print("how many wikipedia request threads :")
work = int(input())

print("how many worker threads :")
_req = int(input())
##create thread pools
RequesterPool = ThreadPoolExecutor(max_workers=_req)
workerpool= ThreadPoolExecutor(max_workers=work)
##start the connection
c = rpyc.connect(adr,port,service= EndService(RequesterPool,workerpool,Stop))
print("connection started")

##main loop
while not Stop:
    ##print queue sizes
    print("worker queue size: ",workerpool._work_queue.qsize())
    print("request queue size: ",RequesterPool._work_queue.qsize())
    ##checks the queue sizes and if stop given
    if not Stop and RequesterPool._work_queue.qsize() <5 and workerpool._work_queue.qsize() <10:
        
        try:
            ##asks for list of topic from server
            req_s_l =list(c.root.Request_get())
            ##for each topic given creates task to the threadpool
            RequesterPool.map(req,req_s_l)
        except RuntimeError as e:
            ##caused if other thread called shutdown on threadpool
            pass
        except EOFError as e:
            Stop = True
            print("Server closed or lost connection")
    ##put this loop on 5 second sleep
    time.sleep(5)
