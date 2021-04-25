from anytree import *
import rpyc
from rpyc.utils.server import ThreadedServer
from random import shuffle
import time
import threading

## class for the server
class server(rpyc.Service):
    
    connections = []
    startpoint = ""
    endpoint = ""
    endNode = None
    tree = None
    exposed_already_found = set()
    exposed_already_handled = set()

    ##takes the given information
    def __init__(self,s,e):
        self.startpoint = s
        self.endpoint = e
        self.connections = []
        self.tree = Node(s)

    ##adds topic name to set of already handled
    def exposed_handled(self,topic):
        self.exposed_already_handled.add(topic)

    ##checks on connection if solution already found and sends end if solved
    def on_connect(self,conn):
        print("Client connected")
        self.connections.append(conn)
        global stop
        if stop:
            try:
                conn.root.End(endNode)
            except:
                pass
        return

    ##checks if topic in already handled
    def exposed_a_handled(self,topic):
        return topic in self.exposed_already_handled

    
    def on_disconnect(self,conn):
        print("Client disconnected")
        self.connections.remove(conn)
        pass

    ##adds node to the tree takes as argument name of the node and the parent you want the node add to
    def exposed_Add_Node(self,Name,_Parent):
        try:
            ##checks if the name already added
            if Name not in self.exposed_already_found:
                ##adds to added list
                self.exposed_already_found.add(Name)
                ##searches the tree for the parent
                Parent = search.find(self.tree,lambda node: node.name == _Parent)
                ##creates the node
                node = Node(Name,parent = Parent)
                ##checks if it is the solution
                if Name.lower() == self.endpoint.lower():
                    ##prints the solution (node prints the route)
                    print("Solution found: ",node)
                    ##calls solution found function
                    self.exposed_Solution_Found(node)
                return
        except Exception as e:
            print(e)
    ##takes the node as argument, calls end in all the clients
    def exposed_Solution_Found(self,Node):
        endNode = Node
        ##prints the whole tree
        print(RenderTree(self.tree, style=ContRoundStyle()))
        ##goes through the connections and calls end on them
        for c in self.connections:
            c.root.End(Node)
        ##sets stop flag
        global stop
        stop = True
        ##prints the solution (the tree can be thousands of lines long)
        print("Solution found: ",Node)
        return

    ##returns 50 random topics to client
    def exposed_Request_get(self):
        print("Requested articles")
        i = 1
        re =[node.name for node in LevelOrderIter(self.tree,maxlevel =i)]
        ##takes topics from the tree (beginning from the root (and goes by depth)) and removes topics if they are in already_handled, then checks if there are topics left if not goes deeper 
        while len(set(re).difference(self.exposed_already_handled)) == 0:
            i+=1
            re =[node.name for node in LevelOrderIter(self.tree,maxlevel =i)]
        re = list(set(re).difference(self.exposed_already_handled))
        length = len(re)
        ##shuffles the list to lessen the chances of two workers getting same topics
        shuffle(re)
        print("Given upto the depth of: ",i)
        ##calls a thread to print the tree
        pthread = threading.Thread(target=self.print_thread,args=())
        pthread.run()
        ##returns max 50 topics
        return re[0:min(length,50)]

    ##thread function for printing
    def print_thread(self):
        print(RenderTree(self.tree, style=ContRoundStyle()))

##asks for the starting information
print("starting point:")
sp = input()
print("ending point:")
ep = input()
stop = False
##creates threaded server with the custom class
t = ThreadedServer(server(sp,ep), port =1234)
print("Starting server")
##starts the server
t.start()
print("server stopped")
