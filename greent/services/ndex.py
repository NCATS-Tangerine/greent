import networkx as nx
from ndex2 import create_nice_cx_from_networkx
from ndex2.client import Ndex2

class NDEx:

   """ An interface to the NDEx network catalog. """ 
   def __init__(self, account, password, uri="http://public.ndexbio.org"):
      self.uri = uri
      self.session = None
      self.account = account
      self.password = password
      try:
         self.session = Ndex2 (uri, account, password)
         self.session.update_status()
         networks = self.session.status.get("networkCount")
         users = self.session.status.get("userCount")
         groups = self.session.status.get("groupCount")
         print(f"session: networks: {networks} users: {users} groups: {groups}")
      except Exception as inst:
         print(f"Could not access account {account}")
         raise inst
      
   def save_nx_graph (self, name, graph):
      """ Save a networkx graph to NDEx. """
      assert name, "A name for the network is required."

      """ Serialize node and edge python objects. """
      g = nx.MultiDiGraph()
      nodes = { n.identifier : i for i, n in enumerate (graph.nodes()) }
      for n in graph.nodes ():
         g.add_node(n.identifier, attr_dict=n.n2json())
      for e in graph.edges (data=True):
         edge = e[2]['object'] 
         g.add_edge (edge.subject_node.identifier,
                     edge.object_node.identifier,
                     attr_dict=e[2]['object'].e2json())

      """ Convert to CX network. """
      nice_cx = create_nice_cx_from_networkx (g)
      nice_cx.set_name (name)
      print (f" connected: {nx.is_connected(graph.to_undirected())} edges: {len(graph.edges())} nodes: {len(graph.nodes())}")
      print (nice_cx)

      """ Upload to NDEx. """
      upload_message = nice_cx.upload_to(self.uri, self.account, self.password)
