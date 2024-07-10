import psycopg2
import networkx as nx
from dotenv import load_dotenv
import os

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')

# Conectar a la base de datos PostgreSQL
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)

def read_topology(conn):
    G = nx.Graph()

    with conn.cursor() as cursor:
        # Leer nodos
        cursor.execute("SELECT id_node, lat, lon FROM nodes WHERE id_topology = 1")
        nodes = cursor.fetchall()
        for node in nodes:
            node_id, lat, lon = node
            G.add_node(node_id, pos=(lon, lat))
        
        # Leer enlaces
        cursor.execute("SELECT id_link, source_node, target_node, ST_Length(geom::geography) AS length FROM links WHERE id_topology = 1")
        links = cursor.fetchall()
        for link in links:
            id_link, source_node, target_node, length = link
            G.add_edge(source_node, target_node, weight=length, id=id_link)

    return G

G = read_topology(conn)

def greedy_shortest_path(graph, source, destination):
    distances = {node: float('inf') for node in graph.nodes}
    previous = {node: None for node in graph.nodes}
    distances[source] = 0
    nodes = list(graph.nodes)
    
    while nodes:
        current_node = min(nodes, key=lambda node: distances[node])
        nodes.remove(current_node)
        
        if current_node == destination:
            break
        
        for neighbor in graph.neighbors(current_node):
            new_distance = distances[current_node] + graph[current_node][neighbor]['weight']
            if new_distance < distances[neighbor]:
                distances[neighbor] = new_distance
                previous[neighbor] = current_node
    
    path = []
    current_node = destination
    while previous[current_node] is not None:
        path.insert(0, current_node)
        current_node = previous[current_node]
    if path:
        path.insert(0, current_node)
    
    return distances, previous, path

def find_backup_path(graph, primary_path):
    G_temp = graph.copy()
    for i in range(len(primary_path) - 1):
        u, v = primary_path[i], primary_path[i + 1]
        if G_temp.has_edge(u, v):
            G_temp[u][v]['weight'] *= 10
    
    return G_temp

def save_route_to_db(conn, id_topology, route_type, path):
    with conn.cursor() as cursor:
        # Construir el LINESTRING para la ruta
        linestring = "LINESTRING("
        for node in path:
            lon, lat = G.nodes[node]['pos']
            linestring += f"{lon} {lat}, "
        linestring = linestring.rstrip(", ") + ")"
        
        cursor.execute("""
            INSERT INTO routes (id_topology, route_type, geom)
            VALUES (%s, %s, ST_GeomFromText(%s, 4326))
        """, (id_topology, route_type, linestring))
    
    conn.commit()

source = 1
destination = 20

distances, previous, primary_path = greedy_shortest_path(G, source, destination)
print("Ruta primaria:", primary_path)

G_temp = find_backup_path(G, primary_path)
distances, previous, backup_path = greedy_shortest_path(G_temp, source, destination)
print("Ruta de respaldo:", backup_path)

# Guardar rutas en la base de datos
save_route_to_db(conn, 1, 'primary', primary_path)
save_route_to_db(conn, 1, 'backup', backup_path)
